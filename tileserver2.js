/* =====================================================================

  Tirex Tileserver

  Tileserver for the Tirex system using node.JS (www.nodejs.org).

  http://wiki.openstreetmap.org/wiki/Tirex

========================================================================

  Copyright (C) 2011  Peter Körner <peter@mazdermind.de>
  
  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License
  as published by the Free Software Foundation; either version 2
  of the License, or (at your option) any later version.
  
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
  
  You should have received a copy of the GNU General Public License
  along with this program; If not, see <http://www.gnu.org/licenses/>.

===================================================================== */

var http = require('http');
var url = require('url');
var fs = require('fs');
var path = require('path');
var dgram = require('dgram');

// size in bytes of metatile header
var metatile_header_size = 20 + 8 * 64;

// open http connections waiting for answer
var pending_requests = {};

// statistics
var stats = 
{
	// when the server was started
	started: new Date(), 
	
	// number of tiles requestet in total
	tiles_requested: 0, 
	
	// number of tiles served from cache
	tiles_from_cache: 0, 
	
	// number of tiles sent to tirex
	tiles_rendered: 0, 
	
	// number of served static files
	static_handled: 0, 
	
	// number of incoming http requests
	http_requests: 0, 
	
	// statistics about the different maps
	maps: {}
}

// basic configuration
var config = 
{
	// directory to read the rirex config from
	configdir: '/etc/tirex',
	
	// port to listen on using http
	http_port: 5000, 
	
	// master socket
	master_socket: '/var/run/tirex/master.sock', 
	
	// tirex enqueue priority
	prio: 10, 
	
	// this will be filled from the tirex config
	maps: {}
}

// get long value at offset from buffer
Buffer.prototype.getLong = function(offset)
{
	return ((this[offset+3] * 256 + this[offset+2]) * 256 + this[offset+1]) * 256 + this[offset];
}

// end with code 200 OK and JSON data
http.ServerResponse.prototype.endJson = function(data) 
{
	this.writeHead(200, {'Content-Type': 'application/json; charset=utf-8'});
	this.end(JSON.stringify(data));
}

// end with and server error
http.ServerResponse.prototype.endError = function(code, desc, headers)
{
	headers = headers || [];
	headers['Content-Type'] = headers['Content-Type'] || 'text/plain';
	desc = desc || '';
	
	this.writeHead(code, headers);
	this.end(desc);
}

// end with a tile
http.ServerResponse.prototype.endTile = function(buffer, map, z, x, y)
{
	// TODO: add meta info & cache header
	this.writeHead(200, {
		'Content-Type': 'image/png', 
		'Content-Length': buffer.length, 
		'X-Map': map, 
		'X-Coord': z+'/'+x+'/'+y, 
		'X-License': 'Map data (c) OpenStreetMap contributors, CC-BY-SA'
	});
	
	this.end(buffer);
}

// log an request to the console
http.IncomingMessage.prototype.log = function(usage)
{
	// numeric resonses should be enhanced with status strings
	if(typeof usage == 'number')
		usage = usage + ' ' + http.STATUS_CODES[usage];
	
	// print the message
	console.log('%s %s -> %s', this.method, this.url, usage);
}

function count(o)
{
	var i = 0;
	for(k in o) i++;
	return i;
}

// the http server
var server = http.createServer(function(req, res)
{
	// count the request
	stats.http_requests++;
	
	// only GET requests are acceptable
	if(req.method != 'GET')
	{
		req.log(405);
		return res.endError(405, 'only get requests are handled');
	}
	
	// analyze the path
	var path = url.parse(req.url).pathname.split('/');
	
	// the first part of the url decides what route we'll take
	switch(path[1] || '')
	{
		// GET / -> list the maps
		case '':
		{
			var names = [];
			for(name in config.maps)
				names.push(name);
			
			req.log('maps');
			return res.endJson(names);
		}
		
		// GET /stats -> get statistics
		case 'stats':
		{
			req.log('stats');
			return res.endJson(stats);
		}
		
		// GET /tiles... -> sth with tiles
		case 'tiles':
		{
			// need to have at least 5 path segments:
			//   /tiles/map/z/x/y.png
			//      1    2  3 4 5
			if(path.length < 6)
			{
				req.log(404);
				return res.endError(404, 'wrong tile path');
			}
			
			// split call
			var map = path[2],  
				z = parseInt(path[3]), 
				x = parseInt(path[4]), 
				y = parseInt(path[5]);
			
			// check if the map-name is known
			if(!config.maps[map])
				res.endError(404, 'unknown map');
			
			// check that the zoom is inside the renderer-range
			if(z < config.maps[map].minz || z > config.maps[map].maxz)
				res.endError(404, 'z out of range');
			
			// switch by action field
			switch(path[6] || '')
			{
				// dirty
				case 'dirty':
					
					// send the request to tirex, don't call back when finished
					sendToTirex(map, z, x, y);
					
					// send response to the client
					res.endJson('submitted to tirex');
					
					// print the request to the log
					return req.log('dirty');
				
				
				
				// status
				case 'status':
					
					// fetch the status of the log, call back when finished
					fetchTileStatus(map, z, x, y, function(status)
					{
						// send the status to the client
						return res.endJson(status);
					});
					
					// print the request to the log
					return req.log('status');
				
				
				
				// fetch tile
				case '':
					
					// count the request
					stats.tiles_requested++;
					stats.maps[map].tiles_requested++;
					
					// try to fetch the tile
					return fetchTile(map, z, x, y, function(buffer)
					{
						// no tile found
						if(!buffer)
						{
							// count the request
							stats.tiles_rendered++;
							stats.maps[map].tiles_rendered++;
							
							// send the request to tirex
							//  TODO: add timeout
							return sendToTirex(map, z, x, y, function(success)
							{
								// if the rendering did not complete
								if(!success)
								{
									// send an server error
									res.endError(500, 'render error');
									
									// print the request to the log
									return req.log(500);
								}
								
								// tile rendered, re-read it from disk
								return fetchTile(map, z, x, y, function(buffer)
								{
									// if the rendering did not complete
									if(!buffer)
									{
										// send an server error
										res.endError(500, 'render error');
										
										// print the request to the log
										return req.log(500);
									}
									
									// tile rendered, send it to client
									res.endTile(buffer, map, z, x, y);
									
									// print the request to the log
									return req.log('tile from tirex');
								});
							});
							
						}
						
						// count the request
						stats.tiles_from_cache++;
						stats.maps[map].tiles_from_cache++;
						
						// tile found, send it to client
						res.endTile(buffer, map, z, x, y);
						
						// print the request to the log
						return req.log('tile from cache');
					});
				
				
				
				// unknown action
				default:
				
					// send an 400 Bad Request to the client
					res.endError(400, 'unknown action');
					
					// print the request to the log
					return req.log(400);
			}
		}
		
		// other requests
		default:
		{
			// try to serve as static file
			return handleStatic(req, res, function(handled) {
				
				// the file was found
				if(handled)
				{
					// count the request
					stats.static_handled++;
					
					return req.log('static');
				}
				
				// the file was not found
				req.log(404);
				return res.endError(404, 'file not found');
			});
		}
	}
});

// log exceptions
server.on('clientError', function(exception)
{
	console.log("exception:", exception);
});


// create an udp4-socket to chat with the master server
var master = dgram.createSocket('unix_dgram');

// read the tirex-config
console.log('reading config from %s', config.configdir);
config.maps = readConfig(config.configdir);

// iterate over all maps
for(k in config.maps)
{
	// one of them
	var map = config.maps[k];
	
	// create stats array for map
	stats.maps[map.name] = 
	{
		// number of tiles requestet for this map
		tiles_requested: 0, 
		
		// number of tiles served from cache for this map
		tiles_from_cache: 0, 
		
		// number of tiles sent to tirex for this map
		tiles_rendered: 0
	};
	
	// print that we know about it
	console.log('   %s (%s) [%d-%d]: %s', map.name, map.renderer.name, map.minz, map.maxz, map.tiledir);
};

// parse the config
//  all fs-ops are synchronous for simplicity, 
//  concurrency does not matter in this stage
function readConfig(dir)
{
	// scan folder for rederer-configurations
	var files = fs.readdirSync(dir + '/renderer/');
	
	// maps configuration
	var maps = {};
	
	// iterate over the found files
	files.forEach(function(file)
	{
		// path to file
		var filepath = dir + '/renderer/' + file;
		
		// check if this is a config file
		if(!isConfigFile(filepath))
			return;
		
		// read the file contents
		var data = fs.readFileSync(filepath, 'utf-8');
		
		// parse the config
		var renderer = parseConfig(data);
		
		// scan renderer-folder for map-configurations
		var mapfiles = fs.readdirSync(dir + '/renderer/' + renderer.name + '/');
		
		// iterate over the found files
		mapfiles.forEach(function(mapfile)
		{
			// path to file
			var mapfilepath = dir + '/renderer/' + renderer.name + '/' + mapfile;
			
			// check if this is a config file
			if(!isConfigFile(mapfilepath))
				return;
			
			// read the file contents
			var mapdata = fs.readFileSync(mapfilepath, 'utf-8');
			
			// parse the config
			var map = parseConfig(mapdata);
			
			// ensure min- and maxz is set
			map.minz = map.minz || 0;
			map.maxz = map.maxz || 0;
			
			// also store renderer info
			map.renderer = renderer;
			
			// store map config
			maps[map.name] = map;
		});
	});
	
	return maps;
}

// check if a file could be a config file
function isConfigFile(filepath)
{
	// we're only looking for .conf files
	if(!/\.conf$/.test(filepath))
		return false;
	
	// we're only looking for files
	var stats = fs.statSync(filepath);
	if(!stats.isFile())
		return false;
	
	return true;
}

// parse a tirex-config-file
function parseConfig(str)
{
	// config-files are read line-by-line
	var lines = str.split('\n');
	
	// infos we want from the renderer
	var config = {};
	
	lines.forEach(function(line)
	{
		// skip empty lines
		if(line.length == 0)
			return;
		
		// skip comments
		if(line[0] == '#' || line[0] == '$')
			return;
		
		// split at delimiter
		var idx = line.indexOf('=');
		
		// no delimiter found
		if(idx < 0)
			return;
		
		// extract k & v
		var k = line.substr(0, idx), 
			v = line.substr(idx+1);
		
		config[k] = v;
	});
	
	return config;
}




// try to serve the file from disc
function handleStatic(req, res, cb) {
	
	// check for directory traversal attacks
	if(req.url.substr(0, 2) == '/.')
		cb(false);
	
	// url to file path mapping
	var file = './'+req.url;
	
	// check if the file exists on disk
	fs.stat(file, function(err, stats) {
		
		// stat callback
		if(stats && stats.isFile())
		{
			// it exists and it is a file (symlinks are resolved)
			
			// generate filename & mimetype
			var filename = path.basename(file), 
				extension = path.extname(file), 
				mimetype = mime.lookup(extension);
			
			// write header line
			res.writeHead(200, {
				'Content-Type': mimetype, 
				
				// 2419200 = 4 Weeks × 7 Days × 24 Hours × 60 Minutes × 60 Seconds
				'Cache-Control': 'max-age=2419200, must-revalidate'
			});
			
			// the file was found and the request method is ok, go on and serve the file's content
			var stream = fs.createReadStream(file);
			
			// pump from input stream to response
			util.pump(stream, res, function(err) {
				
				// the pump operation has ended (with or without error), so close the client connection
				res.end();
				
				// callback with found message
				return cb(true);
			});
		}
		
		// there is no such file -> not found
		else return cb(false);
	});
}

// send to tirex and call back when the tile got rendered
//  if(cb) cb(success)
function sendToTirex(map, z, x, y, cb)
{
	// reduce the coordinates to metatile coordinates
	var mx = x - x%8;
	var my = y - y%8;
	
	// the request-id
	var reqid = 'nodets-' + stats.tiles_requested;
	
	// store the callback under this id
	if(cb) pending_requests[reqid] = cb;
	
	//TODO: add a timeout that clears this request when tirex won't answer in time
	
	var msg = {
		id:   reqid, 
		type: 'metatile_enqueue_request',
		prio: config.prio,
		map:  map,
		x:    mx,
		y:    my,
		z:    z
	};
	
	console.log('tirex>>> ', msg);
	
	// send it to tirex
	var buf = new Buffer(serialize_tirex_msg(msg));
	master.send(buf, 0, buf.length, config.master_socket);
}

master.on('message', function(buf, rinfo)
{
	// deserialize the message from tirex
	var msg = deserialize_tirex_msg(buf.toString('ascii', 0, rinfo.size));
	
	console.log('tirex<<< ', msg);
	
	// check this request has an id
	if(!msg.id)
		return;
	
	// if a request is pending for this id
	if(pending_requests[msg.id])
	{
		// fetch it from the list of ending requests
		cb = pending_requests[msg.id];
		
		// delete the stored request
		delete pending_requests[msg.id];
		console.log('now %d pending requests', count(pending_requests));
		
		// and call it with the result of the request
		cb(msg.result == 'ok');
	}
});

// fetch the tile status
//  if(cb) cb(status)
function fetchTileStatus(map, z, x, y, cb)
{
	console.log('fetchTileStatus');
	cb(false);
}

// fetch the tile
//  if(cb) cb(buffer)
function fetchTile(map, z, x, y, cb)
{
	// convert z/x/y to a metafile-path
	var metafile = zxy_to_metafile(map, z, x, y);
	
	// if the file did not exists
	if(!metafile)
	{
		if(cb) cb();
		return;
	}
	
	// try to open the file
	fs.open(metafile, 'r', null, function(err, fd) {
		// error opening the fille, call back without result
		if(err)
		{
			if(cb) cb();
			return;
		}
		
		// create a buffer fo the metatile header
		var buffer = new Buffer(metatile_header_size);
		
		// try to read the metatile header from disk
		fs.read(fd, buffer, 0, metatile_header_size, 0, function(err, bytesRead)
		{
			// the metatile header could not be read, call back without result
			if (err || bytesRead !== metatile_header_size)
			{
				// close file descriptor
				fs.close(fs);
				
				// call back without result
				if(cb) cb();
				return;
			}
			
			// offset into lookup table in header
			var pib = 20 + ((y%8) * 8) + ((x%8) * 64);
			
			// read file offset and size of the real tile from the header
			var offset = buffer.getLong(pib);
			var size   = buffer.getLong(pib+4);
			
			// create a buffer for the png data
			var png = new Buffer(size);
			
			// read the png from disk
			fs.read(fd, png, 0, size, offset, function(err, bytesRead)
			{
				// the png could not be read
				if (err || bytesRead !== size)
				{
					// close file descriptor
					fs.close(fs);
					
					// call back without result
					if(cb) cb();
					return;
				}
				
				// close file descriptor
				fs.close(fd);
				
				// call back with png
				if(cb) cb(png);
				return;
			});
		});
	});
}

function zxy_to_metafile(map, z, x, y)
{
	var mx = x - x%8;
	var my = y - y%8;
	
	var limit = 1 << z;
	if (x < 0 || x >= limit || y < 0 || y >= limit)
		return;
	
	var path_components = [], i, v;
	for (i=0; i <= 4; i++) {
		v = mx & 0x0f;
		v <<= 4;
		v |= (my & 0x0f);
		mx >>= 4;
		my >>= 4;
		path_components.unshift(v);
	}
	
	path_components.unshift(z);
	
	return path.join(
		config.maps[map].tiledir, 
		path_components.join('/') + '.meta'
	);
}

function serialize_tirex_msg(msg)
{
	var string = '', k;
	
	for (k in msg)
		string += k + '=' + msg[k] + '\n';
	
	return string;
}

function deserialize_tirex_msg(string)
{
	var lines = string.split('\n');
	var msg = {}, i;
	
	for (i=0; i < lines.length; i++)
	{
		var line = lines[i].split('=');
		if (line[0] !== '')
			msg[line[0]] = line[1];
	}
	
	return msg;
}


// connect to the master
console.log('');
console.log('opening dgram socket to master');
master.bind('');
master.on('listening', function()
{
	// start the web-server
	console.log('listening on http://0.0.0.0:%d/', config.http_port);
	server.listen(config.http_port);
});
