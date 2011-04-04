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

/*
 TODO
   - ip-limits (tiles)
*/

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
	prio: 2, 
	
	// timeout in milisecods (seconds × 1000) for render-requests, before the browser get's a 404
	timeout: 5000, 
	
	// specify different cache times for different zoom levels
	cache: [
		{
			//minz: 0, 
			maxz: 5, 
			
			// 28 days
			seconds: 2419200
		}, 
		{
			minz: 6, 
			maxz: 9, 
			
			// 7 days
			seconds: 604800
		}, 
		{
			minz: 10, 
			maxz: 13, 
			
			// 1 day
			seconds: 86400
		}, 
		{
			minz: 14, 
			//maxz: 999, 
			
			// 9 hours
			seconds: 32400
		}
	], 
	
	// cache time for static files (28 days)
	cacheStatic: 2419200, 
	
	// reference-file for dirty tiles
	dirtyRef: '/var/lib/tirex/tiles/dirtyRef', 
	
	// this will be filled from the tirex config
	maps: {}
}

var http = require('http');
var url = require('url');
var fs = require('fs');
var path = require('path');
var dgram = require('dgram');
var mime = require('mime');
var util = require('util');

// string to identify the server via http
var serverString = 'tileserver2.js using nodejs (http://svn.toolserver.org/svnroot/mazder/node-tileserver/)';

// size in bytes of metatile header
var metatile_header_size = 20 + 8 * 64;

// open http connections waiting for answer from tirex
var pending_requests = {};

// incremental counter used to generate unique tirex request ids
var tirex_req_id = 0;

// statistics
var stats = 
{
	// when the server was started
	started: new Date(), 
	
	// number of recieved requests
	recieved_requests: 0, 
	
	// number of currently pending requests
	pending_requests: 0, 
	
	// number of delivered static files
	static_delivered: 0, 
	
	// number of delivered meta information
	meta_delivered: 0, 
	
	// overall number of tiles delivered (interpolated by interpolate-function call)
	tiles_delivered: 0, 
		
	// overall number of tiles sent to tirex (interpolated by interpolate-function call)
	tiles_rendered: 0, 
	
	// overall number of tiles that hit the timeout before returning from tirex (interpolated by interpolate-function call)
	tiles_rendered_timeouted: 0, 
	
	// statistics on a per-map basis
	maps: {}, 
	
	// init the maps structure based on the configured maps
	initMaps: function()
	{
		// iterate over all maps
		for(k in config.maps)
		{
			// one of them
			var map = config.maps[k];
			
			// create stats array for map
			this.maps[map.name] = 
			{
				// overall number of tiles delivered (interpolated by interpolate-function call)
				tiles_delivered: 0, 
					
				// overall number of tiles sent to tirex (interpolated by interpolate-function call)
				tiles_rendered: 0, 
				
				// overall number of tiles that hit the timeout before returning from tirex (interpolated by interpolate-function call)
				tiles_rendered_timeouted: 0, 
				
				// statistics on a per-map-and-zoomlevel basis
				zooms: {}
			};
			
			// create stats array for the zoom levels
			for(z = map.minz; z <= map.maxz; z++)
			{
				this.maps[map.name].zooms[z] = 
				{
					// number of tiles delivered
					tiles_delivered: 0, 
					
					// number of tiles sent to tirex
					tiles_rendered: 0, 
					
					// number of tiles that hit the timeout before returning from tirex
					tiles_rendered_timeouted: 0
				}
			}
		}
	}, 
	
	// from the deep-nested numbers inside the maps-object
	// (all hits ar counted on a per-map-and-zoomlevel basis), 
	// we'll accumulate the per-map and overall numbers
	accumulate: function()
	{
		// overall number of tiles delivered
		this.tiles_delivered = 0;
		
		// overall number of tiles sent to tirex
		this.tiles_rendered = 0;
		
		// overall number of tiles that hit the timeout before returning from tirex
		this.tiles_rendered_timeouted = 0;
		
		// iterate over all maps
		for(mapname in this.maps)
		{
			// a map
			var map = this.maps[mapname];
			
			// per-map number of tiles delivered
			map.tiles_delivered = 0;
			
			// per-map number of tiles sent to tirex
			map.tiles_rendered = 0;
			
			// per-map number of tiles that hit the timeout before returning from tirex
			map.tiles_rendered_timeouted = 0;
			
			// iterate over all zoom levels
			for(zoomlevel in map.zooms)
			{
				// statistics on a per-map-and-zoomlevel basis
				var zoom = map.zooms[zoomlevel];
				
				// accumulate to a per-map basis
				map.tiles_delivered += zoom.tiles_delivered;
				map.tiles_rendered += zoom.tiles_rendered;
				map.tiles_rendered_timeouted += zoom.tiles_rendered_timeouted;
			}
			
			// accumulate to am overall basis
			this.tiles_delivered += map.tiles_delivered;
			this.tiles_rendered += map.tiles_rendered;
			this.tiles_rendered_timeouted += map.tiles_rendered_timeouted;
		}
		
		// return statistics object
		return this;
	}
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
	desc = desc || '';
	
	headers = headers || {};
	headers['Content-Type'] = headers['Content-Type'] || 'text/plain';
	headers['X-Error'] = desc
	
	this.writeHead(code, headers);
	this.end(desc);
}

// end with a tile
http.ServerResponse.prototype.endTile = function(png, map, z, x, y)
{
	// iterate over all cache settings
	for(var i=0; i<config.cache.length; i++)
	{
		// one cache setting
		var cache = config.cache[i];
		
		// check if we're inside the min/max range
		if(cache.minz && cache.minz > z)
			continue;
		
		if(cache.maxz && cache.maxz < z)
			continue;
		
		// yep, we are, use this cache config
		break;
	}
	
	var now = new Date();
	var expires = new Date(now.getTime() + cache.seconds*1000);
	
	this.writeHead(200, {
		
		'Server': serverString, 
		'Content-Type': 'image/png', 
		'Content-Length': png.length, 
		
		'Date': now.toGMTString(), 
		'Expires': expires.toGMTString(), 
		'Cache-Control': 'max-age='+cache.seconds, 
		
		'X-Map': map, 
		'X-Coord': z+'/'+x+'/'+y, 
		'X-License': 'Map data (c) OpenStreetMap contributors, CC-BY-SA'
	});
	
	this.end(png);
}

// log an request to the console
http.IncomingMessage.prototype.log = function(usage, comment)
{
	// numeric resonses should be enhanced with status strings
	if(typeof usage == 'number')
		usage = usage + ' ' + http.STATUS_CODES[usage];
	
	if(comment)
		usage += ' ('+comment+')';
	
	// print the message
	console.log('%s %s -> %s', this.method, this.url, usage);
}

// the http server
var server = http.createServer(function(req, res)
{
	// count the request
	stats.recieved_requests++;
	
	// only GET requests are acceptable
	if(req.method != 'GET')
	{
		res.endError(405, 'only GET requests are handled');
		return req.log(405, 'only GET requests are handled');
	}
	
	// analyze the path
	var path = url.parse(req.url).pathname.split('/');
	
	// the first part of the url decides what route we'll take
	switch(path[1] || '')
	{
		// GET /maps -> list the maps
		case 'maps':
		{
			var maps = {};
			for(name in config.maps)
			{
				maps[name] = {
					// minimal zoom
					minz: config.maps[name].minz, 
					
					// maximal zoom
					maxz: config.maps[name].maxz
				}
			}
			
			// count a meta-request
			stats.meta_delivered++;
			
			res.endJson(maps);
			return req.log('maps');
		}
		
		// GET /stats -> get statistics
		case 'stats':
		{
			// count a meta-request
			stats.meta_delivered++;
			
			res.endJson(stats.accumulate());
			return req.log('stats');
		}
		
		// GET /tiles... -> sth with tiles
		case 'tiles':
		{
			// need to have at least 5 path segments:
			//   /tiles/map/z/x/y.png
			//      1    2  3 4 5
			if(path.length < 6)
			{
				res.endError(404, 'wrong tile path');
				return req.log(404, 'wrong tile path');
			}
			
			// split call
			var map = path[2],  
				z = parseInt(path[3]), 
				x = parseInt(path[4]), 
				y = parseInt(path[5]);
			
			// check if the map-name is known
			if(!config.maps[map])
			{
				res.endError(404, 'unknown map');
				return req.log(404, 'unknown map');
			}
			
			// check that the zoom is inside the renderer-range
			if(z < config.maps[map].minz || z > config.maps[map].maxz)
			{
				res.endError(404, 'z out of range');
				return req.log(404, 'z out of range');
			}
			
			// check that the x & y is inside the range
			var w = Math.pow(2, z)-1;
			if(x > w || x < 0)
			{
				res.endError(404, 'x out of range');
				return req.log(404, 'x out of range');
			}
			
			if(y > w || y < 0)
			{
				res.endError(404, 'y out of range');
				return req.log(404, 'y out of range');
			}
			
			// switch by action field
			switch(path[6] || '')
			{
				// dirty
				case 'dirty':
					
					// count a render-request
					stats.maps[map].zooms[z].tiles_rendered++;
					
					// send the request to tirex, don't call back when finished
					var reqid = sendToTirex(map, z, x, y);
					
					// send response to the client
					res.endJson({
						'status': 'sent to tirex', 
						'reqid': reqid
					});
					
					// print the request to the log
					return req.log('dirty');
				
				
				
				// status
				case 'status':
					
					// count a meta-request
					stats.meta_delivered++;
					
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
					
					// try to fetch the tile
					return fetchTile(map, z, x, y, function(png)
					{
						// no tile found
						if(!png)
						{
							// count a render-request
							stats.maps[map].zooms[z].tiles_rendered++;
							
							// send the request to tirex
							return sendToTirex(map, z, x, y, function(success)
							{
								// if the rendering did not complete
								if(!success)
								{
									// send an server error
									res.endError(500, 'render error');
									
									// print the request to the log
									return req.log(500, 'render error');
								}
								
								// tile rendered, re-read it from disk
								return fetchTile(map, z, x, y, function(png, meta)
								{
									// if the rendering did not complete
									if(!png)
									{
										// send an server error
										res.endError(500, 'render error');
										
										// print the request to the log
										return req.log(500, 'render error');
									}
									
									// count a delivery-request
									stats.maps[map].zooms[z].tiles_delivered++;
									
									// tile rendered, send it to client
									res.endTile(png, map, z, x, y);
									
									// print the request to the log
									return req.log('tile from tirex');
								});
							});
							
						}
						
						// count a delivery-request
						stats.maps[map].zooms[z].tiles_delivered++;
						
						// tile found, send it to client
						res.endTile(png, map, z, x, y);
						
						// print the request to the log
						return req.log('tile from cache');
					});
				
				
				
				// unknown action
				default:
				
					// send an 400 Bad Request to the client
					res.endError(400, 'unknown action');
					
					// print the request to the log
					return req.log(400, 'unknown action');
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
					// count a delivery-request
					stats.static_delivered++;
					
					// print the request to the log
					return req.log('static');
				}
				
				// the file was not found
				req.log(404, 'file not found');
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

// create an unix-socket to talk with the master
var master = dgram.createSocket('unix_dgram');

// read the tirex-config
console.log('reading config from %s', config.configdir);
config.maps = readConfig(config.configdir);

// initiate the per-map- and per-zoom-statistics
stats.initMaps();

// read the reference-time
if(config.dirtyRef) fs.stat(config.dirtyRef, function(err, stats)
{
	// an error -> no ref time available
	if(err)
	{
		// warn the admin
		return console.warn('could not detect dirty reference time from %s', config.dirtyRef);
	}
	
	// safe the references change time as reference
	config.dirtyRefTime = stats.ctime;
	console.log('tiles older then %s will be sent to tirex as dirty', config.dirtyRefTime.toGMTString());
});

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
	
	// yep, this is a config-file
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
		
		// store in the config object
		config[k] = v;
	});
	
	// and return with the config
	return config;
}




// try to serve the file from disc
function handleStatic(req, res, cb)
{
	// parse url
	var pathname = url.parse(req.url).pathname;
	
	// directory index
	if(pathname == '/')
		pathname = '/index.html';
	
	// check for directory traversal attacks
	if(pathname.indexOf('/.') != -1)
		cb(false);
	
	// url to file path mapping
	var file = 'res'+pathname;
	
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
			
			var now = new Date();
			var expires = new Date(now.getTime() + config.cacheStatic*1000);
			
			// write header line
			res.writeHead(200, {
				'Server': serverString, 
				'Content-Type': mimetype, 
		
				'Date': now.toGMTString(), 
				'Expires': expires.toGMTString(), 
				'Cache-Control': 'max-age='+config.cacheStatic, 
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
	var reqid = 'nodets-' + process.pid + '-' + (tirex_req_id++);
	
	// store the callback under this id
	if(cb)
	{
		pending_requests[reqid] = cb;
		stats.pending_requests++;
	}
	
	// construct a message to tirex
	var msg = {
		// the request-id - tirex will call back with this id, once the rendering has finished
		id:   reqid, 
		
		// type of message
		type: 'metatile_enqueue_request',
		
		// rendering priority
		prio: config.prio,
		
		// which map to render
		map:  map,
		
		// tile coordinates
		x:    mx,
		y:    my,
		z:    z
	};
	
	// log the tirex-send to the console
	console.log('tirex>>> ', msg.id);
	
	// send it to tirex
	var buf = new Buffer(serialize_tirex_msg(msg));
	master.send(buf, 0, buf.length, config.master_socket);
	
	// set the timeout to cancel the request
	setTimeout(function()
	{
		// if the request is already answered, don't bug around
		if(!pending_requests[msg.id])
			return;
		
		// delete the pending request
		delete pending_requests[msg.id];
		stats.pending_requests--;
		
		// count a timeout
		stats.maps[map].zooms[z].tiles_rendered_timeouted++;
		
		// call back with negative result
		cb(false);
	}, config.timeout);
	
	// return the reqid
	return reqid;
}

// when a message arrives from tirex
master.on('message', function(buf, rinfo)
{
	// deserialize the message from tirex
	var msg = deserialize_tirex_msg(buf.toString('ascii', 0, rinfo.size));
	
	// check this request has an id
	if(!msg.id)
		return;
	
	// log the incoming message to the console
	console.log('tirex<<< ', msg.id);
	
	// if a request is pending for this id
	if(pending_requests[msg.id])
	{
		// fetch it from the list of ending requests
		cb = pending_requests[msg.id];
		
		// delete the stored request
		delete pending_requests[msg.id];
		stats.pending_requests--;
		
		// and call it with the result of the request
		cb(msg.result == 'ok');
	}
});

// fetch the tile status
//  cb(status)
function fetchTileStatus(map, z, x, y, cb)
{
	// convert z/x/y to a metafile-path
	var metafile = zxy_to_metafile(map, z, x, y);
	
	// if the path could not be constructed
	if(!metafile)
	{
		// return without response
		return cb();
	}
	
	// fetch the stats
	return fs.stat(metafile, function(err, stats)
	{
		// callback
		if(err)
		{
			return cb({
				'status': 'not found'
			});
		}
		
		// if this server has a dirty-reference time
		if(config.dirtyRefTime)
		{
			// return if the tile is clean
			return cb({
				'status': config.dirtyRefTime >= stats.mtime ? 'dirty' : 'clean', 
				'refTime': config.dirtyRefTime, 
				'lastRendered': stats.mtime
			});
		}
		
		// without a referenct time
		else
		{
			// the tile is neither clean nor dirty
			return cb({
				'status': 'unknown', 
				'lastRendered': stats.mtime
			});
		}
	});
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
	
	// try to fetch tile stats
	return fs.stat(metafile, function(err, stats)
	{
		// error stat'ing the file, call back without result
		if(err)
		{
			if(cb) cb();
			return;
		}
		
		// if a dirty time is configured and the tile is older
		if(config.dirtyRefTime && config.dirtyRefTime >= stats.mtime)
		{
			// send the tile to tirex
			sendToTirex(map, z, x, y);
		}
		
		// try to open the file
		return fs.open(metafile, 'r', null, function(err, fd)
		{
			// error opening the file, call back without result
			if(err)
			{
				if(cb) cb();
				return;
			}
			
			// create a buffer fo the metatile header
			var buffer = new Buffer(metatile_header_size);
		
			// try to read the metatile header from disk
			return fs.read(fd, buffer, 0, metatile_header_size, 0, function(err, bytesRead)
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
				return fs.read(fd, png, 0, size, offset, function(err, bytesRead)
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
	});
}

// convert map, z, x, y to a metafile
function zxy_to_metafile(map, z, x, y)
{
	// reduce tile-coords to metatile-coords
	var mx = x - x%8;
	var my = y - y%8;
	
	// do some bit-shifting magic...
	var limit = 1 << z;
	if (x < 0 || x >= limit || y < 0 || y >= limit)
		return;
	
	// and even more...
	var path_components = [], i, v;
	for (i=0; i <= 4; i++) {
		v = mx & 0x0f;
		v <<= 4;
		v |= (my & 0x0f);
		mx >>= 4;
		my >>= 4;
		path_components.unshift(v);
	}
	
	// add the zoom to the left of the path
	path_components.unshift(z);
	
	// now join the tiledir and the calculated path
	return path.join(
		config.maps[map].tiledir, 
		path_components.join('/') + '.meta'
	);
}

// serialize a js-object into a tirex-message
function serialize_tirex_msg(msg)
{
	var string = '', k;
	
	for (k in msg)
		string += k + '=' + msg[k] + '\n';
	
	return string;
}

// deserialize a tirex-message into a js-object
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
console.log('connecting to tirex-master');
master.bind('');

// once the socket is open
master.on('listening', function()
{
	console.log('listening on http://0.0.0.0:%d/', config.http_port);
	
	// iterate over all maps
	for(k in config.maps)
	{
		// one of them
		var map = config.maps[k];
		
		// print that we know about it
		console.log('   %s (%s) [%d-%d]: %s', map.name, map.renderer.name, map.minz, map.maxz, map.tiledir);
	}
	console.log('');
	
	// start the web-server
	server.listen(config.http_port);
});

