var fs = require('fs');
var url = require('url');
var path = require('path');
var qs = require('querystring');
var decoder = require('./decoder.node');
var queue = require('queue-async');
var Buffer = require('buffer').Buffer;
var Agent = require('agentkeepalive');
var knox = require('knox');
var http = require('http');
var crypto = require('crypto');
var get = require('get');
var blank = fs.readFileSync(__dirname + '/blank.png');
var agent = new Agent({
    maxSockets: 128,
    maxKeepAliveRequests: 0,
    maxKeepAliveTime: 30000
});

module.exports = S3;
require('util').inherits(S3, require('events').EventEmitter)
function S3(uri, callback) {
    this._uri = uri;
    this._cacheSolid;
    this._cacheMasks = {};
    this._cacheTiles = {};
    this._cachePacks = {};
    this._cacheRefs = {};
    this._stats = { get: 0, put: 0, copy: 0, noop: 0, transferIn: 0, transferOut: 0 };
    this._prepare = function(url) { return url };
    this._flushing = false;
    this.setMaxListeners(0);

    if (typeof callback !== 'function') throw new Error('callback required');
    if (typeof uri === 'string') uri = url.parse(uri, true);
    else if (typeof uri.query === 'string') uri.query = qs.parse(uri.query);

    if (!uri.pathname) {
        callback(new Error('Invalid URI ' + url.format(uri)));
        return;
    }

    if (uri.hostname === '.' || uri.hostname == '..') {
        uri.pathname = uri.hostname + uri.pathname;
        delete uri.hostname;
        delete uri.host;
    }
    uri.query = uri.query || {};

    var source = this;
    fs.readFile(uri.pathname, 'utf8', function(err, data) {
        if (err) return callback(err);
        data = data.replace(/^\s*\w+\s*\(\s*|\s*\)\s*;?\s*$/g, '');
        try { source.data = JSON.parse(data); }
        catch(err) { return callback(err); }

        source.data.id = source.data.id || path.basename(uri.pathname, path.extname(uri.pathname));
        source.timeout = 'timeout' in uri.query ? uri.query.timeout : 10000;
        source.open = true;

        // Increase number of connections that can be made to S3.
        // Can be specified manually in data for cases where ulimit
        // has been pushed higher.
        // @TODO the global agent is still used for putTile. Test a
        // switch to keepaliveagent for consistency.
        ['tiles', 'grids'].forEach(function(key) {
            if (!source.data[key] || !source.data[key].length) return;
            var hostname = url.parse(source.data[key][0]).hostname;
            var agent = http.globalAgent || http.getAgent(hostname, '80');
            agent.maxSockets = source.data.maxSockets || 128;
        });

        // Allow mtime for source to be determined by JSON data.
        if (source.data.mtime) source.mtime = new Date(source.data.mtime);

        // Allow a prepare function body to be provided for altering a URL
        // based on z/x/y conditions.
        if (source.data.prepare) source._prepare = Function('url,z,x,y', source.data.prepare);

        callback(err, source);
    });
};

S3.registerProtocols = function(tilelive) {
    tilelive.protocols['s3:'] = S3;
};

S3.list = function(filepath, callback) {
    filepath = path.resolve(filepath);
    fs.readdir(filepath, function(err, files) {
        if (err && err.code === 'ENOENT') return callback(null, {});
        if (err) return callback(err);
        for (var result = {}, i = 0; i < files.length; i++) {
            var name = files[i].match(/^([\w-]+)\.s3$/);
            if (name) result[name[1]] = 's3://' + path.join(filepath, name[0]);
        }
        callback(null, result);
    });
};

S3.findID = function(filepath, id, callback) {
    filepath = path.resolve(filepath);
    var file = path.join(filepath, id + '.s3');
    fs.stat(file, function(err, stats) {
        if (err) callback(err);
        else callback(null, 's3://' + file);
    });
};

S3.prototype.close = function(callback) {
    if (callback) callback(null);
};

S3.prototype._prepareURL = function(url, z, x, y) {
    var s = Math.sqrt(this.data.pack || 1);
    x = ((x/s)|0);
    y = ((y/s)|0);
    return (this._prepare(url,z,x,y)
        .replace(/\{prefix\}/g, (x%16).toString(16) + (y%16).toString(16))
        .replace(/\{z\}/g, z)
        .replace(/\{x\}/g, x)
        .replace(/\{y\}/g, (this.data.scheme === 'tms') ? (1 << z) - 1 - y : y));
};

S3.prototype._packRange = function(z, x, y) {
    if (!this.data.pack) return;
    var s = Math.sqrt(this.data.pack || 1);
    x = (x|0) % s;
    y = (y|0) % s;
    var i = (y*s) + x;
    return [i * this.data.packSize, (i+1) * this.data.packSize];
};

S3.prototype._loadTile = function(z, x, y, callback, type, all) {
    type = type || 'tiles';
    var url = this._prepareURL(this.data[type][0], z, x, y);
    var range = this._packRange(z,x,y);
    var headers = { Connection:'Keep-Alive' };
    if (range && !all) headers.Range = 'bytes=' + range.join('-');

    new get({
        uri:url,
        timeout:this.timeout,
        headers:headers,
        agent:agent
    }).asBuffer(function(err, data, headers) {
        // For copy
        if (err && (err.status === 404 || err.status === 403)) return callback(null, blank);
        if (err) return callback(err.status >= 400 && err.status < 500 ? new Error('Tile does not exist') : err);

        // For PUTs
        if (all) return callback(null, data);

        // No detectable mimetype implies a blank range in a packed tile.
        var mimetype = getMimeType(data);
        if (!mimetype) return callback(new Error('Tile does not exist'));

        var modified = headers['last-modified'] ? new Date(headers['last-modified']) : new Date;
        var responseHeaders = {
            'Content-Type': mimetype,
            'Last-Modified': modified,
            'ETag': headers['etag'] || (headers['content-length'] + '-' + +modified)
        };
        if (headers['cache-control']) {
            responseHeaders['Cache-Control'] = headers['cache-control'];
        }
        if (type === 'grids') {
            data = data.toString().replace(/^\s*\w+\s*\(|\)\s*;?\s*$/g, '');
            callback(null, JSON.parse(data), responseHeaders);
        } else {
            callback(null, data, responseHeaders);
        }
    });
};

// Select a tile from S3. Scheme is XYZ.
S3.prototype.getTile = function(z, x, y, callback) {
    if (!this.data) return callback(new Error('Tilesource not loaded'));
    if (!this.data.tiles) return callback(new Error('Tile does not exist'));
    if (this.data.maskLevel && z > this.data.maskLevel) {
        var s3 = this;
        this._getColor(z, x, y, function(err, color) {
            if (err) {
                callback(err);
            } else if (color === 255) {
                s3._getSolid(callback);
            } else if (color === 0) {
                callback(new Error('Tile does not exist'));
            } else {
                s3._loadTile(z, x, y, callback);
            }
        });
    } else {
        this._loadTile(z, x, y, callback);
    }
};

S3.prototype.getGrid = function(z, x, y, callback) {
    if (!this.data) return callback(new Error('Gridsource not loaded'));
    if (!this.data.grids) return callback(new Error('Grid does not exist'));
    this._loadTile(z, x, y, callback, 'grids');
};

S3.prototype._getSolid = function(callback) {
    if (this._cacheSolid) {
        var buffer = new Buffer(this._cacheSolid.data.length);
        this._cacheSolid.data.copy(buffer);
        return callback(null, buffer, this._cacheSolid.headers);
    }

    var s3 = this;
    var z = this.data.maskSolid[0];
    var x = this.data.maskSolid[1];
    var y = this.data.maskSolid[2];
    this._loadTile(z, x, y, function(err, data, headers) {
        if (err) return callback(err);
        s3._cacheSolid = { data: data, headers: headers };
        s3._getSolid(callback);
    });
};

// Calls callback with the alpha value or false of the tile.
S3.prototype._getColor = function(z, x, y, callback) {
    // Determine corresponding mask tile.
    var maskZ = this.data.maskLevel;
    var delta = z - maskZ;
    var maskX = x >> delta;
    var maskY = y >> delta;

    // Load mask tile.
    this._loadTileMask(maskZ, maskX, maskY, function(err, mask) {
        if (err) return callback(err);

        var size = 256 / (1 << delta);
        var minX = size * (x - (maskX << delta));
        var minY = size * (y - (maskY << delta));
        var maxX = minX + size;
        var maxY = minY + size;
        var pivot = mask[minY * 256 + minX];
        // Check that all alpha values in the ROI are identical. If they aren't,
        // we can't determine the color.
        for (var ys = minY; ys < maxY; ys++) {
            for (var xs = minX; xs < maxX; xs++) {
                if (mask[ys * 256 + xs] !== pivot) {
                    return callback(null, false);
                }
            }
        }

        callback(null, pivot);
    });
};

S3.prototype._loadTileMask = function(z, x, y, callback) {
    var key = z + ',' + x + ',' + y;
    if (this._cacheMasks[key])
        return callback(null, this._cacheMasks[key]);

    this._loadTile(z, x, y, function(err, buffer) {
        if (err) return callback(err);
        decoder.decode(buffer, function(err, mask) {
            if (err) return callback(err);
            // Store mask in cache. Reap cache if it grows beyond 1k objects.
            var keys = Object.keys(this._cacheMasks);
            if (keys.length > 1000) delete this._cacheMasks[keys[0]];
            this._cacheMasks[key] = mask;
            callback(null, mask);
        }.bind(this));
    }.bind(this));
};

// Inserts a tile into the S3 store. Scheme is XYZ.
S3.prototype.putTile = function(z, x, y, data, callback) {
    if (this.data.packSize && (data.length > this.data.packSize))
        console.warn('%s/%s/%s %s exceeds packSize %s', z, x, y, data.length, this.data.packSize);

    var key = this._prepareURL(this.putPath, z, x, y);

    // Flush interval here checks for cached packs before putting.
    if (Object.keys(this._cacheTiles).length >= 64 && !(key in this._cacheTiles)) this._flush();

    this._cacheTiles[key] = this._cacheTiles[key] || [];
    this._cacheTiles[key].push({ z:z, x:x, y:y, data: data });
    return callback();
};

S3.prototype._flush = function(callback) {
    // If no packs to put, we're done.
    if (!Object.keys(this._cacheTiles).length) return callback && callback();

    // Lock flushes.
    if (this._flushing) return callback
        ? this.once('flush', function() { this._flush(callback); }.bind(this))
        : false;

    var s3 = this;
    var stats = this._stats;
    var packs = this._cacheTiles;
    this._cacheTiles = {};
    this._flushing = true;

    var put = function(key, md5, buffer, done, retries) {
        retries = retries || 0;
        if (s3._cacheRefs[md5]) {
            stats.copy++;
            var req = s3.client.copy(s3._cacheRefs[md5], key);
        } else {
            stats.put++;
            stats.transferOut += buffer.length;
            var req = s3.client.put(key, {
                'Content-Length': buffer.length,
                'Content-Type': 'image/png'
            });
        }
        req.on('close', function(err) {
            if (retries > 5) return done(err);
            put(key, md5, buffer, done, retries + 1);
        });
        req.on('error', function() {
            if (retries > 5) return done(err);
            put(key, md5, buffer, done, retries + 1);
        });
        req.on('response', function(res) {
            if (res.statusCode === 200) {
                s3._cacheRefs[md5] = key;
                return done();
            }
            done(new Error('S3 put failed: ' + res.statusCode));
        });
        req.end(s3._cacheRefs[md5] ? null : buffer);
    };
    var get = function(key, tiles, done) {
        s3._loadTile(tiles[0].z, tiles[0].x, tiles[0].y, function(err, buffer) {
            // If a non-404 retry indefinitely.
            if (err && err.message !== 'Tile does not exist') return get(key, tiles, done);

            stats.get++;
            stats.transferIn += (buffer && buffer.length) || 0;

            // Build tile pack.
            var oldmd5 = crypto.createHash('md5').update(buffer || new Buffer(0)).digest('hex');
            if (s3.data.pack) {
                var packed = new Buffer(s3.data.pack * s3.data.packSize);

                if (!err && buffer.length === s3.data.pack * s3.data.packSize)
                    buffer.copy(packed);

                tiles.forEach(function(tile) {
                    tile.data.copy(packed, s3._packRange(tile.z,tile.x,tile.y)[0]);
                });
            // Not using packing, buffer is single tile.
            } else {
                var packed = tiles[0].data;
            }

            // Skip PUT if no change.
            var newmd5 = crypto.createHash('md5').update(packed).digest('hex');
            if (oldmd5 === newmd5) {
                stats.noop++;
                return done();
            }
            // Skip PUT if dryrun.
            if (s3.data.dryrun) {
                stats.put++;
                return done();
            }

            put(key, newmd5, packed, done);
        }, 'tiles', true);
    };

    var q = queue(8);
    for (var key in packs) q.defer(get, key, packs[key]);
    q.await(function(err) {
        s3._flushing = false;
        s3.emit('flush');
        return callback && callback();
    });
};

S3.prototype.getInfo = function(callback) {
    if (!this.data) return callback(new Error('Tilesource not loaded'));

    var data = {};
    for (var key in this.data) switch (key) {
    case 'awsKey':
    case 'awsSecret':
    case 'maskLevel':
    case 'maskSolid':
    case 'maxSockets':
    case 'dryrun':
    // Legacy (unused) keys filtered out.
    case 'retry':
    case 'conditional':
        break;
    default:
        data[key] = this.data[key];
        break;
    }
    return callback(null, data);
};

// Insert/update metadata.
S3.prototype.putInfo = function(data, callback) {
    return callback();
};

// Enter write mode.
S3.prototype.startWriting = function(callback) {
    if (!this.data.awsKey) return callback(new Error('AWS key required.'));
    if (!this.data.awsSecret) return callback(new Error('AWS secret required.'));
    if (this.data.dryrun) console.log('Dryrun enabled. No data will be PUT to S3.');

    this.timeout = 60000;
    this.uri = url.parse(this.data.tiles[0]);
    this.putPath = this.uri.pathname;
    this.client = knox.createClient({
        key: this.data.awsKey,
        secret: this.data.awsSecret,
        bucket: this.uri.hostname.split('.')[0],
        secure: false
    });

    return callback();
};

// Leave write mode.
S3.prototype.stopWriting = function(callback) {
    console.warn('');
    console.warn('S3 final flush (%s packs)...', Object.keys(this._cacheTiles).length);
    // Final flush ensures all tiles are written.
    this._flush(function() {
        // @TODO let tilelive or tilemill expect a stats object
        // and process output instead.
        console.log('\n');
        console.log('S3 stats');
        console.log('--------');
        console.log('GET:   %s', this._stats.get);
        console.log('PUT:   %s', this._stats.put);
        console.log('COPY:  %s', this._stats.copy);
        console.log('No-op: %s', this._stats.noop);
        console.log('In:    %s MB', (this._stats.transferIn / 1e6).toFixed(2));
        console.log('Out:   %s MB', (this._stats.transferOut / 1e6).toFixed(2));
        console.log('');
        return callback();
    }.bind(this));
};

S3.prototype.toJSON = function() {
    return url.format(this._uri);
};

function getMimeType(data) {
    if (data[0] === 0x89 && data[1] === 0x50 && data[2] === 0x4E &&
        data[3] === 0x47 && data[4] === 0x0D && data[5] === 0x0A &&
        data[6] === 0x1A && data[7] === 0x0A) {
        return 'image/png';
    } else if (data[0] === 0xFF && data[1] === 0xD8 &&
        data[data.length - 2] === 0xFF && data[data.length - 1] === 0xD9) {
        return 'image/jpeg';
    } else if (data[0] === 0x47 && data[1] === 0x49 && data[2] === 0x46 &&
        data[3] === 0x38 && (data[4] === 0x39 || data[4] === 0x37) &&
        data[5] === 0x61) {
        return 'image/gif';
    } else if (data[0] === 103 && data[1] === 114 &&
        data[2] === 105 && data[3] === 100) {
        return 'application/json';
    }
};

