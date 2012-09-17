var fs = require('fs');
var url = require('url');
var path = require('path');
var qs = require('querystring');
var decoder = require('./decoder.node');
var Buffer = require('buffer').Buffer;
var knox = require('knox');
var http = require('http');
var TileJSON = require('tilejson');
var crypto = require('crypto');
var get = require('get');
var blank = fs.readFileSync(__dirname + '/blank.png');

module.exports = S3;
require('util').inherits(S3, TileJSON);
function S3(uri, callback) {
    this._uri = uri;
    this._cacheSolid;
    this._cacheMasks = {};
    this._cachePacks = [];
    this._stats = { get: 0, put: 0, noop: 0 };
    this._prepare = function(url) { return url };
    return TileJSON.call(this, uri, function(err, source) {
        // Increase number of connections that can be made to S3.
        // Can be specified manually in data for cases where ulimit
        // has been pushed higher.
        // @TODO consider using node-posix to determine the best value here.
        // @TODO this may belong upstream in node-tilejson.
        if (source && source.data) ['tiles', 'grids'].forEach(function(key) {
            if (!source.data[key] || !source.data[key].length) return;
            var hostname = url.parse(source.data[key][0]).hostname;
            var agent = http.globalAgent || http.getAgent(hostname, '80');
            agent.maxSockets = source.data.maxSockets || 128;
        });
        // Allow mtime for source to be determined by JSON data.
        if (source && source.data.mtime)
            source.mtime = new Date(source.data.mtime);
        // Allow a prepare function body to be provided for altering a URL
        // based on z/x/y conditions.
        if (source && source.data.prepare)
            source._prepare = Function('url,z,x,y', source.data.prepare);
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

S3.prototype._loadTile = function(z, x, y, callback, all) {
    if (!this.data) return callback(new Error('Tilesource not loaded'));
    if (!this.data.tiles) return callback(new Error('Tile does not exist'));

    var url = this._prepareURL(this.data.tiles[0], z, x, y);
    var range = this._packRange(z,x,y);
    var headers = { Connection:'Keep-Alive' };
    if (range && !all) headers.Range = 'bytes=' + range.join('-');
    var l = 'get ' + [z,x,y].join(',') + ' ' + (range ? range.join('-') : '');
    // console.time(l);
    new get({
        uri:url,
        timeout:this.timeout,
        headers:headers
    }).asBuffer(function(err, data, headers) {
        // console.timeEnd(l);
        // for copy
        if (err && (err.status === 404 || err.status === 403)) return callback(null, blank);
        if (err) return callback(err.status >= 400 && err.status < 500 ? new Error('Tile does not exist') : err);

        // No detectable mimetype implies a blank range in a packed tile.
        var mimetype = getMimeType(data);
        if (!mimetype) return callback(new Error('Tile does not exist'));

        var modified = headers['last-modified'] ? new Date(headers['last-modified']) : new Date;
        var responseHeaders = {
            'Type-Content': mimetype,
            'Last-Modified': modified,
            'ETag': headers['etag'] || (headers['content-length'] + '-' + +modified)
        };
        if (headers['cache-control']) {
            responseHeaders['Cache-Control'] = headers['cache-control'];
        }

        callback(null, data, responseHeaders);
    });
};

// Select a tile from S3. Scheme is XYZ.
S3.prototype.getTile = function(z, x, y, callback) {
    var s3 = this;
    if (this.data.maskLevel && z > this.data.maskLevel) {
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
    this._cachePacks.push({ z:z, x:x, y:y, data: data });

    if (this._flushing) {
        this.once('flush', callback);
    } else if (this._cachePacks.length > 10000) {
        this._flush();
        return callback();
    } else {
        return callback();
    }
};

S3.prototype._flush = function(callback) {
    var stats = this._stats;
    var packs = this._cachePacks.reduce(function(memo, t) {
        var key = this._prepareURL(this.putPath, t.z, t.x, t.y)
        memo[key] = memo[key] || [];
        memo[key].push(t);
        return memo;
    }.bind(this), {});
    this._cachePacks = [];
    this._flushing = true;

    var remaining = Object.keys(packs).length;

    var complete = function() {
        this._flushing = false;
        this.emit('flush');
        if (callback) return callback();
    }.bind(this);
    var error = function(err) {
        remaining = -1;
        complete();
    }.bind(this);
    var noop = function() {
        stats.noop++;
        remaining--;
        if (remaining !== 0) return;
        complete();
    };
    var done = function() {
        stats.put++;
        remaining--;
        if (remaining !== 0) return;
        complete();
    };
    var put = function(key, buffer, retries) {
        retries = retries || 0;
        var req = this.client.put(key, {
            'Content-Length': buffer.length,
            'Content-Type': 'image/png'
        });
        req.on('close', function(err) {
            if (retries > 5) return error(err);
            console.warn('Retrying %s', key);
            put(key, buffer, retries + 1);
        });
        req.on('error', function() {
            if (retries > 5) return error(err);
            console.warn('Retrying %s', key);
            put(key, buffer, retries + 1);
        });
        req.on('response', function(res) {
            if (res.statusCode === 200) return done();
            return error(new Error('S3 put failed: ' + res.statusCode));
        });
        req.end(buffer);
    }.bind(this);

    console.warn('Flushing %s packs', remaining);

    // If no packs to put, we're done.
    if (!remaining) return complete();

    for (var key in packs) (function(key, tiles) {
        this._loadTile(tiles[0].z, tiles[0].x, tiles[0].y, function(err, buffer) {
            var oldmd5 = crypto.createHash('md5').update(buffer || new Buffer(0)).digest('hex');

            if (err || buffer.length !== this.data.pack * this.data.packSize) {
                buffer = new Buffer(this.data.pack * this.data.packSize);
            }

            tiles.forEach(function(tile) {
                tile.data.copy(buffer, this._packRange(tile.z,tile.x,tile.y)[0]);
            }.bind(this));

            var newmd5 = crypto.createHash('md5').update(buffer).digest('hex');

            // Skip PUT if no change.
            if (oldmd5 === newmd5) return noop();

            // Skip PUT if dryrun.
            if (this.data.dryrun) return done();

            put(key, buffer);
        }.bind(this), true);
    }.bind(this))(key, packs[key]);
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
    case 'retry':
    case 'conditional':
    case 'dryrun':
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
    if (this.data.conditional) this.timeout = 0;

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
    // If flush is already in progress, wait for it to finish.
    if (this._flushing)
        return this.once('flush', function() { this.stopWriting(callback); }.bind(this));

    // Final flush ensures all tiles are written.
    this._flush(function() {
        // @TODO let tilelive or tilemill expect a stats object
        // and process output instead.
        console.log('\n');
        console.log('S3 stats');
        console.log('--------');
        console.log('GET:   %s', this._stats.get);
        console.log('PUT:   %s', this._stats.put);
        console.log('No-op: %s', this._stats.noop);
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
    }
};

