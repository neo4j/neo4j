/**
 * Persisted key/value store for webadmin. Used to store stuff like lists of
 * configured servers etc.
 */
wa.PropertyStorage = function(storageUrl) {
    /**
     * Client-side property cache.
     */
    this.cache = {};

    /**
     * Url to property storage service.
     */
    this.storageUrl = storageUrl;

};

/**
 * Get a value.
 * 
 * @param key
 *            {String} Property key
 * @param cb
 *            {function} Callback that will be called with the value.
 */
wa.PropertyStorage.prototype.get = function(key, cb)
{
    if (typeof (this.cache[key]) === "undefined")
    {
        var cache = this.cache;
        neo4j.Web.get(this.storageUrl + key, (function(key, cb) {
            return function(data) {
                var value = data === "undefined" ? undefined
                        : typeof (data) === "string" ? JSON.parse(data) : data;
                cache[key] = value;
                cb(key, value);
            };
        })(key, cb));
    } else
    {
        value(this.cache[key]);
    }
};

/**
 * Set a value.
 * 
 * @param key
 *            {String} Property key
 * @param value
 *            {Object} Value to set
 * @param cb
 *            {function} Callback that will be called when change has been
 *            applied.
 */
wa.PropertyStorage.prototype.set = function(key, value, cb)
{
    

    var cache = this.cache;
    neo4j.Web.post(this.storageUrl + key, value, (function(cb, key, value) {
        return function() {
            cache[key] = value;

            if (typeof (cb) === "function")
            {
                cb();
            }
        };
    })(cb, key, value));
};
