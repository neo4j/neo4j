/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
    
    this.web = new neo4j.Web();

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
        this.web.get(this.storageUrl + key, (function(key, cb) {
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
    this.web.post(this.storageUrl + key, value, (function(cb, key, value) {
        return function() {
            cache[key] = value;

            if (typeof (cb) === "function")
            {
                cb();
            }
        };
    })(cb, key, value));
};
