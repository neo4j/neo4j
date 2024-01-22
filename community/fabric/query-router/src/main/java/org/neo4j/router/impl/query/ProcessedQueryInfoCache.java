/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.router.impl.query;

import java.util.Set;
import org.neo4j.cypher.internal.PreParsedQuery;
import org.neo4j.cypher.internal.cache.CacheSize;
import org.neo4j.cypher.internal.cache.CacheTracer;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheKeyWithParameterType;
import org.neo4j.cypher.internal.cache.LFUCache;
import org.neo4j.cypher.internal.frontend.phases.BaseState;
import org.neo4j.cypher.internal.util.InternalNotification;
import org.neo4j.function.Observable;
import org.neo4j.router.query.TargetService;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.virtual.MapValue;

public class ProcessedQueryInfoCache {

    public static final String MONITOR_TAG = "cypher.cache.router";

    private final LFUCache<CacheKeyWithParameterType, Value> cache;

    @VisibleForTesting
    public ProcessedQueryInfoCache(
            CaffeineCacheFactory cacheFactory,
            Observable<Integer> cacheSize,
            CacheTracer<CacheKeyWithParameterType> tracer) {
        this.cache = new LFUCache<>(cacheFactory, new CacheSize.Dynamic(cacheSize), tracer);
    }

    public ProcessedQueryInfoCache(
            CaffeineCacheFactory cacheFactory, int cacheSize, CacheTracer<CacheKeyWithParameterType> tracer) {
        this.cache = new LFUCache<>(cacheFactory, new CacheSize.Static(cacheSize), tracer);
    }

    public Value get(PreParsedQuery query, MapValue parameters) {
        var maybeValue = cache.get(CypherQueryCaches.astKeyRawQuery(query, parameters, true));
        if (maybeValue.isEmpty()) {
            return null;
        }
        return maybeValue.get();
    }

    public void put(PreParsedQuery query, MapValue parameters, Value value) {
        cache.put(CypherQueryCaches.astKeyRawQuery(query, parameters, true), value);
    }

    public record Value(
            TargetService.CatalogInfo catalogInfo,
            String rewrittenQueryText,
            MapValue maybeExtractedParams,
            PreParsedQuery preParsedQuery,
            BaseState parsedQuery,
            StatementType statementType,
            Set<InternalNotification> parsingNotifications) {}

    public long clearQueryCachesForDatabase(String databaseName) {
        // Currently, we just clear everything.
        // The reason is that this cache contains entries for databases
        // that might not exist locally, so there is no way how to target
        // such entries with the current API.
        long size = cache.estimatedSize();
        cache.clear();
        return size;
    }
}
