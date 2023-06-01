package org.neo4j.router.impl.query.parsing;

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.cypher.internal.cache.CacheTracer;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.cypher.internal.cache.LFUCache;
import org.neo4j.router.query.QueryTargetParser;

public class QueryTargetCache implements QueryTargetParser.Cache {

    private final LFUCache<String, Optional<CatalogName>> cache;

    public QueryTargetCache(CaffeineCacheFactory cacheFactory, int cacheSize, CacheTracer<String> tracer) {
        this.cache = new LFUCache<>(cacheFactory, cacheSize, tracer);
    }

    @Override
    public Optional<CatalogName> computeIfAbsent(String query, Supplier<Optional<CatalogName>> supplier) {
        return cache.computeIfAbsent(query, supplier::get);
    }
}
