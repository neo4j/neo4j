package org.neo4j.router.query;

import org.neo4j.values.virtual.MapValue;

/**
 * Wrapper of a query text with parameters
 */
public record Query(String text, MapValue parameters) {

    public static Query of(String text) {
        return Query.of(text, MapValue.EMPTY);
    }

    public static Query of(String text, MapValue parameters) {
        return new Query(text, parameters);
    }
}
