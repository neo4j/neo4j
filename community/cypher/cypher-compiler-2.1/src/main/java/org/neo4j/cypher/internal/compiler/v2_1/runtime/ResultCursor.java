package org.neo4j.cypher.internal.compiler.v2_1.runtime;

public interface ResultCursor {
    Object get(String column);
    boolean next();
}
