/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime;

import static org.apache.commons.lang3.ArrayUtils.contains;

import org.neo4j.internal.kernel.api.TokenRead;

/**
 * Resolves tokens for a set of property names.
 */
public interface PropertyTokensResolver {
    /**
     * Resolve any unresolved property tokens.
     *
     * NOTE! Needs to be called for every row, because tokens might be created during a query.
     */
    void populate(DbAccess db);

    /** Returns property names */
    String[] names();

    /** Returns resolved tokens in the same order as names(). Call populate() before accessing this method. */
    int[] tokens();

    /**
     * Returns a new token resolver.
     */
    static PropertyTokensResolver property(String[] names, int[] tokens) {
        assert names.length == tokens.length;

        if (contains(tokens, TokenRead.NO_TOKEN)) {
            return new IncompletePropertyTokenResolverProperty(names, tokens);
        } else {
            return new CompletePropertyTokenResolverProperty(names, tokens);
        }
    }
}

class CompletePropertyTokenResolverProperty implements PropertyTokensResolver {
    private final String[] names;
    private final int[] tokens;

    CompletePropertyTokenResolverProperty(String[] names, int[] tokens) {
        assert !contains(tokens, TokenRead.NO_TOKEN);
        this.names = names;
        this.tokens = tokens;
    }

    @Override
    public void populate(DbAccess db) {}

    @Override
    public String[] names() {
        return names;
    }

    @Override
    public int[] tokens() {
        return tokens;
    }
}

class IncompletePropertyTokenResolverProperty implements PropertyTokensResolver {
    private final String[] names;
    private final int[] tokens;
    private boolean isComplete;

    IncompletePropertyTokenResolverProperty(String[] names, int[] tokens) {
        this.names = names;
        this.tokens = tokens;
    }

    @Override
    public void populate(DbAccess db) {
        if (isComplete) {
            return;
        }

        boolean newIsComplete = true;
        final var size = names.length;
        for (int i = 0; i < size; ++i) {
            final var token = tokens[i];

            if (token == TokenRead.NO_TOKEN) {
                final int newToken = db.propertyKey(names[i]);
                tokens[i] = newToken;
                newIsComplete = newIsComplete && newToken != TokenRead.NO_TOKEN;
            }
        }
        isComplete = newIsComplete;
    }

    @Override
    public String[] names() {
        return names;
    }

    @Override
    public int[] tokens() {
        return tokens;
    }
}
