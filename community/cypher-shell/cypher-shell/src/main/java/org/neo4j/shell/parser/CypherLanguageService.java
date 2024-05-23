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
package org.neo4j.shell.parser;

import java.util.List;
import java.util.stream.Stream;

/**
 * Analyses Cypher.
 */
public interface CypherLanguageService {
    /**
     * Returns tokenized query.
     */
    List<Token> tokenize(String query);

    /**
     * Returns all available Cypher keywords.
     */
    Stream<String> keywords();

    /**
     * Returns suggestions for the next keyword based on the specified incomplete query.
     */
    List<String> suggestNextKeyword(String incompleteQuery);

    /** Returns no-op service, but code is kept for a while in case we want to re-use it with the antlr parser. */
    static CypherLanguageService get() {
        return new CypherLanguageService() {
            @Override
            public List<Token> tokenize(String query) {
                return List.of();
            }

            @Override
            public Stream<String> keywords() {
                return Stream.empty();
            }

            @Override
            public List<String> suggestNextKeyword(String incompleteQuery) {
                return List.of();
            }
        };
    }

    interface Token {
        /** Returns the Java CC parser token kind */
        int kind();

        /** Returns token representation, for escaped unicode characters this is not the same as it appears in the query string  */
        String image();

        /** Returns token first character offset in query */
        int beginOffset();

        /** Returns token last character offset in query */
        int endOffset();

        /** Returns true if this is a query parameter identifier */
        boolean isParameterIdentifier();

        boolean isIdentifier();
    }
}
