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
package org.neo4j.kernel.impl.api;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;

import java.util.Iterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.token.api.NamedToken;

public abstract class TokenAccess<R> {
    public static final TokenAccess<RelationshipType> RELATIONSHIP_TYPES = new TokenAccess<>() {
        @Override
        Iterator<NamedToken> tokens(TokenRead read) {
            return read.relationshipTypesGetAllTokens();
        }

        @Override
        RelationshipType token(NamedToken token) {
            return RelationshipType.withName(token.name());
        }

        @Override
        boolean inUse(Read read, SchemaReadCore schemaReadCore, int tokenId) {
            return hasAny(schemaReadCore.indexesGetForRelationshipType(tokenId))
                    || // used by indexes
                    hasAny(schemaReadCore.constraintsGetForRelationshipType(tokenId))
                    || // used by constraint
                    read.countsForRelationship(ANY_LABEL, tokenId, ANY_LABEL) > 0; // used by data
        }
    };
    public static final TokenAccess<Label> LABELS = new TokenAccess<>() {
        @Override
        Iterator<NamedToken> tokens(TokenRead read) {
            return read.labelsGetAllTokens();
        }

        @Override
        Label token(NamedToken token) {
            return label(token.name());
        }

        @Override
        boolean inUse(Read read, SchemaReadCore schemaReadCore, int tokenId) {
            return hasAny(schemaReadCore.indexesGetForLabel(tokenId))
                    || // used by index
                    hasAny(schemaReadCore.constraintsGetForLabel(tokenId))
                    || // used by constraint
                    read.countsForNode(tokenId) > 0; // used by data
        }
    };

    public static final TokenAccess<String> PROPERTY_KEYS = new TokenAccess<>() {
        @Override
        Iterator<NamedToken> tokens(TokenRead read) {
            return read.propertyKeyGetAllTokens();
        }

        @Override
        String token(NamedToken token) {
            return token.name();
        }

        @Override
        boolean inUse(Read read, SchemaReadCore schemaReadCore, int tokenId) {
            return true;
        }
    };

    private static <T> Iterator<T> inUse(
            Read dataRead, SchemaRead schemaRead, TokenRead tokenRead, TokenAccess<T> access) {
        SchemaReadCore schemaReadCore = schemaRead.snapshot();
        return new TokenIterator<>(tokenRead, access) {
            @Override
            protected T fetchNextOrNull() {
                while (tokens.hasNext()) {
                    NamedToken token = tokens.next();
                    if (this.access.inUse(dataRead, schemaReadCore, token.id())) {
                        return this.access.token(token);
                    }
                }
                return null;
            }
        };
    }

    private static <T> Iterator<T> all(TokenRead tokenRead, TokenAccess<T> access) {
        return new TokenIterator<>(tokenRead, access) {
            @Override
            protected T fetchNextOrNull() {
                if (tokens.hasNext()) {
                    return access.token(tokens.next());
                } else {
                    return null;
                }
            }
        };
    }

    public final Iterator<R> inUse(Read dataRead, SchemaRead schemaRead, TokenRead tokenRead) {
        return inUse(dataRead, schemaRead, tokenRead, this);
    }

    public final Iterator<R> all(TokenRead tokenRead) {
        return all(tokenRead, this);
    }

    private static boolean hasAny(Iterator<?> iter) {
        if (iter.hasNext()) {
            return true;
        }
        if (iter instanceof Resource) {
            ((Resource) iter).close();
        }
        return false;
    }

    private abstract static class TokenIterator<T> extends PrefetchingIterator<T> {
        protected final TokenAccess<T> access;
        protected final Iterator<NamedToken> tokens;

        private TokenIterator(TokenRead tokenRead, TokenAccess<T> access) {
            this.access = access;
            this.tokens = access.tokens(tokenRead);
        }
    }

    abstract Iterator<NamedToken> tokens(TokenRead tokenRead);

    abstract R token(NamedToken token);

    abstract boolean inUse(Read read, SchemaReadCore schemaReadCore, int tokenId);
}
