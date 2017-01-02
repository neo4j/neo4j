/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.storageengine.api.Token;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;

public abstract class TokenAccess<R>
{
    public static final TokenAccess<RelationshipType> RELATIONSHIP_TYPES = new TokenAccess<RelationshipType>()
    {
        @Override
        Iterator<Token> tokens( ReadOperations read )
        {
            return read.relationshipTypesGetAllTokens();
        }

        @Override
        RelationshipType token( Token token )
        {
            if ( token instanceof RelationshipType )
            {
                return (RelationshipType) token;
            }
            return RelationshipType.withName( token.name() );
        }

        @Override
        boolean inUse( ReadOperations read, int tokenId )
        {
            return hasAny( read.constraintsGetForRelationshipType( tokenId ) ) ||   // used by constraint
                   read.countsForRelationship( ANY_LABEL, tokenId, ANY_LABEL ) > 0; // used by data
        }
    };
    public static final TokenAccess<Label> LABELS = new TokenAccess<Label>()
    {
        @Override
        Iterator<Token> tokens( ReadOperations read )
        {
            return read.labelsGetAllTokens();
        }

        @Override
        Label token( Token token )
        {
            return label( token.name() );
        }

        @Override
        boolean inUse( ReadOperations read, int tokenId )
        {
            return hasAny( read.indexesGetForLabel( tokenId ) ) ||     // used by index
                   hasAny( read.constraintsGetForLabel( tokenId ) ) || // used by constraint
                   read.countsForNode( tokenId ) > 0;                  // used by data
        }
    };

    public static final TokenAccess<String> PROPERTY_KEYS = new TokenAccess<String>()
    {
        @Override
        Iterator<Token> tokens( ReadOperations read )
        {
            return read.propertyKeyGetAllTokens();
        }

        @Override
        String token( Token token )
        {
            return token.name();
        }

        @Override
        boolean inUse( ReadOperations read, int tokenId )
        {
            return true; // TODO: add support for telling if a property key is in use or not
        }
    };

    public final ResourceIterator<R> inUse( Statement statement )
    {
        return TokenIterator.inUse( statement, this );
    }

    public final ResourceIterator<R> all( Statement statement )
    {
        return TokenIterator.all( statement, this );
    }

    private static boolean hasAny( Iterator<?> iter )
    {
        if ( iter.hasNext() )
        {
            return true;
        }
        if ( iter instanceof Resource )
        {
            ((Resource) iter).close();
        }
        return false;
    }

    private abstract static class TokenIterator<T> extends PrefetchingResourceIterator<T>
    {
        final Statement statement;
        final TokenAccess<T> access;
        final Iterator<Token> tokens;

        TokenIterator( Statement statement, TokenAccess<T> access )
        {
            this.statement = statement;
            this.access = access;
            this.tokens = access.tokens( statement.readOperations() );
        }

        @Override
        public void close()
        {
            statement.close();
        }

        static <T> ResourceIterator<T> inUse( Statement statement, TokenAccess<T> access )
        {
            return new TokenIterator<T>( statement, access )
            {
                @Override
                protected T fetchNextOrNull()
                {
                    while ( tokens.hasNext() )
                    {
                        Token token = tokens.next();
                        if ( access.inUse( statement.readOperations(), token.id() ) )
                        {
                            return access.token( token );
                        }
                    }
                    return null;
                }
            };
        }

        static <T> ResourceIterator<T> all( Statement statement, TokenAccess<T> access )
        {
            return new TokenIterator<T>( statement, access )
            {
                @Override
                protected T fetchNextOrNull()
                {
                    return tokens.hasNext() ? access.token( tokens.next() ) : null;
                }
            };
        }
    }

    abstract Iterator<Token> tokens( ReadOperations read );

    abstract R token( Token token );

    abstract boolean inUse( ReadOperations read, int tokenId );
}
