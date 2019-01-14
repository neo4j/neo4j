/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.StatementConstants.ANY_LABEL;

public abstract class TokenAccess<R>
{
    public static final TokenAccess<RelationshipType> RELATIONSHIP_TYPES = new TokenAccess<RelationshipType>()
    {
        @Override
        Iterator<NamedToken> tokens( TokenRead read )
        {
            return read.relationshipTypesGetAllTokens();
        }

        @Override
        RelationshipType token( NamedToken token )
        {
            return RelationshipType.withName( token.name() );
        }

        @Override
        boolean inUse( KernelTransaction transaction, int tokenId )
        {
            return hasAny( transaction.schemaRead().constraintsGetForRelationshipType( tokenId ) ) ||   // used by constraint
                   transaction.dataRead().countsForRelationship( ANY_LABEL, tokenId, ANY_LABEL ) > 0; // used by data
        }
    };
    public static final TokenAccess<Label> LABELS = new TokenAccess<Label>()
    {
        @Override
        Iterator<NamedToken> tokens( TokenRead read )
        {
            return read.labelsGetAllTokens();
        }

        @Override
        Label token( NamedToken token )
        {
            return label( token.name() );
        }

        @Override
        boolean inUse( KernelTransaction transaction, int tokenId )
        {

            SchemaRead schemaRead = transaction.schemaRead();
            return hasAny( schemaRead.indexesGetForLabel( tokenId ) ) ||     // used by index
                   hasAny( schemaRead.constraintsGetForLabel( tokenId ) ) || // used by constraint
                   transaction.dataRead().countsForNode( tokenId ) > 0;                  // used by data
        }
    };

    public static final TokenAccess<String> PROPERTY_KEYS = new TokenAccess<String>()
    {
        @Override
        Iterator<NamedToken> tokens( TokenRead read )
        {
            return read.propertyKeyGetAllTokens();
        }

        @Override
        String token( NamedToken token )
        {
            return token.name();
        }

        @Override
        boolean inUse( KernelTransaction transaction, int tokenId )
        {
            return true; // TODO: add support for telling if a property key is in use or not
        }
    };

    public final ResourceIterator<R> inUse( KernelTransaction transaction )
    {
        return TokenIterator.inUse( transaction, this );
    }

    public final ResourceIterator<R> all( KernelTransaction transaction )
    {
        return TokenIterator.all( transaction, this );
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
        private Statement statement;
        protected final TokenAccess<T> access;
        protected final Iterator<NamedToken> tokens;

        private TokenIterator( KernelTransaction transaction, TokenAccess<T> access )
        {
            this.access = access;
            this.statement = transaction.acquireStatement();
            try
            {
                this.tokens = access.tokens( transaction.tokenRead() );
            }
            catch ( Exception e )
            {
                close();
                throw e;
            }
        }

        @Override
        public void close()
        {
            if ( statement != null )
            {
                statement.close();
                statement = null;
            }
        }

        static <T> ResourceIterator<T> inUse( KernelTransaction transaction, TokenAccess<T> access )
        {
            return new TokenIterator<T>( transaction, access )
            {
                @Override
                protected T fetchNextOrNull()
                {
                    while ( tokens.hasNext() )
                    {
                        NamedToken token = tokens.next();
                        if ( this.access.inUse( transaction, token.id() ) )
                        {
                            return this.access.token( token );
                        }
                    }
                    close();
                    return null;
                }
            };
        }

        static <T> ResourceIterator<T> all( KernelTransaction transaction, TokenAccess<T> access )
        {
            return new TokenIterator<T>( transaction, access )
            {
                @Override
                protected T fetchNextOrNull()
                {
                    if ( tokens.hasNext() )
                    {
                        return access.token( tokens.next() );
                    }
                    else
                    {
                        close();
                        return null;
                    }
                }
            };
        }
    }

    abstract Iterator<NamedToken> tokens( TokenRead tokenRead );

    abstract R token( NamedToken token );

    abstract boolean inUse( KernelTransaction transaction, int tokenId );
}
