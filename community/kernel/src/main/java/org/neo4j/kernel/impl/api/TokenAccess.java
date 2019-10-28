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
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.token.api.NamedToken;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;

public abstract class TokenAccess<R>
{
    public static final TokenAccess<RelationshipType> RELATIONSHIP_TYPES = new TokenAccess<>()
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
        boolean inUse( KernelTransaction transaction, SchemaReadCore schemaReadCore, int tokenId )
        {
            return hasAny( schemaReadCore.indexesGetForRelationshipType( tokenId ) ) ||                // used by indexes
                    hasAny( schemaReadCore.constraintsGetForRelationshipType( tokenId ) ) ||           // used by constraint
                    transaction.dataRead().countsForRelationship( ANY_LABEL, tokenId, ANY_LABEL ) > 0; // used by data
        }
    };
    public static final TokenAccess<Label> LABELS = new TokenAccess<>()
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
        boolean inUse( KernelTransaction transaction, SchemaReadCore schemaReadCore, int tokenId )
        {
            return hasAny( schemaReadCore.indexesGetForLabel( tokenId ) ) ||     // used by index
                   hasAny( schemaReadCore.constraintsGetForLabel( tokenId ) ) || // used by constraint
                   transaction.dataRead().countsForNode( tokenId ) > 0;          // used by data
        }
    };

    public static final TokenAccess<String> PROPERTY_KEYS = new TokenAccess<>()
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
        boolean inUse( KernelTransaction transaction, SchemaReadCore schemaReadCore, int tokenId )
        {
            return true; // TODO: add support for telling if a property key is in use or not
        }
    };

    private static <T> Iterator<T> inUse( KernelTransaction transaction, TokenAccess<T> access )
    {
        SchemaReadCore schemaReadCore = transaction.schemaRead().snapshot();
        return new TokenIterator<>( transaction, access )
        {
            @Override
            protected T fetchNextOrNull()
            {
                while ( tokens.hasNext() )
                {
                    NamedToken token = tokens.next();
                    if ( this.access.inUse( transaction, schemaReadCore, token.id() ) )
                    {
                        return this.access.token( token );
                    }
                }
                return null;
            }
        };
    }

    private static <T> Iterator<T> all( KernelTransaction transaction, TokenAccess<T> access )
    {
        return new TokenIterator<>( transaction, access )
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
                    return null;
                }
            }
        };
    }

    public final Iterator<R> inUse( KernelTransaction transaction )
    {
        return inUse( transaction, this );
    }

    public final Iterator<R> all( KernelTransaction transaction )
    {
        return all( transaction, this );
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

    private abstract static class TokenIterator<T> extends PrefetchingIterator<T>
    {
        protected final TokenAccess<T> access;
        protected final Iterator<NamedToken> tokens;

        private TokenIterator( KernelTransaction transaction, TokenAccess<T> access )
        {
            this.access = access;
            this.tokens = access.tokens( transaction.tokenRead() );
        }
    }

    abstract Iterator<NamedToken> tokens( TokenRead tokenRead );

    abstract R token( NamedToken token );

    abstract boolean inUse( KernelTransaction transaction, SchemaReadCore schemaReadCore, int tokenId );
}
