/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.graphdb.index;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * 
 * @author Tobias Lindaaker
 * 
 * @param <T>
 */
public abstract class UniqueFactory<T extends PropertyContainer>
{
    public static abstract class UniqueNodeFactory extends UniqueFactory<Node>
    {
        public UniqueNodeFactory( Index<Node> index )
        {
            super( index );
        }

        public UniqueNodeFactory( GraphDatabaseService graphdb, String index )
        {
            super( graphdb.index().forNodes( index ) );
        }

        @Override
        protected abstract void initialize( Node node );

        @Override
        final Node create()
        {
            return graphDatabase().createNode();
        }

        @Override
        void delete( Node node )
        {
            node.delete();
        }
    }

    public static abstract class UniqueRelationshipFactory extends UniqueFactory<Relationship>
    {
        public UniqueRelationshipFactory( Index<Relationship> index )
        {
            super( index );
        }

        public UniqueRelationshipFactory( GraphDatabaseService graphdb, String index )
        {
            super( graphdb.index().forRelationships( index ) );
        }

        @Override
        abstract Relationship create();

        @Override
        void initialize( Relationship relationship )
        {
            // creation is done in create()
        }

        @Override
        void delete( Relationship relationship )
        {
            relationship.delete();
        }
    }

    private final Index<T> index;

    private UniqueFactory( Index<T> index )
    {
        this.index = index;
    }

    protected final GraphDatabaseService graphDatabase()
    {
        return index.getGraphDatabase();
    }

    abstract T create();

    abstract void initialize( T result );

    abstract void delete( T result );

    public final T getOrCreate( String key, Object value )
    {
        T result, created = null;
        try
        {
            do
            {
                result = index.get( key, value ).getSingle();
                if ( result != null ) return result;
                if ( created == null )
                {
                    created = create();
                    if ( created == null )
                        throw new AssertionError( "create() returned null" );
                }
                result = created;
                if ( index.putIfAbsent( result, key, value ) )
                {
                    initialize( result );
                    created = null;
                    return result;
                }
                else
                {
                    result = null; // try again
                }
            }
            while ( result == null );
            return result;
        }
        finally
        {
            if ( created != null ) delete( created );
        }
    }
}
