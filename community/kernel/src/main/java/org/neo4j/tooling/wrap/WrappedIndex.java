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
package org.neo4j.tooling.wrap;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.graphdb.index.RelationshipIndex;

import static org.neo4j.tooling.wrap.WrappedEntity.unwrap;

public abstract class WrappedIndex<T extends PropertyContainer, I extends ReadableIndex<T>> implements Index<T>
{
    final WrappedGraphDatabase graphdb;

    private WrappedIndex( WrappedGraphDatabase graphdb )
    {
        this.graphdb = graphdb;
    }

    @SuppressWarnings( "unchecked" )
    static <T extends PropertyContainer> Index<T> unwrapIndex( Index<T> index )
    {
        if ( index instanceof WrappedIndex<?, ?> )
        {
            return ( (WrappedIndex<T, Index<T>>) index ).actual();
        }
        else
        {
            return index;
        }
    }

    protected abstract I actual();

    abstract T wrap( T entity );
    
    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return graphdb;
    }

    @Override
    public String toString()
    {
        return actual().toString();
    }

    @Override
    public String getName()
    {
        return actual().getName();
    }

    @Override
    public Class<T> getEntityType()
    {
        return actual().getEntityType();
    }

    @Override
    public void add( T entity, String key, Object value )
    {
        I actual = actual();
        if ( actual instanceof Index<?> )
        {
            ( (Index<T>) actual ).add( unwrap( entity ), key, value );
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void remove( T entity, String key, Object value )
    {
        I actual = actual();
        if ( actual instanceof Index<?> )
        {
            ( (Index<T>) actual ).remove( unwrap( entity ), key, value );
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void remove( T entity, String key )
    {
        I actual = actual();
        if ( actual instanceof Index<?> )
        {
            ( (Index<T>) actual ).remove( unwrap( entity ), key );
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void remove( T entity )
    {
        I actual = actual();
        if ( actual instanceof Index<?> )
        {
            ( (Index<T>) actual ).remove( unwrap( entity ) );
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void delete()
    {
        I actual = actual();
        if ( actual instanceof Index<?> )
        {
            ( (Index<T>) actual ).delete();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public IndexHits<T> get( String key, Object value )
    {
        return new Hits( actual().get( key, value ) );
    }

    @Override
    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        return new Hits( actual().query( key, queryOrQueryObject ) );
    }

    @Override
    public IndexHits<T> query( Object queryOrQueryObject )
    {
        return new Hits( actual().query( queryOrQueryObject ) );
    }

    @Override
    public boolean isWriteable()
    {
        return actual().isWriteable();
    }

    @Override
    public boolean putIfAbsent( T entity, String key, Object value )
    {
        I actual = actual();
        if ( actual instanceof Index<?> )
        {
            return ((Index<T>) actual).putIfAbsent( entity, key, value );
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public static abstract class WrappedNodeIndex extends
            WrappedIndex<Node, ReadableIndex<Node>>
    {
        protected WrappedNodeIndex( WrappedGraphDatabase graphdb )
        {
            super( graphdb );
        }

        protected WrappedGraphDatabase graphdb()
        {
            return graphdb;
        }

        @Override
        Node wrap( Node entity )
        {
            return graphdb.node( entity, false );
        }
    }

    public static abstract class WrappedRelationshipIndex extends
            WrappedIndex<Relationship, ReadableRelationshipIndex> implements RelationshipIndex
    {
        protected WrappedRelationshipIndex( WrappedGraphDatabase graphdb )
        {
            super( graphdb );
        }

        protected WrappedGraphDatabase graphdb()
        {
            return graphdb;
        }

        @Override
        public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull, Node endNodeOrNull )
        {
            return new Hits( actual().get( key, valueOrNull, unwrap( startNodeOrNull ), unwrap( endNodeOrNull ) ) );
        }

        @Override
        public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull, Node startNodeOrNull,
                Node endNodeOrNull )
        {
            return new Hits( actual().query( key, queryOrQueryObjectOrNull, unwrap( startNodeOrNull ),
                    unwrap( endNodeOrNull ) ) );
        }

        @Override
        public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )
        {
            return new Hits( actual().query( queryOrQueryObjectOrNull, unwrap( startNodeOrNull ), unwrap( endNodeOrNull ) ) );
        }

        @Override
        Relationship wrap( Relationship entity )
        {
            return graphdb.relationship( entity, false );
        }
    }

    private static abstract class WrappedIndexHits<T> implements IndexHits<T>
    {
        private final IndexHits<T> hits;

        WrappedIndexHits( IndexHits<T> hits )
        {
            this.hits = hits;
        }

        abstract T wrap( T item );

        @Override
        public int hashCode()
        {
            return hits.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj ) return true;
            if ( obj instanceof WrappedIndex.WrappedIndexHits )
            {
                @SuppressWarnings( "rawtypes" ) WrappedIndex.WrappedIndexHits other = (WrappedIndex.WrappedIndexHits) obj;
                return hits.equals( other.hits );
            }
            return false;
        }

        @Override
        public String toString()
        {
            return hits.toString();
        }

        @Override
        public boolean hasNext()
        {
            return hits.hasNext();
        }

        @Override
        public T next()
        {
            return wrap( hits.next() );
        }

        @Override
        public void remove()
        {
            hits.remove();
        }

        @Override
        public Iterator<T> iterator()
        {
            return this;
        }

        @Override
        public int size()
        {
            return hits.size();
        }

        @Override
        public void close()
        {
            hits.close();
        }

        @Override
        public T getSingle()
        {
            T single = hits.getSingle();
            if (single == null) return null;
            return wrap( single );
        }

        @Override
        public float currentScore()
        {
            return hits.currentScore();
        }
    }

    class Hits extends WrappedIndexHits<T>
    {
        Hits( IndexHits<T> hits )
        {
            super( hits );
        }

        @Override
        T wrap( T item )
        {
            return WrappedIndex.this.wrap( item );
        }
    }
}
