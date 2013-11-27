/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.index;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class DummyIndexExtensionFactory extends
        KernelExtensionFactory<DummyIndexExtensionFactory.Dependencies> implements IndexImplementation, Lifecycle
{
    static final String IDENTIFIER = "test-dummy-neo-index";
    private InternalAbstractGraphDatabase db;
    private IndexProviders indexProviders;

    public DummyIndexExtensionFactory()
    {
        super( IDENTIFIER );
    }

    public interface Dependencies
    {
        InternalAbstractGraphDatabase getDatabase();

        IndexProviders getIndexProviders();
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        db = dependencies.getDatabase();
        indexProviders = dependencies.getIndexProviders();
        return this;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        indexProviders.registerIndexProvider( IDENTIFIER, this );
    }

    @Override
    public void stop() throws Throwable
    {
        indexProviders.unregisterIndexProvider( IDENTIFIER );
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public String getDataSourceName()
    {
        return NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;
    }

    @Override
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        return new DummyNodeIndex( indexName, db );
    }

    @Override
    public RelationshipIndex relationshipIndex( String indexName, Map<String, String> config )
    {
        return new DummyRelationshipIndex( indexName, db );
    }

    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        return config;
    }

    @Override
    public boolean configMatches( Map<String, String> storedConfig, Map<String, String> config )
    {
        return true;
    }

    private abstract class DummyIndex<T extends PropertyContainer> implements Index<T>
    {
        private final String name;
        private final InternalAbstractGraphDatabase db;

        public DummyIndex( String name, InternalAbstractGraphDatabase db )
        {
            this.name = name;
            this.db = db;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public IndexHits<T> get( String key, Object value )
        {
            return new IteratorIndexHits<>( Collections.<T>emptyList() );
        }

        @Override
        public IndexHits<T> query( String key, Object queryOrQueryObject )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexHits<T> query( Object queryOrQueryObject )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWriteable()
        {
            return false;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return null;
        }

        @Override
        public void add( T entity, String key, Object value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove( T entity, String key, Object value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove( T entity, String key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove( T entity )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public T putIfAbsent( T entity, String key, Object value )
        {
            throw new UnsupportedOperationException();
        }
    }

    private class DummyNodeIndex extends DummyIndex<Node>
    {
        public DummyNodeIndex( String name, InternalAbstractGraphDatabase db )
        {
            super( name, db );
        }

        @Override
        public Class<Node> getEntityType()
        {
            return Node.class;
        }
    }

    private class DummyRelationshipIndex extends DummyIndex<Relationship> implements RelationshipIndex
    {
        public DummyRelationshipIndex( String name, InternalAbstractGraphDatabase db )
        {
            super( name, db );
        }

        @Override
        public Class<Relationship> getEntityType()
        {
            return Relationship.class;
        }

        @Override
        public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull,
                                            Node endNodeOrNull )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
                                              Node startNodeOrNull, Node endNodeOrNull )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull,
                                              Node startNodeOrNull, Node endNodeOrNull )
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class IteratorIndexHits<T> implements IndexHits<T>
    {
        private final List<T> list;
        private final Iterator<T> iterator;

        IteratorIndexHits( List<T> list )
        {
            this.list = list;
            this.iterator = list.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public T next()
        {
            return iterator.next();
        }

        @Override
        public void remove()
        {
            iterator.remove();
        }

        @Override
        public IndexHits<T> iterator()
        {
            return this;
        }

        @Override
        public int size()
        {
            return list.size();
        }

        @Override
        public void close()
        {
        }

        @Override
        public T getSingle()
        {
            return IteratorUtil.singleOrNull( (Iterator<T>) this );
        }

        @Override
        public float currentScore()
        {
            return Float.NaN;
        }
    }
}
