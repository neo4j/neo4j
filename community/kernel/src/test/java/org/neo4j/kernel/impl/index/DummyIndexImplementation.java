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
package org.neo4j.kernel.impl.index;

import java.io.File;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.spi.explicitindex.ExplicitIndexProviderTransaction;
import org.neo4j.kernel.spi.explicitindex.IndexCommandFactory;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;

public class DummyIndexImplementation extends LifecycleAdapter implements IndexImplementation
{
    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        return config;
    }

    @Override
    public boolean configMatches( Map<String, String> storedConfig, Map<String, String> suppliedConfig )
    {
        return true;
    }

    private boolean failing( Map<String, String> config )
    {
        return Boolean.parseBoolean( config.get( DummyIndexExtensionFactory.KEY_FAIL_ON_MUTATE ) );
    }

    private static class EmptyHits extends PrimitiveLongBaseIterator implements ExplicitIndexHits
    {
        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public float currentScore()
        {
            return 0;
        }

        @Override
        protected boolean fetchNext()
        {
            return false;
        }
    }

    private static final ExplicitIndexHits NO_HITS = new EmptyHits();

    private static class EmptyExplicitIndex implements ExplicitIndex
    {
        private final boolean failing;

        private EmptyExplicitIndex( boolean failing )
        {
            this.failing = failing;
        }

        @Override
        public void remove( long entity )
        {
            mutate();
        }

        @Override
        public void remove( long entity, String key )
        {
            mutate();
        }

        @Override
        public void remove( long entity, String key, Object value )
        {
            mutate();
        }

        @Override
        public ExplicitIndexHits query( Object queryOrQueryObject, long startNode, long endNode )
        {
            return NO_HITS;
        }

        @Override
        public ExplicitIndexHits query( String key, Object queryOrQueryObject, long startNode, long endNode )
        {
            return NO_HITS;
        }

        @Override
        public ExplicitIndexHits query( Object queryOrQueryObject )
        {
            return NO_HITS;
        }

        @Override
        public ExplicitIndexHits query( String key, Object queryOrQueryObject )
        {
            return NO_HITS;
        }

        @Override
        public ExplicitIndexHits get( String key, Object value, long startNode, long endNode )
        {
            return NO_HITS;
        }

        @Override
        public ExplicitIndexHits get( String key, Object value )
        {
            return NO_HITS;
        }

        @Override
        public void drop()
        {
            mutate();
        }

        @Override
        public void addRelationship( long entity, String key, Object value, long startNode, long endNode )
        {
            mutate();
        }

        @Override
        public void addNode( long entity, String key, Object value )
        {
            mutate();
        }

        @Override
        public void removeRelationship( long entity, String key, Object value, long startNode, long endNode )
        {
            mutate();
        }

        @Override
        public void removeRelationship( long entity, String key, long startNode, long endNode )
        {
            mutate();
        }

        @Override
        public void removeRelationship( long entity, long startNode, long endNode )
        {
            mutate();
        }

        private void mutate()
        {
            if ( failing )
            {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Override
    public File getIndexImplementationDirectory( File storeDir )
    {
        return storeDir;
    }

    @Override
    public ExplicitIndexProviderTransaction newTransaction( IndexCommandFactory commandFactory )
    {
        return new ExplicitIndexProviderTransaction()
        {
            @Override
            public ExplicitIndex relationshipIndex( String indexName, Map<String, String> configuration )
            {
                return new EmptyExplicitIndex( failing( configuration ) );
            }

            @Override
            public ExplicitIndex nodeIndex( String indexName, Map<String, String> configuration )
            {
                return new EmptyExplicitIndex( failing( configuration ) );
            }

            @Override
            public void close()
            {
            }
        };
    }

    private static final TransactionApplier NO_APPLIER = new TransactionApplier.Adapter();

    @Override
    public TransactionApplier newApplier( boolean recovery )
    {
        return NO_APPLIER;
    }

    @Override
    public void force()
    {
    }

    @Override
    public ResourceIterator<File> listStoreFiles()
    {
        return Iterators.emptyResourceIterator();
    }
}
