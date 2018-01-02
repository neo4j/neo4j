/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.File;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexCommandFactory;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.LegacyIndexProviderTransaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

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

    private static class EmptyHits extends PrimitiveLongBaseIterator implements LegacyIndexHits
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

    private static final LegacyIndexHits NO_HITS = new EmptyHits();

    private static class EmptyLegacyIndex implements LegacyIndex
    {
        private final boolean failing;

        private EmptyLegacyIndex( boolean failing )
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
        public LegacyIndexHits query( Object queryOrQueryObject, long startNode, long endNode )
        {
            return NO_HITS;
        }

        @Override
        public LegacyIndexHits query( String key, Object queryOrQueryObject, long startNode, long endNode )
        {
            return NO_HITS;
        }

        @Override
        public LegacyIndexHits query( Object queryOrQueryObject )
        {
            return NO_HITS;
        }

        @Override
        public LegacyIndexHits query( String key, Object queryOrQueryObject )
        {
            return NO_HITS;
        }

        @Override
        public LegacyIndexHits get( String key, Object value, long startNode, long endNode )
        {
            return NO_HITS;
        }

        @Override
        public LegacyIndexHits get( String key, Object value )
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
    public LegacyIndexProviderTransaction newTransaction( IndexCommandFactory commandFactory )
    {
        return new LegacyIndexProviderTransaction()
        {
            @Override
            public LegacyIndex relationshipIndex( String indexName, Map<String, String> configuration )
            {
                return new EmptyLegacyIndex( failing( configuration ) );
            }

            @Override
            public LegacyIndex nodeIndex( String indexName, Map<String, String> configuration )
            {
                return new EmptyLegacyIndex( failing( configuration ) );
            }

            @Override
            public void close()
            {
            }
        };
    }

    private static final CommandHandler NO_APPLIER = new CommandHandler.Adapter();

    @Override
    public CommandHandler newApplier( boolean recovery )
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
        return IteratorUtil.emptyIterator();
    }
}
