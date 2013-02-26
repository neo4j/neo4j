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
package org.neo4j.kernel.impl.api.index;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

public class OnlineIndexService implements IndexPopulationService
{
    private final SchemaIndexProvider indexProvider;
    private final Map<Long, UpdateProcessor> indexes = new CopyOnWriteHashMap<Long, UpdateProcessor>();

    public OnlineIndexService( SchemaIndexProvider indexProvider )
    {
        this.indexProvider = indexProvider;
    }
    
    void indexReady( IndexRule index )
    {
        indexes.get( index.getId() ).ready();
    }
    
    @Override
    public void indexCreated( IndexRule index )
    {
        indexes.put( index.getId(), new UpdateProcessor( index, indexProvider.getPopulator( index.getId() ) ) );
    }

    @Override
    public void indexUpdates( Iterable<NodePropertyUpdate> updates )
    {
        for ( UpdateProcessor processor : indexes.values() )
            processor.process( updates );
    }
    
    private class UpdateProcessor
    {
        private final IndexWriter indexWriter;
        private final CountDownLatch latch = new CountDownLatch( 1 );
        private final IndexRule indexRule;
        
        public UpdateProcessor( IndexRule indexRule, IndexWriter indexWriter )
        {
            this.indexRule = indexRule;
            this.indexWriter = indexWriter;
        }
        
        void ready()
        {
            latch.countDown();
        }
        
        void process( Iterable<NodePropertyUpdate> updates )
        {
            try
            {
                latch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
            
            // TODO filter before getting here
            for ( NodePropertyUpdate update : updates )
                if ( update.getPropertyKeyId() == indexRule.getPropertyKey() && update.hasLabel( indexRule.getLabel() ) )
                    update.apply( indexWriter );
        }
    }
}
