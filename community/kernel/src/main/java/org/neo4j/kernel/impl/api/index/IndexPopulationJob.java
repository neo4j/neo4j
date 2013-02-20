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

import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RecordStore.Processor;

/**
 * Represents one job of initially populating an index over existing data in the database.
 * Scans the store directly.
 * 
 * @author Mattias Persson
 */
class IndexPopulationJob implements Runnable
{
    private final long labelId;
    private final long propertyKeyId;
    private final IndexPopulator indexManipulator;
    private final NeoStore neoStore;

    public IndexPopulationJob( long labelId, long propertyKeyId, IndexPopulator indexManipulator, NeoStore neoStore )
    {
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
        this.indexManipulator = indexManipulator;
        this.neoStore = neoStore;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public void run()
    {
        // Create a processor that for each accepted node (containing the desired label) looks through its properties,
        // getting the desired one (if any) and feeds to the index manipulator.
        final PropertyStore propertyStore = neoStore.getPropertyStore();
        Processor processor = new Processor()
        {
            private int count;
            
            @Override
            public void processNode( RecordStore<NodeRecord> store, NodeRecord node )
            {
                // TODO check cache if that property is in cache and use it, instead of loading it from the store.
                
                long firstPropertyId = node.getCommittedNextProp();
                if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
                    return; // It didn't have any properties at all.
                
                // TODO optimize so that we don't have to load all property records, but instead stop
                // when we first find the property we're looking for.
                for ( PropertyRecord propertyRecord : propertyStore.getPropertyRecordChain( firstPropertyId ) )
                {
                    for ( PropertyBlock property : propertyRecord.getPropertyBlocks() )
                    {
                        if ( property.getKeyIndexId() == propertyKeyId )
                        {
                            // Make sure the value is loaded, even if it's of a "heavy" kind.
                            propertyStore.makeHeavy( property );
                            Object propertyValue = property.getType().getValue( property, propertyStore );
                            indexManipulator.add( count++, node.getId(), propertyValue );
                        }
                    }
                }
            }
        };
        
        // Run the processor for the nodes containing the given label.
        // TODO When we've got a decent way of getting nodes with a label, use that instead.
        final NodeStore nodeStore = neoStore.getNodeStore();
        processor.applyFiltered( nodeStore, new Predicate<NodeRecord>()
        {
            @Override
            public boolean accept( NodeRecord node )
            {
                for ( long nodelabelId : nodeStore.getLabelsForNode( node ) )
                    if ( nodelabelId == labelId )
                        return true;
                return false;
            }
        } );
        
        indexManipulator.done();
    }
    
    /**
     * A transaction happened that produced the given diff. Let this job incorporate its data
     * into, feeding it to the {@link IndexPopulator}.
     */
    public void transactionData( DiffSets<NodePropertyUpdate> diff )
    {
        throw new UnsupportedOperationException();
    }
}
