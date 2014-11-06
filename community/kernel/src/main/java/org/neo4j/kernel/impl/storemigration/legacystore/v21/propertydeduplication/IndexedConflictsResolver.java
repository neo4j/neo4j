/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

import java.io.IOException;
import java.util.*;

class IndexedConflictsResolver implements NodeStore.NodeRecordProcessor, AutoCloseable
{
    private final PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters;
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final PropertyRemover propertyRemover;
    private final List<DeferredIndexedConflictResolution> deferredResolutions;
    private final IndexLookup indexLookup;

    public IndexedConflictsResolver( PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters,
                                     IndexLookup indexLookup,
                                     NodeStore nodeStore,
                                     PropertyStore propertyStore )
    {
        this.duplicateClusters = duplicateClusters;
        this.indexLookup = indexLookup;
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        propertyRemover = new PropertyRemover( nodeStore, propertyStore );
        deferredResolutions = new ArrayList<>();
    }

    @Override
    public void process( NodeRecord record ) throws IOException
    {
        List<DuplicateCluster> duplicateClusterList = duplicateClusters.get( record.getNextProp() );

        if ( duplicateClusterList != null )
        {
            deferredResolutions.add( new DeferredIndexedConflictResolution( record.clone(), duplicateClusterList) );
        }
    }

    @Override
    public void close() throws IOException
    {
        complete();
    }

    private void complete() throws IOException
    {
        for ( DeferredIndexedConflictResolution deferredResolution : deferredResolutions )
        {
            deferredResolution.resolve();
        }
    }

    private class DeferredIndexedConflictResolution
    {
        private final NodeRecord record;
        private final List<DuplicateCluster> duplicateClusterList;

        public DeferredIndexedConflictResolution( NodeRecord record, List<DuplicateCluster> duplicateClusters)
        {
            this.record = record;
            this.duplicateClusterList = duplicateClusters;
        }

        public void resolve() throws IOException
        {
            // For every conflicting property key id, if we can find a matching index, delete all the property blocks
            // whose value match nothing in the index.
            // Otherwise, leave the duplicateClusters to be resolved in a later step.
            long[] labelIds = NodeLabelsField.get(record, nodeStore);
            ListIterator<DuplicateCluster> it = duplicateClusterList.listIterator();
            while ( it.hasNext() )
            {
                // Figure out if the node is indexed by the property key for this conflict, and resolve the
                // conflict if that is the case.
                DuplicateCluster duplicateCluster = it.next();
                final IndexLookup.Index index = indexLookup.getAnyIndexOrNull( labelIds, duplicateCluster.propertyKeyId );

                if ( index != null )
                {
                    IndexConsultedPropertyBlockSweeper sweeper = new IndexConsultedPropertyBlockSweeper(index, record);
                    duplicateCluster.propertyRecordIds.visitKeys(sweeper);
                    assert sweeper.foundExact;
                    it.remove();
                }
            }
        }

        private class IndexConsultedPropertyBlockSweeper implements PrimitiveLongVisitor {
            private final IndexLookup.Index index;
            private final NodeRecord nodeRecord;
            boolean foundExact;

            public IndexConsultedPropertyBlockSweeper(IndexLookup.Index index, NodeRecord nodeRecord) {
                this.index = index;
                this.nodeRecord = nodeRecord;
                this.foundExact = false;
            }

            @Override
            public void visited(long recordId) {
                PropertyRecord record = propertyStore.getRecord( recordId );
                boolean changed = false;

                List<PropertyBlock> blocks = record.getPropertyBlocks();
                ListIterator<PropertyBlock> it = blocks.listIterator();
                while (it.hasNext()) {
                    PropertyBlock block = it.next();
                    Object lastPropertyValue = propertyStore.getValue( block );

                    try {
                        if ( index.contains( nodeRecord.getId(), lastPropertyValue ) && !foundExact )
                        {
                            foundExact = true;
                        }
                        else
                        {
                            it.remove();
                            changed = true;
                        }
                    } catch (IOException e) {
                        throw new InnerIterationIOException( e );
                    }
                }
                if (changed)
                {
                    propertyStore.updateRecord(record);
                    if (blocks.isEmpty()) {
                        propertyRemover.fixUpPropertyLinksAroundUnusedRecord( nodeRecord, record );
                    }
                }
            }
        }
    }
}
