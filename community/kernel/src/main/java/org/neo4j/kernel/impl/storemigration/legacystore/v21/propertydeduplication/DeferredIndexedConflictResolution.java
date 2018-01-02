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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

class DeferredIndexedConflictResolution
{
    private final NodeRecord record;
    private final List<DuplicateCluster> duplicateClusterList;
    private NodeStore nodeStore;
    private IndexLookup indexLookup;
    private PropertyStore propertyStore;
    private DuplicatePropertyRemover propertyRemover;

    public DeferredIndexedConflictResolution( NodeRecord record, List<DuplicateCluster> duplicateClusters,
                                              NodeStore nodeStore, IndexLookup indexLookup,
                                              PropertyStore propertyStore, DuplicatePropertyRemover propertyRemover )
    {
        this.record = record;
        this.duplicateClusterList = duplicateClusters;
        this.nodeStore = nodeStore;
        this.indexLookup = indexLookup;
        this.propertyStore = propertyStore;
        this.propertyRemover = propertyRemover;
    }

    public void resolve() throws IOException
    {
        assert duplicateClusterList.size() > 0;

        // For every conflicting property key id, if we can find a matching index, delete all the property blocks
        // whose value match nothing in the index.
        // Otherwise, leave the duplicateClusters to be resolved in a later step.
        long[] labelIds = NodeLabelsField.get( record, nodeStore );
        Iterator<DuplicateCluster> it = duplicateClusterList.iterator();
        while ( it.hasNext() )
        {
            // Figure out if the node is indexed by the property key for this conflict, and resolve the
            // conflict if that is the case.
            DuplicateCluster duplicateCluster = it.next();
            assert duplicateCluster.size() > 0;

            final IndexLookup.Index index = indexLookup.getAnyIndexOrNull( labelIds, duplicateCluster.propertyKeyId );

            if ( index != null )
            {
                IndexConsultedPropertyBlockSweeper sweeper = new IndexConsultedPropertyBlockSweeper(
                        duplicateCluster.propertyKeyId, index, record, propertyStore, propertyRemover );
                duplicateCluster.propertyRecordIds.visitKeys( sweeper );
                assert sweeper.foundExact;
                it.remove();
            }
        }
    }
}
