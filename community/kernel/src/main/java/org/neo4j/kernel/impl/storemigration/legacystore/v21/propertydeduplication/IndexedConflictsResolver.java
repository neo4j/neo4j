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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

class IndexedConflictsResolver implements Visitor<NodeRecord, IOException>, AutoCloseable
{
    private final PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters;
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final DuplicatePropertyRemover propertyRemover;
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
        propertyRemover = new DuplicatePropertyRemover( nodeStore, propertyStore );
        deferredResolutions = new ArrayList<>();
    }

    @Override
    public boolean visit( NodeRecord record ) throws IOException
    {
        List<DuplicateCluster> duplicateClusterList = duplicateClusters.get( record.getNextProp() );

        if ( duplicateClusterList != null )
        {
            deferredResolutions.add( new DeferredIndexedConflictResolution( record.clone(), duplicateClusterList,
                    nodeStore, indexLookup, propertyStore, propertyRemover ) );
        }
        return false;
    }

    @Override
    public void close() throws IOException
    {
        for ( DeferredIndexedConflictResolution deferredResolution : deferredResolutions )
        {
            deferredResolution.resolve();
        }
    }

}
