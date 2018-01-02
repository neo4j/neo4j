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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.logging.NullLogProvider;

public class PropertyDeduplicator
{
    private final FileSystemAbstraction fileSystem;
    private final File workingDir;
    private final PageCache pageCache;
    private final SchemaIndexProvider schemaIndexProvider;
    private final PrimitiveIntObjectMap<Long> seenPropertyKeys;
    private final PrimitiveIntObjectMap<DuplicateCluster> localDuplicateClusters;

    public PropertyDeduplicator( FileSystemAbstraction fileSystem, File workingDir, PageCache pageCache,
                                 SchemaIndexProvider schemaIndexProvider )
    {
        this.fileSystem = fileSystem;
        this.workingDir = workingDir;
        this.pageCache = pageCache;
        this.schemaIndexProvider = schemaIndexProvider;

        seenPropertyKeys = Primitive.intObjectMap();
        localDuplicateClusters = Primitive.intObjectMap();
    }

    public void deduplicateProperties() throws IOException
    {
        final StoreFactory storeFactory =
                new StoreFactory( fileSystem, workingDir, pageCache, NullLogProvider.getInstance() );
        try ( NeoStores neoStores = storeFactory.openNeoStores( NeoStores.StoreType.PROPERTY, NeoStores.StoreType
                .NODE, NeoStores.StoreType.SCHEMA) )
        {
            PropertyStore propertyStore = neoStores.getPropertyStore();
            NodeStore nodeStore = neoStores.getNodeStore();
            SchemaStore schemaStore = neoStores.getSchemaStore();
            PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters = collectConflictingProperties( propertyStore );
            resolveConflicts( duplicateClusters, propertyStore, nodeStore, schemaStore );
        }
    }

    private PrimitiveLongObjectMap<List<DuplicateCluster>> collectConflictingProperties( final PropertyStore store )
    {
        final PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters = Primitive.longObjectMap();

        long highId = store.getHighId();
        for ( long headRecordId = 0; headRecordId < highId; ++headRecordId )
        {
            PropertyRecord record = store.forceGetRecord( headRecordId );
            // Skip property propertyRecordIds that are not in use.
            // Skip property propertyRecordIds that are not at the start of a chain.
            if ( !record.inUse() || record.getPrevProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                continue;
            }

            long propertyId = headRecordId;
            while ( propertyId != Record.NO_NEXT_PROPERTY.intValue() )
            {
                record = store.getRecord( propertyId );

                Iterable<PropertyBlock> propertyBlocks = record;
                scanForDuplicates( propertyId, propertyBlocks );

                propertyId = record.getNextProp();
            }

            final long localHeadRecordId = headRecordId;
            localDuplicateClusters.visitEntries( new PrimitiveIntObjectVisitor<DuplicateCluster, RuntimeException>()
            {
                @Override
                public boolean visited( int key, DuplicateCluster duplicateCluster )
                {
                    List<DuplicateCluster> clusters = duplicateClusters.get( localHeadRecordId );
                    if ( clusters == null )
                    {
                        clusters = new ArrayList<>();
                        duplicateClusters.put( localHeadRecordId, clusters );
                    }
                    clusters.add( duplicateCluster );
                    return false;
                }
            } );

            seenPropertyKeys.clear();
            localDuplicateClusters.clear();
        }

        return duplicateClusters;
    }

    private void scanForDuplicates( long propertyId, Iterable<PropertyBlock> propertyBlocks )
    {
        for ( PropertyBlock block : propertyBlocks )
        {
            int propertyKeyId = block.getKeyIndexId();

            // If we've seen this property key in this chain before, we schedule the newly found
            // duplicate for removal.
            if ( seenPropertyKeys.containsKey( propertyKeyId ) )
            {
                DuplicateCluster cluster = localDuplicateClusters.get( propertyKeyId );
                if ( cluster == null )
                {
                    cluster = new DuplicateCluster( propertyKeyId );
                    localDuplicateClusters.put( propertyKeyId, cluster );
                }
                cluster.add( seenPropertyKeys.get( propertyKeyId ) );
                cluster.add( propertyId );
            }
            else
            {
                seenPropertyKeys.put( propertyKeyId, propertyId );
            }
        }
    }

    private void resolveConflicts(
            final PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters,
            PropertyStore propertyStore,
            final NodeStore nodeStore,
            SchemaStore schemaStore ) throws IOException
    {
        if ( duplicateClusters.isEmpty() )
        {
            // Happiest of cases.
            return;
        }

        // For each conflict:
        //  - If we have an index for the given property key id, and the property is on an indexed node, then remove
        //    all the duplicates that do not match the indexed value.
        //  - Otherwise, we keep the "most recent" property block as is, and change the key of all the other duplicates
        //    to have a distinct property key id for a property key prefixed with "__DUPLICATE_<key>".

        // First find and resolve the duplicateClusters for all properties whose nodes are indexed.
        // The duplicateClusters are indexed by the propertyRecordId of the head-record in the property chain, so any node
        // whose nextProp() is amongst our duplicateClusters is potentially interesting.
        try ( IndexLookup indexLookup = new IndexLookup( schemaStore, schemaIndexProvider );
              IndexedConflictsResolver indexedConflictsResolver =
                      new IndexedConflictsResolver( duplicateClusters, indexLookup, nodeStore, propertyStore ) )
        {
            if ( indexLookup.hasAnyIndexes() )
            {
                nodeStore.scanAllRecords( indexedConflictsResolver );
            }
        }

        // Then resolve all duplicateClusters by changing the propertyKey for the first conflicting property block, to
        // one that is prefixed with "__DUPLICATE_<key>".
        PropertyKeyTokenStore keyTokenStore = propertyStore.getPropertyKeyTokenStore();
        NonIndexedConflictResolver resolver = new NonIndexedConflictResolver( keyTokenStore, propertyStore );
        duplicateClusters.visitEntries( resolver );
    }
}
