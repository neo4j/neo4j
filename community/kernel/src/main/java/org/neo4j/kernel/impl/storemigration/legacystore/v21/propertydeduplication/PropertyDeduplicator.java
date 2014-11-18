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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.store.*;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.monitoring.Monitors;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class PropertyDeduplicator
{
    private final FileSystemAbstraction fileSystem;
    private final File workingDir;
    private final PageCache pageCache;
    private final SchemaIndexProvider schemaIndexProvider;
    private final File propertyStorePath;
    private final File nodeStorePath;
    private final File schemaStorePath;
    private PrimitiveIntObjectMap<Long> seenPropertyKeys;
    private PrimitiveIntObjectMap<DuplicateCluster> localDuplicateClusters;

    public PropertyDeduplicator( FileSystemAbstraction fileSystem, File workingDir, PageCache pageCache,
                                 SchemaIndexProvider schemaIndexProvider )
    {
        this.fileSystem = fileSystem;
        this.workingDir = workingDir;
        this.pageCache = pageCache;
        this.schemaIndexProvider = schemaIndexProvider;

        propertyStorePath = new File( workingDir, NeoStore.DEFAULT_NAME + StoreFactory.PROPERTY_STORE_NAME );
        nodeStorePath = new File( workingDir, NeoStore.DEFAULT_NAME + StoreFactory.NODE_STORE_NAME );
        schemaStorePath = new File( workingDir, NeoStore.DEFAULT_NAME + StoreFactory.SCHEMA_STORE_NAME );

        seenPropertyKeys = Primitive.intObjectMap();
        localDuplicateClusters = Primitive.intObjectMap();
    }

    public void deduplicateProperties() throws IOException
    {
        final StoreFactory storeFactory = new StoreFactory(
                fileSystem, workingDir, pageCache, DEV_NULL, new Monitors() );

        try ( PropertyStore propertyStore = storeFactory.newPropertyStore( propertyStorePath );
              NodeStore nodeStore = storeFactory.newNodeStore( nodeStorePath );
              SchemaStore schemaStore = storeFactory.newSchemaStore( schemaStorePath ) )
        {
            PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters = collectConflictingProperties( propertyStore );
            resolveConflicts( duplicateClusters, propertyStore, nodeStore, schemaStore );
        }
    }

    private PrimitiveLongObjectMap<List<DuplicateCluster>> collectConflictingProperties( final PropertyStore store )
            throws IOException
    {
        final PrimitiveLongObjectMap<List<DuplicateCluster>> duplicateClusters = Primitive.longObjectMap();

        for ( long headRecordId = 0; headRecordId < store.getHighestPossibleIdInUse(); ++headRecordId )
        {
            final long localHeadRecordId = headRecordId;
            PropertyRecord record = store.forceGetRecord( headRecordId );
            // Skip property propertyRecordIds that are not in use.
            // Skip property propertyRecordIds that are not at the start of a chain.
            if ( !record.inUse() || record.getPrevProp() != -1 )
            {
                continue;
            }

            long propertyId = headRecordId;
            while ( propertyId != Record.NO_NEXT_PROPERTY.intValue() )
            {
                record = store.getRecord( propertyId );

                List<PropertyBlock> propertyBlocks = record.getPropertyBlocks();
                scanForDuplicates( propertyId, propertyBlocks );

                propertyId = record.getNextProp();
            }

            localDuplicateClusters.visitEntries(new PrimitiveIntObjectVisitor<DuplicateCluster>() {
                @Override
                public void visited(int key, DuplicateCluster duplicateCluster) {
                    List<DuplicateCluster> clusters = duplicateClusters.get(localHeadRecordId);
                    if (clusters == null) {
                        clusters = new ArrayList<>();
                        duplicateClusters.put(localHeadRecordId, clusters);
                    }
                    clusters.add(duplicateCluster);
                }
            });

            seenPropertyKeys.clear();
            localDuplicateClusters.clear();
        }

        return duplicateClusters;
    }

    private void scanForDuplicates( long propertyId,
                                    List<PropertyBlock> propertyBlocks )
    {
        for (PropertyBlock block : propertyBlocks) {
            int propertyKeyId = block.getKeyIndexId();

            // If we've seen this property key in this chain before, we schedule the newly found
            // duplicate for removal.
            if (seenPropertyKeys.containsKey(propertyKeyId)) {
                DuplicateCluster cluster = localDuplicateClusters.get(propertyKeyId);
                if (cluster == null) {
                    cluster = new DuplicateCluster(propertyKeyId);
                    localDuplicateClusters.put(propertyKeyId, cluster);
                }
                cluster.add(seenPropertyKeys.get(propertyKeyId));
                cluster.add(propertyId);
            } else {
                seenPropertyKeys.put(propertyKeyId, propertyId);
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
            // The happiest of cases.
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
            nodeStore.processRecords( 0, nodeStore.getHighestPossibleIdInUse(), indexedConflictsResolver );
        }

        // Initially resolve all duplicateClusters by changing the propertyKey for the first conflicting property block, to
        // one that is prefixed with "__DUPLICATE_<key>".
        PropertyKeyTokenStore keyTokenStore = propertyStore.getPropertyKeyTokenStore();
        try
        {
            NonIndexedConflictResolver resolver = new NonIndexedConflictResolver(keyTokenStore, propertyStore);
            duplicateClusters.visitEntries( resolver );
        }
        catch ( InnerIterationIOException e )
        {
            throw e.getCause();
        }
    }
}
