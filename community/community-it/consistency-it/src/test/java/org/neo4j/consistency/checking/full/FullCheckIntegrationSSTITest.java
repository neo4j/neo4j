/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.checking.full;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.common.EntityType;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;

import static org.neo4j.io.pagecache.tracing.cursor.CursorContext.NULL;

public class FullCheckIntegrationSSTITest extends FullCheckIntegrationTest
{
    @Override
    protected Map<Setting<?>,Object> getSettings()
    {
        Map<Setting<?>,Object> settings = super.getSettings();
        settings.put( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
        return settings;
    }

    @Test
    void shouldReportRelationshipTypeIndexInconsistencies1() throws Exception
    {
        // given
        // relationship present in type index but not in store
        GraphStoreFixture.IdGenerator idGenerator = fixture.idGenerator();
        long relationshipId = idGenerator.relationship();
        long relationshipTypeId = idGenerator.relationshipType() - 1;

        IndexDescriptor rtiDescriptor = findTokenIndex( fixture, EntityType.RELATIONSHIP );
        IndexAccessor accessor = fixture.indexAccessorLookup().apply( rtiDescriptor );
        try ( IndexUpdater indexUpdater = accessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            indexUpdater.process( IndexEntryUpdate.change( relationshipId, rtiDescriptor, new long[]{}, new long[]{relationshipTypeId} ) );
        }

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, 1 )
                .andThatsAllFolks();
    }

    @Test
    void shouldReportRelationshipTypeIndexInconsistencies2() throws Exception
    {
        // given
        // type index and store has different type for same relationship
        RecordStore<RelationshipRecord> relationshipStore = fixture.directStoreAccess().nativeStores().getRelationshipStore();
        GraphStoreFixture.IdGenerator idGenerator = fixture.idGenerator();
        RelationshipRecord relationshipRecord = new RelationshipRecord( 0 );

        long relationshipId = idGenerator.relationship() - 1;
        relationshipStore.getRecord( relationshipId, relationshipRecord, RecordLoad.NORMAL, NULL );
        relationshipRecord.setType( relationshipRecord.getType() + 1 );
        relationshipStore.updateRecord( relationshipRecord, NULL );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, 2 )
                .verify( RecordType.COUNTS, 2 )
                .andThatsAllFolks();
    }

    private IndexDescriptor findTokenIndex( GraphStoreFixture fixture, EntityType entityType )
    {
        Iterator<IndexDescriptor> indexDescriptors = fixture.getIndexDescriptors();
        while ( indexDescriptors.hasNext() )
        {
            IndexDescriptor next = indexDescriptors.next();
            if ( next.isTokenIndex() && next.schema().entityType() == entityType )
            {
                return next;
            }
        }
        throw new RuntimeException( entityType + " index missing" );
    }

    @Override
    void writeToNodeLabelStructure( GraphStoreFixture fixture, Iterable<EntityTokenUpdate> entityTokenUpdates ) throws IOException, IndexEntryConflictException
    {
        IndexDescriptor tokenIndex = findTokenIndex( fixture, EntityType.NODE );
        IndexAccessor accessor = fixture.indexAccessorLookup().apply( tokenIndex );
        try ( IndexUpdater indexUpdater = accessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            for ( EntityTokenUpdate entityTokenUpdate : entityTokenUpdates )
            {
                indexUpdater.process( IndexEntryUpdate
                        .change( entityTokenUpdate.getEntityId(), tokenIndex, entityTokenUpdate.getTokensBefore(), entityTokenUpdate.getTokensAfter() ) );
            }
        }
    }
}
