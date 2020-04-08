/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Map;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.EntityTokenUpdate;

import static org.neo4j.internal.helpers.collection.Iterables.asIterable;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.EntityTokenUpdate.tokenChanges;

public class FullCheckIntegrationWithRelationshipTypeScanStoreTest extends FullCheckIntegrationTest
{
    @Override
    protected Map<Setting<?>,Object> getSettings()
    {
        Map<Setting<?>,Object> settings = super.getSettings();
        settings.put( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
        return settings;
    }

    @Test
    void shouldReportRelationshipTypeScanStoreInconsistencies1() throws Exception
    {
        // given
        // relationship present in type index but not in store
        GraphStoreFixture.IdGenerator idGenerator = fixture.idGenerator();
        long relationshipId = idGenerator.relationship();
        long relationshipTypeId = idGenerator.relationshipType() - 1;

        RelationshipTypeScanStore relationshipTypeScanStore = fixture.directStoreAccess().relationshipTypeScanStore();
        Iterable<EntityTokenUpdate> relationshipTypeUpdates = asIterable( tokenChanges( relationshipId, new long[]{}, new long[]{relationshipTypeId} ) );
        write( relationshipTypeScanStore, relationshipTypeUpdates );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, 1 )
                .andThatsAllFolks();
    }

    @Test
    void shouldReportRelationshipTypeScanStoreInconsistencies2() throws Exception
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
}
