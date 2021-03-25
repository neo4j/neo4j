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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.IntPredicate;

import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanReader;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.lock.LockService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StubStorageCursors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class RelationshipTypeViewRelationshipStoreScanTest
{
    private final StubStorageCursors cursors = new StubStorageCursors();
    private final RelationshipTypeScanStore relationshipTypeScanStore = mock( RelationshipTypeScanStore.class );
    private final TokenScanReader relationshipTypeScanReader = mock( TokenScanReader.class );
    private final IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private final TokenScanConsumer typeScanConsumer = mock( TokenScanConsumer.class );
    private final PropertyScanConsumer propertyScanConsumer = mock( PropertyScanConsumer.class );
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @BeforeEach
    void setUp()
    {
        when( relationshipTypeScanStore.newReader() ).thenReturn( relationshipTypeScanReader );
    }

    @Test
    void iterateOverRelationshipIds()
    {
        PrimitiveLongResourceIterator relationshipsWithType = PrimitiveLongResourceCollections.iterator( null, 1, 2, 4, 8 );

        long highId = 15L;
        for ( long i = 0; i < highId; i++ )
        {
            cursors.withRelationship( i, 1, 0, 1 );
        }
        int[] types = new int[]{1};
        when( relationshipTypeScanReader.entitiesWithAnyOfTokens( eq( types ), any() ) ).thenReturn( relationshipsWithType );

        RelationshipTypeViewRelationshipStoreScan storeScan = getRelationshipTypeScanViewStoreScan( types );
        PrimitiveLongResourceIterator idIterator = storeScan.getEntityIdIterator( PageCursorTracer.NULL );

        assertThat( idIterator.next() ).isEqualTo( 1L );
        assertThat( idIterator.next() ).isEqualTo( 2L );
        assertThat( idIterator.next() ).isEqualTo( 4L );
        assertThat( idIterator.next() ).isEqualTo( 8L );
        assertThat( idIterator.hasNext() ).isEqualTo( false );
    }

    private RelationshipTypeViewRelationshipStoreScan getRelationshipTypeScanViewStoreScan( int[] relationshipTypeIds )
    {
        return new RelationshipTypeViewRelationshipStoreScan( Config.defaults(), cursors, LockService.NO_LOCK_SERVICE, relationshipTypeScanStore,
                typeScanConsumer, propertyScanConsumer, relationshipTypeIds, propertyKeyIdFilter, false, jobScheduler, PageCacheTracer.NULL, INSTANCE );
    }
}
