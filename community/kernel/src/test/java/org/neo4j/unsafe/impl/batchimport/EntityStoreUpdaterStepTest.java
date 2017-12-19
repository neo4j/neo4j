/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.ForcedSecondaryUnitRecordFormats;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.SimpleStageControl;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.PrepareIdSequence;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.store.StoreType.PROPERTY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;

public class EntityStoreUpdaterStepTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Test
    public void shouldAllocateDoubleRecordUnitsNextToRecord() throws Exception
    {
        // given
        RecordFormats formats = new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS );
        try ( NeoStores stores = new StoreFactory( storage.directory().absolutePath(), Config.defaults(),
                new DefaultIdGeneratorFactory( storage.fileSystem() ), storage.pageCache(), storage.fileSystem(), formats,
                NullLogProvider.getInstance() ).openNeoStores( true, RELATIONSHIP, PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING ) )
        {
            StageControl control = new SimpleStageControl();
            PrepareIdSequence idSequence = PrepareIdSequence.of( true );
            int batchSize = 100;
            stores.getRelationshipStore().setHighId( batchSize * 10 );
            Batch<InputRelationship,RelationshipRecord> batch = batchOfRelationshipsWithPreallocatedSecondaryUnits( batchSize );
            try ( EntityStoreUpdaterStep<RelationshipRecord,InputRelationship> step = new EntityStoreUpdaterStep<>( control,
                    DEFAULT, stores.getRelationshipStore(), stores.getPropertyStore(), mock( IoMonitor.class ),
                    mock( EntityStoreUpdaterStep.Monitor.class ), idSequence ) )
            {
                step.start( 0 );

                // when
                step.receive( 0, batch );
                step.endOfUpstream();
                while ( !step.isCompleted() )
                {
                    Thread.sleep( 10 );
                    control.assertHealthy();
                }
            }

            // then
            for ( int i = 0; i < batchSize; i++ )
            {
                RelationshipRecord record = batch.records[i];
                assertTrue( record.hasSecondaryUnitId() );
                assertEquals( record.getId() + 1, record.getSecondaryUnitId() );
            }
        }
    }

    @Test
    public void shouldSkipNullAndUnusedRecords() throws Exception
    {
        // given
        RecordFormats formats = new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS );
        try ( NeoStores stores = new StoreFactory( storage.directory().absolutePath(), Config.defaults(),
                new DefaultIdGeneratorFactory( storage.fileSystem() ), storage.pageCache(), storage.fileSystem(), formats,
                NullLogProvider.getInstance() ).openNeoStores( true, RELATIONSHIP, PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING ) )
        {
            StageControl control = new SimpleStageControl();
            PrepareIdSequence idSequence = PrepareIdSequence.of( false );
            int batchSize = 100;
            RelationshipStore relationshipStore = stores.getRelationshipStore();
            relationshipStore.setHighId( batchSize * 10 );
            Batch<InputRelationship,RelationshipRecord> batch = batchOfRelationshipsWithPreallocatedSecondaryUnits( batchSize );
            int expectedCount = 0;
            for ( int i = 0; i < batchSize; i++ )
            {
                if ( i % 3 == 0 )
                {
                    batch.records[i].setInUse( false );
                }
                else if ( i % 3 == 1 )
                {
                    batch.records[i] = null;
                }
                else
                {
                    expectedCount++;
                }
            }

            try ( EntityStoreUpdaterStep<RelationshipRecord,InputRelationship> step = new EntityStoreUpdaterStep<>( control,
                    DEFAULT, relationshipStore, stores.getPropertyStore(), mock( IoMonitor.class ),
                    mock( EntityStoreUpdaterStep.Monitor.class ), idSequence ) )
            {
                step.start( 0 );

                // when
                step.receive( 0, batch );
                step.endOfUpstream();
                while ( !step.isCompleted() )
                {
                    Thread.sleep( 10 );
                    control.assertHealthy();
                }
            }

            // then
            long highId = relationshipStore.getHighId();
            int count = 0;
            try ( RecordCursor<RelationshipRecord> cursor =
                    relationshipStore.newRecordCursor( relationshipStore.newRecord() ).acquire( 0, CHECK ) )
            {
                for ( long id = 0; id < highId; id++ )
                {
                    if ( cursor.next( id ) )
                    {
                        count++;
                    }
                }
            }
            assertEquals( expectedCount, count );
        }
    }

    private Batch<InputRelationship,RelationshipRecord> batchOfRelationshipsWithPreallocatedSecondaryUnits( int batchSize )
    {
        Batch<InputRelationship,RelationshipRecord> batch = new Batch<>( new InputRelationship[batchSize] );
        batch.records = new RelationshipRecord[batchSize];
        batch.propertyRecords = new PropertyRecord[batchSize][];
        for ( int i = 0; i < batchSize; i++ )
        {
            batch.records[i] = new RelationshipRecord( i * 2 );
            batch.records[i].setInUse( true );
        }
        return batch;
    }
}
