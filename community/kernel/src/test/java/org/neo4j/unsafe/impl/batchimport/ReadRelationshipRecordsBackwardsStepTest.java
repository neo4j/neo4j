/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Test;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReadRelationshipRecordsBackwardsStepTest
{
    @Test
    public void reservedIdIsSkipped()
    {
        long highId = 5;
        int batchSize = (int) highId;
        RelationshipStore store = StoreWithReservedId.newRelationshipStoreMock( highId );
        when( store.getHighId() ).thenReturn( highId );

        ReadRelationshipRecordsBackwardsStep step = new ReadRelationshipRecordsBackwardsStep(
                mock( StageControl.class ), Configuration.DEFAULT, store, 0 );

        Object batch = step.nextBatchOrNull( 0, batchSize );

        assertNotNull( batch );

        RelationshipRecord[] records = (RelationshipRecord[]) batch;
        boolean hasRecordWithReservedId = Stream.of( records ).anyMatch( recordWithReservedId() );
        assertFalse( "Batch contains record with reserved id " + Arrays.toString( records ), hasRecordWithReservedId );
    }

    private static Predicate<RelationshipRecord> recordWithReservedId()
    {
        return record -> record.getId() == IdGeneratorImpl.INTEGER_MINUS_ONE;
    }
}
