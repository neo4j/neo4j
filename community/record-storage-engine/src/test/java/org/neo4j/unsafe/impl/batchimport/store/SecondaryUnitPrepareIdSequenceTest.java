/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Test;

import org.neo4j.kernel.impl.store.id.IdSequence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SecondaryUnitPrepareIdSequenceTest
{
    @Test
    public void shouldReturnIdImmediatelyAfterRecordId()
    {
        // given
        PrepareIdSequence idSequence = new SecondaryUnitPrepareIdSequence();
        IdSequence actual = mock( IdSequence.class );

        // when
        long recordId = 10;
        IdSequence prepared = idSequence.apply( actual ).apply( recordId );
        long nextRecordId = prepared.nextId();

        // then
        assertEquals( 10 + 1, nextRecordId );
        verifyNoMoreInteractions( actual );
    }

    @Test
    public void shouldReturnIdImmediatelyAfterRecordIdOnlyOnce()
    {
        // given
        PrepareIdSequence idSequence = new SecondaryUnitPrepareIdSequence();
        IdSequence actual = mock( IdSequence.class );

        // when
        long recordId = 10;
        IdSequence prepared = idSequence.apply( actual ).apply( recordId );
        long nextRecordId = prepared.nextId();
        assertEquals( 10 + 1, nextRecordId );
        verifyNoMoreInteractions( actual );
        try
        {
            prepared.nextId();
            fail( "Should've failed" );
        }
        catch ( IllegalStateException e )
        {   // good
        }

        // and when
        recordId = 20;
        prepared = idSequence.apply( actual ).apply( recordId );
        nextRecordId = prepared.nextId();

        // then
        assertEquals( 20 + 1, nextRecordId );
        verifyNoMoreInteractions( actual );
    }
}
