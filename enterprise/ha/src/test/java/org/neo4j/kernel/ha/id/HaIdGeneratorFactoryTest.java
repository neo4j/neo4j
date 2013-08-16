/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.id;

import java.io.File;

import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.com.Response;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdRange;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HaIdGeneratorFactoryTest
{
    @Test
    public void slaveIdGeneratorShouldReturnFromAssignedRange() throws Exception
    {
        // GIVEN
        IdAllocation firstResult = new IdAllocation( new IdRange( new long[]{}, 42, 123 ), 123, 0 );
        Response<IdAllocation> toReturn = mock( Response.class );
        when(toReturn.response()).thenReturn( firstResult );

        Master returning = mock(Master.class);
        when(returning.allocateIds( Matchers.<IdType>any() ) ).thenReturn( toReturn );

        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

        HaIdGeneratorFactory fac  = new HaIdGeneratorFactory( returning, new DevNullLoggingService() );

        // WHEN
        fac.switchToSlave();
        IdGenerator gen = fac.open( fs, new File("someFile"), 10, IdType.NODE, 1 );

        // THEN
        for ( long i = firstResult.getIdRange().getRangeStart(); i < firstResult.getIdRange().getRangeLength(); i++ )
        {
            assertEquals(i, gen.nextId());
        }
        verify( returning, times(1) ).allocateIds( IdType.NODE );
    }

    @Test
    public void slaveIdGeneratorShouldAskForMoreWhenRangeIsOver() throws Exception
    {

        // GIVEN
        IdAllocation firstResult = new IdAllocation( new IdRange( new long[]{}, 42, 123 ), 42 + 123, 0 );
        IdAllocation secondResult = new IdAllocation( new IdRange( new long[]{}, 1042, 223 ), 1042 + 223, 0 );
        Response<IdAllocation> toReturn = mock( Response.class );
        when(toReturn.response()).thenReturn( firstResult, secondResult );

        Master returning = mock(Master.class);
        when(returning.allocateIds( Matchers.<IdType>any() ) ).thenReturn( toReturn );

        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

        HaIdGeneratorFactory fac  = new HaIdGeneratorFactory( returning, new DevNullLoggingService() );

        // WHEN
        fac.switchToSlave();
        IdGenerator gen = fac.open( fs, new File("someFile"), 10, IdType.NODE, 1 );

        // THEN
        long startAt = firstResult.getIdRange().getRangeStart();
        long forThatMany = firstResult.getIdRange().getRangeLength();
        for ( long i =startAt ; i < startAt + forThatMany; i++ )
        {
            assertEquals(i, gen.nextId());
        }
        verify( returning, times(1) ).allocateIds( IdType.NODE );

        startAt = secondResult.getIdRange().getRangeStart();
        forThatMany = secondResult.getIdRange().getRangeLength();
        for ( long i =startAt ; i < startAt + forThatMany; i++ )
        {
            assertEquals(i, gen.nextId());
        }

        verify( returning, times(2) ).allocateIds( IdType.NODE );
    }

    @Test
    public void shouldUseDefraggedIfPresent() throws Exception
    {
        // GIVEN
        long[] defragIds = {42, 27172828, 314159};
        IdAllocation firstResult = new IdAllocation( new IdRange( defragIds, 0, 0 ), 0, defragIds.length );
        Response<IdAllocation> toReturn = mock( Response.class );
        when(toReturn.response()).thenReturn( firstResult );

        Master returning = mock(Master.class);
        when(returning.allocateIds( Matchers.<IdType>any() ) ).thenReturn( toReturn );

        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

        HaIdGeneratorFactory fac  = new HaIdGeneratorFactory( returning, new DevNullLoggingService() );

        // WHEN
        fac.switchToSlave();
        IdGenerator gen = fac.open( fs, new File("someFile"), 10, IdType.NODE, 1 );

        // THEN
        for ( int i = 0; i < defragIds.length; i++ )
        {
            assertEquals(defragIds[i], gen.nextId() );
        }
    }

    @Test
    public void shouldMoveFromDefraggedToRange() throws Exception
    {
        // GIVEN
        long[] defragIds = {42, 27172828, 314159};
        IdAllocation firstResult = new IdAllocation( new IdRange( defragIds, 0, 10 ), 100, defragIds.length );
        Response<IdAllocation> toReturn = mock( Response.class );
        when(toReturn.response()).thenReturn( firstResult );

        Master returning = mock(Master.class);
        when(returning.allocateIds( Matchers.<IdType>any() ) ).thenReturn( toReturn );

        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

        HaIdGeneratorFactory fac  = new HaIdGeneratorFactory( returning, new DevNullLoggingService() );

        // WHEN
        fac.switchToSlave();
        IdGenerator gen = fac.open( fs, new File("someFile"), 10, IdType.NODE, 1 );

        // THEN
        for ( int i = 0; i < defragIds.length; i++ )
        {
            assertEquals(defragIds[i], gen.nextId() );
        }
    }

    @Test
    public void slaveShouldNeverAllowReducingHighId() throws Exception
    {
        // GIVEN
        final int highIdFromAllocation =  123;
        IdAllocation firstResult = new IdAllocation( new IdRange( new long[]{}, 42, highIdFromAllocation ), highIdFromAllocation, 0 );
        Response<IdAllocation> toReturn = mock( Response.class );
        when(toReturn.response()).thenReturn( firstResult );

        Master returning = mock(Master.class);
        when(returning.allocateIds( Matchers.<IdType>any() ) ).thenReturn( toReturn );

        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

        HaIdGeneratorFactory fac  = new HaIdGeneratorFactory( returning, new DevNullLoggingService() );

        // WHEN
        fac.switchToSlave();
        IdGenerator gen = fac.open( fs, new File("someFile"), 10, IdType.PROPERTY, 1 );
        final int highIdFromUpdatedRecord = highIdFromAllocation + 1;
        gen.setHighId( highIdFromUpdatedRecord ); // Assume this is from a received transaction
        gen.nextId(); // that will ask the master for an IdRange

        // THEN
        assertEquals ( highIdFromUpdatedRecord, gen.getHighId() );
    }
}
