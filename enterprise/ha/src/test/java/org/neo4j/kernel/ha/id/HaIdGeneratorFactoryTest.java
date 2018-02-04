/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdRangeIterator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.kernel.impl.store.id.IdGeneratorImpl.INTEGER_MINUS_ONE;
import static org.neo4j.kernel.impl.store.id.IdRangeIterator.VALUE_REPRESENTING_NULL;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.logging.NullLogProvider.getInstance;


public class HaIdGeneratorFactoryTest
{
    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private Master master;
    private DelegateInvocationHandler<Master> masterDelegate;
    private EphemeralFileSystemAbstraction fs;
    private HaIdGeneratorFactory fac;

    @BeforeEach
    public void before()
    {
        master = mock( Master.class );
        masterDelegate = new DelegateInvocationHandler<>( Master.class );
        fs = fileSystemRule.get();
        fac  = new HaIdGeneratorFactory( masterDelegate, getInstance(),
                mock( RequestContextFactory.class ), fs, new CommunityIdTypeConfigurationProvider()  );
    }

    @Test
    public void slaveIdGeneratorShouldReturnFromAssignedRange()
    {
        // GIVEN
        IdAllocation firstResult = new IdAllocation( new IdRange( new long[]{}, 42, 123 ), 123, 0 );
        Response<IdAllocation> response = response( firstResult );
        when( master.allocateIds( isNull(), any( IdType.class ) ) ).thenReturn( response );

        // WHEN
        IdGenerator gen = switchToSlave();

        // THEN
        for ( long i = firstResult.getIdRange().getRangeStart(); i < firstResult.getIdRange().getRangeLength(); i++ )
        {
            assertEquals(i, gen.nextId());
        }
        verify( master, times( 1 ) ).allocateIds( isNull(), eq( NODE ) );
    }

    @Test
    public void slaveIdGeneratorShouldAskForMoreWhenRangeIsOver()
    {
        // GIVEN
        IdAllocation firstResult = new IdAllocation( new IdRange( new long[]{}, 42, 123 ), 42 + 123, 0 );
        IdAllocation secondResult = new IdAllocation( new IdRange( new long[]{}, 1042, 223 ), 1042 + 223, 0 );
        Response<IdAllocation> response = response( firstResult, secondResult );
        when( master.allocateIds( isNull(), any( IdType.class ) ) ).thenReturn( response );

        // WHEN
        IdGenerator gen = switchToSlave();

        // THEN
        long startAt = firstResult.getIdRange().getRangeStart();
        long forThatMany = firstResult.getIdRange().getRangeLength();
        for ( long i = startAt; i < startAt + forThatMany; i++ )
        {
            assertEquals( i, gen.nextId() );
        }
        verify( master, times( 1 ) ).allocateIds( isNull(), eq( NODE ) );

        startAt = secondResult.getIdRange().getRangeStart();
        forThatMany = secondResult.getIdRange().getRangeLength();
        for ( long i = startAt; i < startAt + forThatMany; i++ )
        {
            assertEquals( i, gen.nextId() );
        }

        verify( master, times( 2 ) ).allocateIds( isNull(), eq( NODE ) );
    }

    @Test
    public void shouldUseDefraggedIfPresent()
    {
        // GIVEN
        long[] defragIds = {42, 27172828, 314159};
        IdAllocation firstResult = new IdAllocation( new IdRange( defragIds, 0, 0 ), 0, defragIds.length );
        Response<IdAllocation> response = response( firstResult );
        when( master.allocateIds( isNull(), any( IdType.class ) ) ).thenReturn( response );

        // WHEN
        IdGenerator gen = switchToSlave();

        // THEN
        for ( long defragId : defragIds )
        {
            assertEquals( defragId, gen.nextId() );
        }
    }

    @Test
    public void shouldMoveFromDefraggedToRange()
    {
        // GIVEN
        long[] defragIds = {42, 27172828, 314159};
        IdAllocation firstResult = new IdAllocation( new IdRange( defragIds, 0, 10 ), 100, defragIds.length );
        Response<IdAllocation> response = response( firstResult );
        when( master.allocateIds( isNull(), any( IdType.class ) ) ).thenReturn( response );

        // WHEN
        IdGenerator gen = switchToSlave();

        // THEN
        for ( long defragId : defragIds )
        {
            assertEquals( defragId, gen.nextId() );
        }
    }

    @Test
    public void slaveShouldNeverAllowReducingHighId()
    {
        // GIVEN
        final int highIdFromAllocation = 123;
        IdAllocation firstResult = new IdAllocation( new IdRange( new long[] {}, 42, highIdFromAllocation ),
                highIdFromAllocation, 0 );
        Response<IdAllocation> response = response( firstResult );
        when( master.allocateIds( isNull(), any( IdType.class ) ) ).thenReturn( response );

        // WHEN
        IdGenerator gen = switchToSlave();
        final int highIdFromUpdatedRecord = highIdFromAllocation + 1;
        gen.setHighId( highIdFromUpdatedRecord ); // Assume this is from a received transaction
        gen.nextId(); // that will ask the master for an IdRange

        // THEN
        assertEquals( highIdFromUpdatedRecord, gen.getHighId() );
    }

    @Test
    public void shouldDeleteIdGeneratorsAsPartOfSwitchingToSlave()
    {
        // GIVEN we're in master mode. We do that to allow HaIdGeneratorFactory to open id generators at all
        fac.switchToMaster();
        File idFile = new File( "my.id" );
        // ... opening an id generator as master
        fac.create( idFile, 10, true );
        IdGenerator idGenerator = fac.open( idFile, 10, NODE, () -> 10L, LATEST_RECORD_FORMATS.node().getMaxId() );
        assertTrue( fs.fileExists( idFile ) );
        idGenerator.close();

        // WHEN switching to slave
        fac.switchToSlave();

        // THEN the .id file underneath should be deleted
        assertFalse( fs.fileExists( idFile ), "Id file should've been deleted by now" );
    }

    @Test
    public void shouldDeleteIdGeneratorsAsPartOfOpenAfterSwitchingToSlave()
    {
        // GIVEN we're in master mode. We do that to allow HaIdGeneratorFactory to open id generators at all
        fac.switchToSlave();
        File idFile = new File( "my.id" );
        // ... opening an id generator as master
        fac.create( idFile, 10, true );

        // WHEN
        IdGenerator idGenerator = fac.open( idFile, 10, NODE, () -> 10L, LATEST_RECORD_FORMATS.node().getMaxId() );

        // THEN
        assertFalse( fs.fileExists( idFile ), "Id file should've been deleted by now" );
    }

    @Test
    public void shouldTranslateComExceptionsIntoTransientTransactionFailures()
    {
        assertThrows( TransientTransactionFailureException.class, () -> {
            when( master.allocateIds( isNull(), any( IdType.class ) ) ).thenThrow( new ComException() );
            IdGenerator generator = switchToSlave();
            generator.nextId();
        } );
    }

    @Test
    public void shouldNotUseForbiddenMinusOneIdFromIdBatches()
    {
        // GIVEN
        long[] defragIds = {3, 5};
        int size = 10;
        long low = INTEGER_MINUS_ONE - size / 2;
        IdRange idRange = new IdRange( defragIds, low, size );

        // WHEN
        IdRangeIterator iterartor = idRange.iterator();

        // THEN
        for ( long id : defragIds )
        {
            assertEquals( id, iterartor.nextId() );
        }

        int expectedRangeSize = size - 1; // due to the forbidden id
        for ( long i = 0, expectedId = low; i < expectedRangeSize; i++, expectedId++ )
        {
            if ( expectedId == INTEGER_MINUS_ONE )
            {
                expectedId++;
            }

            long id = iterartor.nextId();
            assertNotEquals( INTEGER_MINUS_ONE, id );
            assertEquals( expectedId, id );
        }
        assertEquals( VALUE_REPRESENTING_NULL, iterartor.nextId() );
    }

    @SuppressWarnings( "unchecked" )
    private Response<IdAllocation> response( IdAllocation firstValue, IdAllocation... additionalValues )
    {
        Response<IdAllocation> response = mock( Response.class );
        when( response.response() ).thenReturn( firstValue, additionalValues );
        return response;
    }

    private IdGenerator switchToSlave()
    {
        fac.switchToSlave();
        IdGenerator gen = fac.open( new File( "someFile" ), 10, NODE, () -> 1L, LATEST_RECORD_FORMATS.node().getMaxId() );
        masterDelegate.setDelegate( master );
        return gen;
    }
}
