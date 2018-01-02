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
package org.neo4j.kernel.impl.store.id;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.function.Supplier;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.test.EphemeralFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BufferingIdGeneratorFactoryTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldDelayFreeingOfAggressivelyReusedIds() throws Exception
    {
        // GIVEN
        MockedIdGeneratorFactory actual = new MockedIdGeneratorFactory();
        CommunityIdTypeConfigurationProvider idTypeConfigurationProvider =
                new CommunityIdTypeConfigurationProvider();
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( actual, idTypeConfigurationProvider );
        ControllableSnapshotSupplier boundaries = new ControllableSnapshotSupplier();
        IdGenerator idGenerator = bufferingIdGeneratorFactory.open(
                new File( "doesnt-matter" ), 10, IdType.STRING_BLOCK, 0 );
        bufferingIdGeneratorFactory.initialize( boundaries, IdReuseEligibility.ALWAYS );

        // WHEN
        idGenerator.freeId( 7 );
        verifyNoMoreInteractions( actual.get( IdType.STRING_BLOCK ) );

        // after some maintenance and transaction still not closed
        bufferingIdGeneratorFactory.maintenance();
        verifyNoMoreInteractions( actual.get( IdType.STRING_BLOCK ) );

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance();

        // THEN
        verify( actual.get( IdType.STRING_BLOCK ) ).freeId( 7 );
    }

    @Test
    public void shouldDelayFreeingOfAggressivelyReusedIdsConsideringTimeAsWell() throws Exception
    {
        // GIVEN
        MockedIdGeneratorFactory actual = new MockedIdGeneratorFactory();
        final FakeClock clock = new FakeClock();
        final long safeZone = MINUTES.toMillis( 1 );
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( actual,
                new CommunityIdTypeConfigurationProvider() );
        ControllableSnapshotSupplier boundaries = new ControllableSnapshotSupplier();
        IdGenerator idGenerator = bufferingIdGeneratorFactory.open(
                new File( "doesnt-matter" ), 10, IdType.STRING_BLOCK, 0 );
        bufferingIdGeneratorFactory.initialize( boundaries, new IdReuseEligibility()
        {
            @Override
            public boolean isEligible( KernelTransactionsSnapshot t )
            {
                return clock.currentTimeMillis() - t.snapshotTime() >= safeZone;
            }
        } );

        // WHEN
        idGenerator.freeId( 7 );
        verifyNoMoreInteractions( actual.get( IdType.STRING_BLOCK ) );

        // after some maintenance and transaction still not closed
        bufferingIdGeneratorFactory.maintenance();
        verifyNoMoreInteractions( actual.get( IdType.STRING_BLOCK ) );

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance();
        // ... the clock would still say "nope" so no interaction
        verifyNoMoreInteractions( actual.get( IdType.STRING_BLOCK ) );

        // then finally after time has passed as well
        clock.forward( 70, SECONDS );
        bufferingIdGeneratorFactory.maintenance();

        // THEN
        verify( actual.get( IdType.STRING_BLOCK ) ).freeId( 7 );
    }

    private static class ControllableSnapshotSupplier implements Supplier<KernelTransactionsSnapshot>
    {
        KernelTransactionsSnapshot mostRecentlyReturned;

        @Override
        public KernelTransactionsSnapshot get()
        {
            return mostRecentlyReturned = mock( KernelTransactionsSnapshot.class );
        }

        void setMostRecentlyReturnedSnapshotToAllClosed()
        {
            when( mostRecentlyReturned.allClosed() ).thenReturn( true );
        }
    }

    private static class MockedIdGeneratorFactory implements IdGeneratorFactory
    {
        private final IdGenerator[] generators = new IdGenerator[IdType.values().length];

        @Override
        public IdGenerator open( File filename, IdType idType, long highId )
        {
            return open( filename, 0, idType, highId );
        }

        @Override
        public IdGenerator open( File filename, int grabSize, IdType idType, long highId )
        {
            return generators[idType.ordinal()] = mock( IdGenerator.class );
        }

        @Override
        public void create( File filename, long highId, boolean throwIfFileExists )
        {
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return generators[idType.ordinal()];
        }
    }
}
