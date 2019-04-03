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
package org.neo4j.internal.id;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.id.IdController.ConditionSnapshot;
import org.neo4j.internal.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.id.IdType.STRING_BLOCK;

@ExtendWith( EphemeralFileSystemExtension.class )
class BufferingIdGeneratorFactoryTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Test
    void shouldDelayFreeingOfAggressivelyReusedIds()
    {
        // GIVEN
        MockedIdGeneratorFactory actual = new MockedIdGeneratorFactory();
        ControllableSnapshotSupplier boundaries = new ControllableSnapshotSupplier();
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( actual, new CommunityIdTypeConfigurationProvider() );
        bufferingIdGeneratorFactory.initialize( boundaries );
        IdGenerator idGenerator = bufferingIdGeneratorFactory.open( new File( "doesnt-matter" ), 10, STRING_BLOCK, () -> 0L, Integer.MAX_VALUE );

        // WHEN
        idGenerator.freeId( 7 );
        verifyNoMoreInteractions( actual.get( STRING_BLOCK ) );

        // after some maintenance and transaction still not closed
        bufferingIdGeneratorFactory.maintenance();
        verifyNoMoreInteractions( actual.get( STRING_BLOCK ) );

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance();

        // THEN
        verify( actual.get( STRING_BLOCK ) ).freeId( 7 );
    }

    private static class ControllableSnapshotSupplier implements Supplier<ConditionSnapshot>
    {
        ConditionSnapshot mostRecentlyReturned;

        @Override
        public ConditionSnapshot get()
        {
            return mostRecentlyReturned = mock( ConditionSnapshot.class );
        }

        void setMostRecentlyReturnedSnapshotToAllClosed()
        {
            when( mostRecentlyReturned.conditionMet() ).thenReturn( true );
        }
    }

    private static class MockedIdGeneratorFactory implements IdGeneratorFactory
    {
        private final IdGenerator[] generators = new IdGenerator[IdType.values().length];

        @Override
        public IdGenerator open( File filename, IdType idType, LongSupplier highId, long maxId )
        {
            return open( filename, 0, idType, highId, maxId );
        }

        @Override
        public IdGenerator open( File filename, int grabSize, IdType idType, LongSupplier highId, long maxId )
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
