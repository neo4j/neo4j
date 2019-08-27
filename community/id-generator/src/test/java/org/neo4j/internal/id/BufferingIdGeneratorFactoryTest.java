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
import java.nio.file.OpenOption;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.internal.id.IdController.ConditionSnapshot;
import org.neo4j.internal.id.IdGenerator.CommitMarker;
import org.neo4j.internal.id.IdGenerator.ReuseMarker;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
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
    void shouldDelayFreeingOfDeletedIds()
    {
        // GIVEN
        MockedIdGeneratorFactory actual = new MockedIdGeneratorFactory();
        ControllableSnapshotSupplier boundaries = new ControllableSnapshotSupplier();
        PageCache pageCache = mock( PageCache.class );
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( actual );
        bufferingIdGeneratorFactory.initialize( boundaries );
        IdGenerator idGenerator = bufferingIdGeneratorFactory.open( pageCache, new File( "doesnt-matter" ), IdType.STRING_BLOCK, () -> 0L, Integer.MAX_VALUE );

        // WHEN
        try ( CommitMarker marker = idGenerator.commitMarker() )
        {
            marker.markDeleted( 7 );
        }
        verify( actual.commitMarkers[STRING_BLOCK.ordinal()] ).markDeleted( 7 );
        verifyNoMoreInteractions( actual.reuseMarkers[STRING_BLOCK.ordinal()] );

        // after some maintenance and transaction still not closed
        bufferingIdGeneratorFactory.maintenance();
        verifyNoMoreInteractions( actual.reuseMarkers[STRING_BLOCK.ordinal()] );

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance();

        // THEN
        verify( actual.reuseMarkers[STRING_BLOCK.ordinal()] ).markFree( 7 );
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
        private final CommitMarker[] commitMarkers = new CommitMarker[IdType.values().length];
        private final ReuseMarker[] reuseMarkers = new ReuseMarker[IdType.values().length];

        @Override
        public IdGenerator open( PageCache pageCache, File filename, IdType idType, LongSupplier highIdScanner, long maxId, OpenOption... openOptions )
        {
            IdGenerator idGenerator = mock( IdGenerator.class );
            CommitMarker commitMarker = mock( CommitMarker.class );
            ReuseMarker reuseMarker = mock( ReuseMarker.class );
            int ordinal = idType.ordinal();
            generators[ordinal] = idGenerator;
            commitMarkers[ordinal] = commitMarker;
            reuseMarkers[ordinal] = reuseMarker;
            when( idGenerator.commitMarker() ).thenReturn( commitMarker );
            when( idGenerator.reuseMarker() ).thenReturn( reuseMarker );
            return idGenerator;
        }

        @Override
        public IdGenerator create( PageCache pageCache, File filename, IdType idType, long highId, boolean throwIfFileExists, long maxId,
                OpenOption... openOptions )
        {
            return open( pageCache, filename, idType, () -> highId, maxId, openOptions );
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return generators[idType.ordinal()];
        }

        @Override
        public void visit( Consumer<IdGenerator> visitor )
        {
            Stream.of( generators ).forEach( visitor );
        }
    }
}
