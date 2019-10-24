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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.internal.id.IdController.ConditionSnapshot;
import org.neo4j.internal.id.IdGenerator.Marker;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
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

    private MockedIdGeneratorFactory actual;
    private ControllableSnapshotSupplier boundaries;
    private PageCache pageCache;
    private BufferingIdGeneratorFactory bufferingIdGeneratorFactory;
    private IdGenerator idGenerator;

    @BeforeEach
    void setup()
    {
        actual = new MockedIdGeneratorFactory();
        boundaries = new ControllableSnapshotSupplier();
        pageCache = mock( PageCache.class );
        bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( actual );
        bufferingIdGeneratorFactory.initialize( boundaries );
        idGenerator = bufferingIdGeneratorFactory.open( pageCache, new File( "doesnt-matter" ), IdType.STRING_BLOCK, () -> 0L, Integer.MAX_VALUE, false );
    }

    @Test
    void shouldDelayFreeingOfDeletedIds()
    {
        // WHEN
        try ( Marker marker = idGenerator.marker() )
        {
            marker.markDeleted( 7 );
        }
        verify( actual.markers[STRING_BLOCK.ordinal()] ).markDeleted( 7 );
        verify( actual.markers[STRING_BLOCK.ordinal()] ).close();
        verifyNoMoreInteractions( actual.markers[STRING_BLOCK.ordinal()] );

        // after some maintenance and transaction still not closed
        bufferingIdGeneratorFactory.maintenance();
        verifyNoMoreInteractions( actual.markers[STRING_BLOCK.ordinal()] );

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance();

        // THEN
        verify( actual.markers[STRING_BLOCK.ordinal()] ).markFree( 7 );
    }

    @Test
    void shouldDelayFreeingOfDeletedIdsUntilCheckpoint()
    {
        // WHEN
        try ( Marker marker = idGenerator.marker() )
        {
            marker.markDeleted( 7 );
        }
        verify( actual.markers[STRING_BLOCK.ordinal()] ).markDeleted( 7 );
        verify( actual.markers[STRING_BLOCK.ordinal()] ).close();
        verifyNoMoreInteractions( actual.markers[STRING_BLOCK.ordinal()] );

        // after some maintenance and transaction still not closed
        idGenerator.checkpoint( IOLimiter.UNLIMITED );
        verifyNoMoreInteractions( actual.markers[STRING_BLOCK.ordinal()] );

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        idGenerator.checkpoint( IOLimiter.UNLIMITED );

        // THEN
        verify( actual.markers[STRING_BLOCK.ordinal()] ).markFree( 7 );
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
        private final Marker[] markers = new Marker[IdType.values().length];

        @Override
        public IdGenerator open( PageCache pageCache, File filename, IdType idType, LongSupplier highIdScanner, long maxId, boolean readOnly,
                OpenOption... openOptions )
        {
            IdGenerator idGenerator = mock( IdGenerator.class );
            Marker marker = mock( Marker.class );
            int ordinal = idType.ordinal();
            generators[ordinal] = idGenerator;
            markers[ordinal] = marker;
            when( idGenerator.marker() ).thenReturn( marker );
            return idGenerator;
        }

        @Override
        public IdGenerator create( PageCache pageCache, File filename, IdType idType, long highId, boolean throwIfFileExists, long maxId,
                boolean readOnly, OpenOption... openOptions )
        {
            return open( pageCache, filename, idType, () -> highId, maxId, readOnly, openOptions );
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

        @Override
        public void clearCache()
        {
            // no-op
        }

        @Override
        public Collection<File> listIdFiles()
        {
            return Collections.emptyList();
        }
    }
}
