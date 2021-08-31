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
package org.neo4j.internal.id;

import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.id.IdController.IdFreeCondition;
import org.neo4j.internal.id.IdGenerator.Marker;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.test.Race;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.test.Race.throwing;

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
    void setup() throws IOException
    {
        actual = new MockedIdGeneratorFactory();
        boundaries = new ControllableSnapshotSupplier();
        pageCache = mock( PageCache.class );
        bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( actual );
        bufferingIdGeneratorFactory.initialize( boundaries );
        idGenerator = bufferingIdGeneratorFactory.open( pageCache, Path.of( "doesnt-matter" ), TestIdType.TEST, () -> 0L, Integer.MAX_VALUE, writable(),
                Config.defaults(), NULL, immutable.empty(), SINGLE_IDS );
    }

    @Test
    void shouldDelayFreeingOfDeletedIds()
    {
        // WHEN
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markDeleted( 7, 2 );
        }
        verify( actual.markers.get( TestIdType.TEST ) ).markDeleted( 7, 2 );
        verify( actual.markers.get( TestIdType.TEST ) ).close();
        verifyNoMoreInteractions( actual.markers.get( TestIdType.TEST ) );

        // after some maintenance and transaction still not closed
        bufferingIdGeneratorFactory.maintenance( NULL );
        verifyNoMoreInteractions( actual.markers.get( TestIdType.TEST ) );

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance( NULL );

        // THEN
        verify( actual.markers.get( TestIdType.TEST ) ).markFree( 7, 2 );
    }

    @Test
    void shouldHandleDeletingAndFreeingConcurrently()
    {
        // given
        AtomicLong nextId = new AtomicLong();
        AtomicInteger numMaintenanceCalls = new AtomicInteger();
        Race race = new Race().withEndCondition( () -> numMaintenanceCalls.get() >= 10 && nextId.get() >= 1_000 );
        race.addContestants( 4, () ->
        {
            int numIds = ThreadLocalRandom.current().nextInt( 1, 5 );
            try ( Marker marker = idGenerator.marker( NULL ) )
            {
                for ( int i = 0; i < numIds; i++ )
                {
                    marker.markDeleted( nextId.getAndIncrement(), 1 );
                }
            }
        } );
        List<ControllableIdFreeCondition> conditions = new ArrayList<>();
        race.addContestant( throwing( () ->
        {
            bufferingIdGeneratorFactory.maintenance( NULL );
            if ( boundaries.mostRecentlyReturned == null )
            {
                return;
            }

            ControllableIdFreeCondition condition = boundaries.mostRecentlyReturned;
            boundaries.mostRecentlyReturned = null;
            if ( ThreadLocalRandom.current().nextBoolean() )
            {
                // Chance to let this condition be true immediately
                condition.enable();
            }
            if ( ThreadLocalRandom.current().nextBoolean() )
            {
                // Chance to enable a previously disabled condition
                for ( ControllableIdFreeCondition olderCondition : conditions )
                {
                    if ( !olderCondition.eligibleForFreeing() )
                    {
                        olderCondition.enable();
                        break;
                    }
                }
            }
            conditions.add( condition );
            numMaintenanceCalls.incrementAndGet();
        } ) );

        // when
        race.goUnchecked();
        for ( ControllableIdFreeCondition condition : conditions )
        {
            condition.enable();
        }
        boundaries.automaticallyEnableConditions = true;
        bufferingIdGeneratorFactory.maintenance( NULL );
        for ( long id = 0; id < nextId.get(); id++ )
        {
            verify( actual.markers.get( TestIdType.TEST ), times( 1 ) ).markFree( id, 1 );
        }
    }

    private static class ControllableSnapshotSupplier implements Supplier<IdFreeCondition>
    {
        boolean automaticallyEnableConditions;
        volatile ControllableIdFreeCondition mostRecentlyReturned;

        @Override
        public IdFreeCondition get()
        {
            return mostRecentlyReturned = new ControllableIdFreeCondition( automaticallyEnableConditions );
        }

        void setMostRecentlyReturnedSnapshotToAllClosed()
        {
            mostRecentlyReturned.enable();
        }
    }

    private static class ControllableIdFreeCondition implements IdFreeCondition
    {
        private volatile boolean conditionMet;

        ControllableIdFreeCondition( boolean enabled )
        {
            conditionMet = enabled;
        }

        void enable()
        {
            conditionMet = true;
        }

        @Override
        public boolean eligibleForFreeing()
        {
            return conditionMet;
        }
    }

    private static class MockedIdGeneratorFactory implements IdGeneratorFactory
    {
        private final Map<IdType,IdGenerator> generators = new HashMap<>();
        private final Map<IdType,Marker> markers = new HashMap<>();

        @Override
        public IdGenerator open( PageCache pageCache, Path filename, IdType idType, LongSupplier highIdScanner, long maxId,
                DatabaseReadOnlyChecker readOnlyChecker, Config config, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions,
                IdSlotDistribution slotDistribution )
        {
            IdGenerator idGenerator = mock( IdGenerator.class );
            Marker marker = mock( Marker.class );
            generators.put( idType, idGenerator );
            markers.put( idType, marker );
            when( idGenerator.marker( NULL ) ).thenReturn( marker );
            return idGenerator;
        }

        @Override
        public IdGenerator create( PageCache pageCache, Path filename, IdType idType, long highId, boolean throwIfFileExists, long maxId,
                DatabaseReadOnlyChecker readOnlyChecker, Config config, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions,
                IdSlotDistribution slotDistribution )
        {
            return open( pageCache, filename, idType, () -> highId, maxId, readOnlyChecker, config, cursorContext, openOptions, SINGLE_IDS );
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }

        @Override
        public void visit( Consumer<IdGenerator> visitor )
        {
            generators.values().forEach( visitor );
        }

        @Override
        public void clearCache( CursorContext cursorContext )
        {
            // no-op
        }

        @Override
        public Collection<Path> listIdFiles()
        {
            return Collections.emptyList();
        }
    }
}
