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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.test.OtherThreadExecutor.command;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.stringValue;

public class BlockBasedIndexPopulatorTest
{
    private static final StoreIndexDescriptor INDEX_DESCRIPTOR = IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( 1, 1 ) ).withId( 1 );

    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "MERGER" );

    @Rule
    public final OtherThreadRule<Void> t3 = new OtherThreadRule<>( "CLOSER" );

    @Test
    public void shouldAwaitMergeToBeFullyAbortedBeforeLeavingCloseMethod() throws Exception
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor();
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulatorWithSomeData( monitor );

        // when starting to merge (in a separate thread)
        Future<Object> mergeFuture = t2.execute( command( () -> populator.scanCompleted( PhaseTracker.nullInstance ) ) );
        // and waiting for merge to get going
        monitor.barrier.awaitUninterruptibly();
        // calling close here should wait for the merge future, so that checking the merge future for "done" immediately afterwards must say true
        Future<Object> closeFuture = t3.execute( command( () -> populator.close( false ) ) );
        t3.get().waitUntilWaiting();
        monitor.barrier.release();
        closeFuture.get();

        // then
        assertTrue( mergeFuture.isDone() );
    }

    @Test
    public void shouldCorrectlyDecideToAwaitMergeDependingOnProgress() throws Throwable
    {
        // given
        TrappingMonitor monitor = new TrappingMonitor();
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulatorWithSomeData( monitor );

        // when
        Race race = new Race();
        race.addContestant( throwing( () -> populator.scanCompleted( PhaseTracker.nullInstance ) ) );
        race.addContestant( throwing( () -> populator.close( false ) ) );
        race.go();

        // then regardless of who wins (close/merge) after close call returns no files should still be mapped
        EphemeralFileSystemAbstraction ephemeralFileSystem = (EphemeralFileSystemAbstraction) storage.fileSystem();
        ephemeralFileSystem.assertNoOpenFiles();
    }

    private BlockBasedIndexPopulator<GenericKey,NativeIndexValue> instantiatePopulatorWithSomeData( TrappingMonitor monitor )
    {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        Config config = Config.defaults();
        IndexSpecificSpaceFillingCurveSettingsCache spatialSettings =
                new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( config ), new HashMap<>() );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        IndexDirectoryStructure directoryStructure = directoriesByProvider( storage.directory().directory( "schema" ) ).forProvider( providerDescriptor );
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator =
                new BlockBasedIndexPopulator<GenericKey,NativeIndexValue>( storage.pageCache(), storage.fileSystem(), storage.directory().file( "file" ),
                        layout, IndexProvider.Monitor.EMPTY, INDEX_DESCRIPTOR, spatialSettings, directoryStructure, false, 100, 2, monitor )
                {
                    @Override
                    NativeIndexReader<GenericKey,NativeIndexValue> newReader()
                    {
                        throw new UnsupportedOperationException( "Not needed in this test" );
                    }
                };
        populator.create();
        populator.add( batchOfUpdates() );
        return populator;
    }

    private static Collection<IndexEntryUpdate<?>> batchOfUpdates()
    {
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();
        for ( int i = 0; i < 50; i++ )
        {
            updates.add( IndexEntryUpdate.add( i, INDEX_DESCRIPTOR, stringValue( "Value" + i ) ) );
        }
        return updates;
    }

    private static class TrappingMonitor extends BlockStorage.Monitor.Adapter
    {
        private final Barrier.Control barrier = new Barrier.Control();

        @Override
        public void mergedBlocks( long resultingBlockSize, long resultingEntryCount, int numberOfBlocks )
        {
            barrier.reached();
        }
    }
}
