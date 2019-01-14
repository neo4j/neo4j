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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.values.storable.Value;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

public abstract class NativeIndexProviderTest
{
    @Rule
    public PageCacheAndDependenciesRule rules = new PageCacheAndDependenciesRule();

    private static final int indexId = 1;
    private static final int labelId = 1;
    private static final int propId = 1;
    private IndexProvider provider;
    private final AssertableLogProvider logging = new AssertableLogProvider();
    private final IndexProvider.Monitor monitor = new LoggingMonitor( logging.getLog( "test" ) );

    @Before
    public void setup() throws IOException
    {
        File nativeSchemaIndexStoreDirectory = newProvider().directoryStructure().rootDirectory();
        rules.fileSystem().mkdirs( nativeSchemaIndexStoreDirectory );
    }

    /* getPopulator */

    @Test
    public void getPopulatorMustThrowIfInReadOnlyMode()
    {
        // given
        provider = newReadOnlyProvider();

        try
        {
            // when
            provider.getPopulator( indexId, descriptor(), samplingConfig() );
            fail( "Should have failed" );
        }
        catch ( UnsupportedOperationException e )
        {
            // then
            // good
        }
    }

    /* getOnlineAccessor */

    @Test
    public void shouldNotCheckConflictsWhenApplyingUpdatesInOnlineAccessor() throws IOException, IndexEntryConflictException
    {
        // given
        provider = newProvider();

        // when
        SchemaIndexDescriptor descriptor = descriptorUnique();
        try ( IndexAccessor accessor = provider.getOnlineAccessor( indexId, descriptor, samplingConfig() );
              IndexUpdater indexUpdater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            Value value = someValue();
            indexUpdater.process( IndexEntryUpdate.add( 1, descriptor.schema(), value ) );

            // then
            // ... expect no failure on duplicate value
            indexUpdater.process( IndexEntryUpdate.add( 2, descriptor.schema(), value ) );
        }
    }

    /* getPopulationFailure */

    @Test
    public void getPopulationFailureMustThrowIfNoFailure() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( indexId, descriptor(), samplingConfig() );
        populator.create();
        populator.close( true );

        // when
        // ... no failure on populator

        // then
        try
        {
            provider.getPopulationFailure( indexId, descriptor() );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage(), Matchers.containsString( Long.toString( indexId ) ) );
        }
    }

    @Test
    public void getPopulationFailureMustThrowEvenIfFailureOnOtherIndex() throws IOException
    {
        // given
        provider = newProvider();

        int nonFailedIndexId = NativeIndexProviderTest.indexId;
        IndexPopulator nonFailedPopulator = provider.getPopulator( nonFailedIndexId, descriptor(), samplingConfig() );
        nonFailedPopulator.create();
        nonFailedPopulator.close( true );

        int failedIndexId = 2;
        IndexPopulator failedPopulator = provider.getPopulator( failedIndexId, descriptor(), samplingConfig() );
        failedPopulator.create();

        // when
        failedPopulator.markAsFailed( "failure" );
        failedPopulator.close( false );

        // then
        try
        {
            provider.getPopulationFailure( nonFailedIndexId, descriptor() );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage(), Matchers.containsString( Long.toString( nonFailedIndexId ) ) );
        }
    }

    @Test
    public void getPopulationFailureMustReturnReportedFailure() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( indexId, descriptor(), samplingConfig() );
        populator.create();

        // when
        String failureMessage = "fail";
        populator.markAsFailed( failureMessage );
        populator.close( false );

        // then
        String populationFailure = provider.getPopulationFailure( indexId, descriptor() );
        assertThat( populationFailure, is( failureMessage ) );
    }

    @Test
    public void getPopulationFailureMustReturnReportedFailuresForDifferentIndexIds() throws IOException
    {
        // given
        provider = newProvider();
        int first = 1;
        int second = 2;
        int third = 3;
        IndexPopulator firstPopulator = provider.getPopulator( first, descriptor(), samplingConfig() );
        firstPopulator.create();
        IndexPopulator secondPopulator = provider.getPopulator( second, descriptor(), samplingConfig() );
        secondPopulator.create();
        IndexPopulator thirdPopulator = provider.getPopulator( third, descriptor(), samplingConfig() );
        thirdPopulator.create();

        // when
        String firstFailure = "first failure";
        firstPopulator.markAsFailed( firstFailure );
        firstPopulator.close( false );
        secondPopulator.close( true );
        String thirdFailure = "third failure";
        thirdPopulator.markAsFailed( thirdFailure );
        thirdPopulator.close( false );

        // then
        assertThat( provider.getPopulationFailure( first, descriptor() ), is( firstFailure ) );
        assertThat( provider.getPopulationFailure( third, descriptor() ), is( thirdFailure ) );
        try
        {
            provider.getPopulationFailure( second, descriptor() );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
    }

    @Test
    public void getPopulationFailureMustPersistReportedFailure() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( indexId, descriptor(), samplingConfig() );
        populator.create();

        // when
        String failureMessage = "fail";
        populator.markAsFailed( failureMessage );
        populator.close( false );

        // then
        provider = newProvider();
        String populationFailure = provider.getPopulationFailure( indexId, descriptor() );
        assertThat( populationFailure, is( failureMessage ) );
    }

    /* getInitialState */
    // pattern: open populator, markAsFailed, close populator, getInitialState, getPopulationFailure

    @Test
    public void shouldReportCorrectInitialStateIfIndexDoesntExist()
    {
        // given
        provider = newProvider();

        // when
        InternalIndexState state = provider.getInitialState( indexId, descriptor() );

        // then
        InternalIndexState expected = expectedStateOnNonExistingSubIndex();
        assertEquals( expected, state );
        if ( InternalIndexState.POPULATING == expected )
        {
            logging.assertContainsLogCallContaining( "Failed to open index" );
        }
        else
        {
            logging.assertNoLogCallContaining( "Failed to open index" );
        }
    }

    @Test
    public void shouldReportInitialStateAsPopulatingIfPopulationStartedButIncomplete() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( indexId, descriptor(), samplingConfig() );
        populator.create();

        // when
        InternalIndexState state = provider.getInitialState( indexId, descriptor() );

        // then
        assertEquals( InternalIndexState.POPULATING, state );
        populator.close( true );
    }

    @Test
    public void shouldReportInitialStateAsFailedIfMarkedAsFailed() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( indexId, descriptor(), samplingConfig() );
        populator.create();
        populator.markAsFailed( "Just some failure" );
        populator.close( false );

        // when
        InternalIndexState state = provider.getInitialState( indexId, descriptor() );

        // then
        assertEquals( InternalIndexState.FAILED, state );
    }

    @Test
    public void shouldReportInitialStateAsOnlineIfPopulationCompletedSuccessfully() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( indexId, descriptor(), samplingConfig() );
        populator.create();
        populator.close( true );

        // when
        InternalIndexState state = provider.getInitialState( indexId, descriptor() );

        // then
        assertEquals( InternalIndexState.ONLINE, state );
    }

    /* storeMigrationParticipant */

    protected abstract InternalIndexState expectedStateOnNonExistingSubIndex();

    protected abstract Value someValue();

    abstract IndexProvider newProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory dir,
                                        IndexProvider.Monitor monitor, RecoveryCleanupWorkCollector collector );

    abstract IndexProvider newReadOnlyProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory dir,
                                                IndexProvider.Monitor monitor, RecoveryCleanupWorkCollector collector );

    private IndexProvider newProvider()
    {
        return newProvider( pageCache(), fs(), directoriesByProvider( baseDir() ), monitor, immediate() );
    }

    private IndexProvider newReadOnlyProvider()
    {
        return newReadOnlyProvider( pageCache(), fs(), directoriesByProvider( baseDir() ), monitor, immediate() );
    }

    private IndexSamplingConfig samplingConfig()
    {
        return new IndexSamplingConfig( Config.defaults() );
    }

    private SchemaIndexDescriptor descriptor()
    {
        return SchemaIndexDescriptorFactory.forLabel( labelId, propId );
    }

    private SchemaIndexDescriptor descriptorUnique()
    {
        return SchemaIndexDescriptorFactory.uniqueForLabel( labelId, propId );
    }

    private PageCache pageCache()
    {
        return rules.pageCache();
    }

    private FileSystemAbstraction fs()
    {
        return rules.fileSystem();
    }

    private File baseDir()
    {
        return rules.directory().absolutePath();
    }
}
