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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
abstract class IndexProviderTests
{
    static final int indexId = 1;
    static final int labelId = 1;
    static final int propId = 1;

    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private final AssertableLogProvider logging;
    private final Monitors monitors;
    private final ProviderFactory factory;
    final TokenNameLookup tokenNameLookup = SchemaTestUtil.SIMPLE_NAME_LOOKUP;
    IndexProvider provider;

    IndexProviderTests( ProviderFactory factory )
    {
        this.factory = factory;
        this.logging = new AssertableLogProvider();
        this.monitors = new Monitors();
        this.monitors.addMonitorListener( new LoggingMonitor( logging.getLog( "test" ) ) );
    }

    @BeforeEach
    void setup() throws IOException
    {
        setupIndexFolders( fs );
    }

    abstract void setupIndexFolders( FileSystemAbstraction fs ) throws IOException;
    abstract IndexDescriptor descriptor();
    abstract IndexDescriptor otherDescriptor();
    abstract IndexPrototype validPrototype();
    abstract List<IndexPrototype> invalidPrototypes();

    /* validatePrototype */

    @Test
    void validatePrototypeMustAcceptValidPrototype()
    {
        // given
        provider = newProvider();

        // when
        IndexPrototype validPrototype = validPrototype();

        // then
        assertDoesNotThrow( () -> provider.validatePrototype( validPrototype ) );
    }

    @Test
    void validatePrototypeMustThrowOnInvalidPrototype()
    {
        // given
        provider = newProvider();

        // when
        List<IndexPrototype> invalidPrototypes = invalidPrototypes();

        // then
        for ( IndexPrototype invalidPrototype : invalidPrototypes )
        {
            assertThrows( IllegalArgumentException.class, () -> provider.validatePrototype( invalidPrototype ) );
        }
    }

    /* getPopulator */

    @Test
    void getPopulatorMustThrowIfInReadOnlyMode()
    {
        // given
        provider = newReadOnlyProvider();

        assertThrows( UnsupportedOperationException.class, () -> provider.getPopulator( descriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup ) );
    }

    /* getPopulationFailure */

    @Test
    void getPopulationFailureReturnEmptyStringIfNoFailure() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( descriptor(), samplingConfig(), heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        populator.create();
        populator.close( true, NULL );

        // when
        // ... no failure on populator

        assertEquals( StringUtils.EMPTY, provider.getPopulationFailure( descriptor(), NULL ) );
    }

    @Test
    void getPopulationFailureReturnEmptyStringIfFailureOnOtherIndex() throws IOException
    {
        // given
        provider = newProvider();

        IndexPopulator nonFailedPopulator = provider.getPopulator( descriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        nonFailedPopulator.create();
        nonFailedPopulator.close( true, NULL );

        IndexPopulator failedPopulator = provider.getPopulator( otherDescriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        failedPopulator.create();

        // when
        failedPopulator.markAsFailed( "failure" );
        failedPopulator.close( false, NULL );

        var populationFailure = provider.getPopulationFailure( descriptor(), NULL );
        assertEquals( StringUtils.EMPTY, populationFailure );
    }

    @Test
    void getPopulationFailureMustReturnReportedFailure() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( descriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        populator.create();

        // when
        String failureMessage = "fail";
        populator.markAsFailed( failureMessage );
        populator.close( false, NULL );

        // then
        String populationFailure = provider.getPopulationFailure( descriptor(), NULL );
        assertThat( populationFailure ).isEqualTo( failureMessage );
    }

    @Test
    void getPopulationFailureMustReturnReportedFailuresForDifferentIndexIds() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator firstPopulator = provider.getPopulator( descriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        firstPopulator.create();
        IndexPopulator secondPopulator = provider.getPopulator( otherDescriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        secondPopulator.create();

        // when
        String firstFailure = "first failure";
        firstPopulator.markAsFailed( firstFailure );
        firstPopulator.close( false, NULL );
        String secondFailure = "second failure";
        secondPopulator.markAsFailed( secondFailure );
        secondPopulator.close( false, NULL );

        // then
        assertThat( provider.getPopulationFailure( descriptor(), NULL ) ).isEqualTo( firstFailure );
        assertThat( provider.getPopulationFailure( otherDescriptor(), NULL ) ).isEqualTo( secondFailure );
    }

    @Test
    void getPopulationFailureMustPersistReportedFailure() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( descriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        populator.create();

        // when
        String failureMessage = "fail";
        populator.markAsFailed( failureMessage );
        populator.close( false, NULL );

        // then
        provider = newProvider();
        String populationFailure = provider.getPopulationFailure( descriptor(), NULL );
        assertThat( populationFailure ).isEqualTo( failureMessage );
    }

    /* getInitialState */
    // pattern: open populator, markAsFailed, close populator, getInitialState, getPopulationFailure

    @Test
    void shouldReportCorrectInitialStateIfIndexDoesntExist()
    {
        // given
        provider = newProvider();

        // when
        InternalIndexState state = provider.getInitialState( descriptor(), NULL );

        // then
        assertEquals( InternalIndexState.POPULATING, state );
        assertThat( logging ).containsMessages( "Failed to open index" );
    }

    @Test
    void shouldReportInitialStateAsPopulatingIfPopulationStartedButIncomplete() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( descriptor(), samplingConfig(),
                heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        populator.create();

        // when
        InternalIndexState state = provider.getInitialState( descriptor(), NULL );

        // then
        assertEquals( InternalIndexState.POPULATING, state );
        populator.close( true, NULL );
    }

    @Test
    void shouldReportInitialStateAsFailedIfMarkedAsFailed() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( descriptor(), samplingConfig(), heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        populator.create();
        populator.markAsFailed( "Just some failure" );
        populator.close( false, NULL );

        // when
        InternalIndexState state = provider.getInitialState( descriptor(), NULL );

        // then
        assertEquals( InternalIndexState.FAILED, state );
    }

    @Test
    void shouldReportInitialStateAsOnlineIfPopulationCompletedSuccessfully() throws IOException
    {
        // given
        provider = newProvider();
        IndexPopulator populator = provider.getPopulator( descriptor(), samplingConfig(), heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        populator.create();
        populator.close( true, NULL );

        // when
        InternalIndexState state = provider.getInitialState( descriptor(), NULL );

        // then
        assertEquals( InternalIndexState.ONLINE, state );
    }

    private IndexProvider newProvider( DatabaseReadOnlyChecker readOnlyChecker )
    {
        return factory.create( pageCache, fs, directoriesByProvider( testDirectory.absolutePath() ), monitors, immediate(), readOnlyChecker, databaseLayout );
    }

    IndexProvider newProvider()
    {
        return newProvider( writable() );
    }

    private IndexProvider newReadOnlyProvider()
    {
        return newProvider( readOnly() );
    }

    static IndexSamplingConfig samplingConfig()
    {
        return new IndexSamplingConfig( Config.defaults() );
    }

    IndexDescriptor completeConfiguration( IndexDescriptor indexDescriptor )
    {
        return provider.completeConfiguration( indexDescriptor );
    }

    @FunctionalInterface
    interface ProviderFactory
    {
        IndexProvider create( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory dir,
                Monitors monitors, RecoveryCleanupWorkCollector collector, DatabaseReadOnlyChecker readOnlyChecker, DatabaseLayout databaseLayout );
    }
}
