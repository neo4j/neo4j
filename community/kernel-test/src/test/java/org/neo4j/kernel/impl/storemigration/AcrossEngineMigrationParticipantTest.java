/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.progress.ProgressListener.NONE;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.PrintStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccessExtended;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
class AcrossEngineMigrationParticipantTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private JobScheduler scheduler;
    private DatabaseLayout fromLayout;
    private DatabaseLayout toLayout;
    private StorageEngineFactory sourceSef;
    private StorageEngineFactory targetSef;
    private ArgumentCaptor<Configuration> importerConfigurationCaptor;
    private ArgumentCaptor<PrintStream> progressOutputCaptor;
    private ArgumentCaptor<Boolean> verboseCaptor;
    private ArgumentCaptor<Monitor> monitorCaptor;

    @BeforeEach
    void start() throws IOException {
        scheduler = new ThreadPoolJobScheduler();
        fromLayout = Neo4jLayout.of(directory.directory("from")).databaseLayout(DEFAULT_DATABASE_NAME);
        toLayout = Neo4jLayout.of(directory.directory("to")).databaseLayout(DEFAULT_DATABASE_NAME);
        sourceSef = mock(StorageEngineFactory.class);
        targetSef = mock(StorageEngineFactory.class);
        var importer = mock(BatchImporter.class);
        importerConfigurationCaptor = ArgumentCaptor.forClass(Configuration.class);
        progressOutputCaptor = ArgumentCaptor.forClass(PrintStream.class);
        verboseCaptor = ArgumentCaptor.forClass(Boolean.TYPE);
        monitorCaptor = ArgumentCaptor.forClass(Monitor.class);
        when(targetSef.batchImporter(
                        any(),
                        any(),
                        anyBoolean(),
                        any(),
                        importerConfigurationCaptor.capture(),
                        any(),
                        progressOutputCaptor.capture(),
                        verboseCaptor.capture(),
                        any(),
                        any(),
                        any(),
                        monitorCaptor.capture(),
                        any(),
                        any(),
                        any(),
                        any(),
                        any(),
                        any(),
                        any(),
                        anyInt(),
                        any(),
                        any(),
                        any()))
                .thenReturn(importer);
        var input = mock(Input.class);
        when(sourceSef.asBatchImporterInput(
                        any(), any(), any(), any(), any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(input);
        var schemaRuleMigrationAccess = mock(SchemaRuleMigrationAccessExtended.class);
        when(targetSef.schemaRuleMigrationAccess(any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(schemaRuleMigrationAccess);
    }

    @AfterEach
    void stop() {
        scheduler.close();
    }

    @Test
    void shouldPassThroughPageCacheToImporterOnNullMaxOffHeapMemory() throws IOException, KernelException {
        // given
        var participant = new AcrossEngineMigrationParticipant(
                fs,
                pageCache,
                NULL,
                defaults(),
                NullLogService.getInstance(),
                scheduler,
                NULL_CONTEXT_FACTORY,
                INSTANCE,
                sourceSef,
                targetSef,
                false,
                false,
                -1,
                null,
                false);

        // when
        participant.migrate(
                fromLayout,
                toLayout,
                NONE,
                mock(StoreVersion.class),
                mock(StoreVersion.class),
                new IndexImporterFactoryImpl(),
                mock(LogTailMetadata.class));

        // then
        assertThat(importerConfigurationCaptor.getValue().providedPageCache()).isNotNull();
    }

    @Test
    void shouldPassThroughMaxOffHeapMemoryOnNonNullMaxOffHeapMemory() throws IOException, KernelException {
        // given
        long maxOffHeapMemory = mebiBytes(80);
        var participant = new AcrossEngineMigrationParticipant(
                fs,
                pageCache,
                NULL,
                defaults(),
                NullLogService.getInstance(),
                scheduler,
                NULL_CONTEXT_FACTORY,
                INSTANCE,
                sourceSef,
                targetSef,
                false,
                false,
                maxOffHeapMemory,
                null,
                false);

        // when
        participant.migrate(
                fromLayout,
                toLayout,
                NONE,
                mock(StoreVersion.class),
                mock(StoreVersion.class),
                new IndexImporterFactoryImpl(),
                mock(LogTailMetadata.class));

        // then
        assertThat(importerConfigurationCaptor.getValue().providedPageCache()).isNull();
        assertThat(importerConfigurationCaptor.getValue().maxOffHeapMemory()).isEqualTo(maxOffHeapMemory);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSelectCorrectOutputsDependingOnVerbose(boolean verbose) throws IOException, KernelException {
        // given
        var participant = new AcrossEngineMigrationParticipant(
                fs,
                pageCache,
                NULL,
                defaults(),
                NullLogService.getInstance(),
                scheduler,
                NULL_CONTEXT_FACTORY,
                INSTANCE,
                sourceSef,
                targetSef,
                false,
                false,
                mebiBytes(80),
                System.out,
                verbose);

        // when
        participant.migrate(
                fromLayout,
                toLayout,
                NONE,
                mock(StoreVersion.class),
                mock(StoreVersion.class),
                new IndexImporterFactoryImpl(),
                mock(LogTailMetadata.class));

        // then
        assertThat(verboseCaptor.getValue()).isEqualTo(verbose);
        if (verbose) {
            assertThat(monitorCaptor.getValue()).isEqualTo(Monitor.NO_MONITOR);
            assertThat(progressOutputCaptor.getValue()).isEqualTo(System.out);
        } else {
            assertThat(monitorCaptor.getValue()).isNotEqualTo(Monitor.NO_MONITOR);
            assertThat(progressOutputCaptor.getValue()).isNotEqualTo(System.out);
        }
    }

    @ParameterizedTest
    @MethodSource
    void shouldHandleVectorData(ShouldHandleVectorData context) {
        // given
        var input = mock(Input.class);
        when(input.containsVectorData()).thenReturn(context.sourceHasVectorData());
        when(sourceSef.asBatchImporterInput(
                        any(), any(), any(), any(), any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(input);
        when(targetSef.supportsVectorData()).thenReturn(context.targetSupportsVectorData());

        var participant = new AcrossEngineMigrationParticipant(
                fs,
                pageCache,
                NULL,
                defaults(),
                NullLogService.getInstance(),
                scheduler,
                NULL_CONTEXT_FACTORY,
                INSTANCE,
                sourceSef,
                targetSef,
                false,
                false,
                mebiBytes(80),
                System.out,
                false);

        // when
        var migrateAssert = assertThatCode(() -> participant.migrate(
                fromLayout,
                toLayout,
                NONE,
                mock(StoreVersion.class),
                mock(StoreVersion.class),
                new IndexImporterFactoryImpl(),
                mock(LogTailMetadata.class)));

        // then
        if (context.shouldMigrate()) {
            migrateAssert.doesNotThrowAnyException();
        } else {
            migrateAssert
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(
                            "Provided input is known to contain vector value data, which is not supported by the target storage engine.");
        }
    }

    static Stream<ShouldHandleVectorData> shouldHandleVectorData() {
        return Stream.of(
                new ShouldHandleVectorData(false, false),
                new ShouldHandleVectorData(false, true),
                new ShouldHandleVectorData(true, false),
                new ShouldHandleVectorData(true, true));
    }

    private record ShouldHandleVectorData(boolean sourceHasVectorData, boolean targetSupportsVectorData) {
        boolean shouldMigrate() {
            return !sourceHasVectorData || targetSupportsVectorData;
        }
    }
}
