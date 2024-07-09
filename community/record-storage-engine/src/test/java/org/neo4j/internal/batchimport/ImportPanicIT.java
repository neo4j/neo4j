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
package org.neo4j.internal.batchimport;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.DataAfterQuoteException;
import org.neo4j.csv.reader.Readables;
import org.neo4j.internal.batchimport.input.InputEntityDecorators;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.csv.CsvInput;
import org.neo4j.internal.batchimport.input.csv.DataFactories;
import org.neo4j.internal.batchimport.input.csv.DataFactory;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
@ExtendWith(RandomExtension.class)
class ImportPanicIT {
    private static final int BUFFER_SIZE = 1000;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    @Inject
    private DatabaseLayout databaseLayout;

    /**
     * There was this problem where some steps and in particular parallel CSV input parsing that
     * paniced would hang the import entirely.
     */
    @Test
    void shouldExitAndThrowExceptionOnPanic() throws Exception {
        try (JobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
            Config config = Config.defaults(GraphDatabaseSettings.db_format, FormatFamily.STANDARD.name());
            BatchImporter importer = new ParallelBatchImporter(
                    databaseLayout,
                    testDirectory.getFileSystem(),
                    PageCacheTracer.NULL,
                    Configuration.DEFAULT,
                    NullLogService.getInstance(),
                    ExecutionMonitor.INVISIBLE,
                    DefaultAdditionalIds.EMPTY,
                    new EmptyLogTailMetadata(config),
                    config,
                    Monitor.NO_MONITOR,
                    jobScheduler,
                    Collector.EMPTY,
                    LogFilesInitializer.NULL,
                    IndexImporterFactory.EMPTY,
                    EmptyMemoryTracker.INSTANCE,
                    new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER));
            Iterable<DataFactory> nodeData = DataFactories.datas(DataFactories.data(
                    InputEntityDecorators.NO_DECORATOR, fileAsCharReadable(nodeCsvFileWithBrokenEntries())));
            Input brokenCsvInput = new CsvInput(
                    nodeData,
                    DataFactories.defaultFormatNodeFileHeader(),
                    DataFactories.datas(),
                    DataFactories.defaultFormatRelationshipFileHeader(),
                    IdType.ACTUAL,
                    csvConfigurationWithLowBufferSize(),
                    false,
                    CsvInput.NO_MONITOR,
                    INSTANCE);
            var e = assertThrows(InputException.class, () -> importer.doImport(brokenCsvInput));
            assertInstanceOf(DataAfterQuoteException.class, e.getCause());
        }
    }

    private static org.neo4j.csv.reader.Configuration csvConfigurationWithLowBufferSize() {
        return COMMAS.toBuilder().withBufferSize(BUFFER_SIZE).build();
    }

    private static Supplier<CharReadable> fileAsCharReadable(Path path) {
        return () -> {
            try {
                return Readables.files(StandardCharsets.UTF_8, path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private Path nodeCsvFileWithBrokenEntries() throws IOException {
        Path file = testDirectory.file("broken-node-data.csv");
        try (Writer writer = new StringWriter()) {
            writer.write(":ID,name" + System.lineSeparator());
            int numberOfLines = BUFFER_SIZE * 10;
            int brokenLine = random.nextInt(numberOfLines);
            for (int i = 0; i < numberOfLines; i++) {
                if (i == brokenLine) {
                    writer.write(i + ",\"broken\"line" + System.lineSeparator());
                } else {
                    writer.write(i + ",name" + i + System.lineSeparator());
                }
            }
            FileSystemUtils.writeString(testDirectory.getFileSystem(), file, writer.toString(), INSTANCE);
        }
        return file;
    }
}
