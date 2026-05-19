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
package org.neo4j.importer;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import blue.strategic.parquet.ParquetWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.input.FileGroup;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.importer.FileImporter.FileInputType;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class FileImporterTest {
    @Inject
    private TestDirectory testDir;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void writesReportToSpecifiedReportFile() throws Exception {
        Path reportLocation = testDir.file("the_report");
        Path inputFile = testDir.file("foobar.csv");
        List<String> lines = Collections.singletonList("foo\\tbar\\tbaz");
        Files.write(inputFile, lines, Charset.defaultCharset());
        Config config = dbConfig();

        var databaseName = "foodb";
        try (var importContext = ImportContext.create(
                testDir.getFileSystem(),
                new NormalizedDatabaseName(databaseName),
                config,
                reportLocation,
                false,
                false,
                true)) {
            var csvImporter = importerBuilder(databaseLayout.getNeo4jLayout().databaseLayout(databaseName))
                    .withDatabaseConfig(config)
                    .withReportFile(reportLocation.toAbsolutePath())
                    .withCsvConfig(Configuration.TABS)
                    .withStdOut(NullPrintStream.INSTANCE)
                    .withStdErr(NullPrintStream.INSTANCE)
                    .withLogProvider(importContext)
                    .addNodeFiles(emptySet(), new FileGroup(new FileGroup.NumberedFile(0, inputFile.toAbsolutePath())))
                    .build();

            csvImporter.doImport(fullImport(), false);

            assertThat(importContext.baseDir()).exists();
            assertThat(importContext.logPath())
                    .exists()
                    .content()
                    .contains("[" + databaseName + "]", "Import starting");
        }

        assertThat(reportLocation).exists();
    }

    @Test
    void complainsOnNonEmptyDirectoryUnlessForced() throws Exception {
        // Given
        Path file = databaseLayout.getTransactionLogsDirectory().resolve(TransactionLogFilesHelper.DEFAULT_NAME + ".0");
        List<String> lines = Collections.singletonList("foo\\tbar\\tbaz");
        Files.write(file, lines, Charset.defaultCharset());
        Path reportLocation = testDir.file("the_report");

        var dbConfig = dbConfig();

        FileImporter.Builder csvImporterBuilder = importerBuilder()
                .withDatabaseConfig(Config.defaults(GraphDatabaseSettings.neo4j_home, testDir.homePath()))
                .withCsvConfig(Configuration.TABS)
                .withDatabaseConfig(dbConfig)
                .withReportFile(reportLocation.toAbsolutePath());
        // Then
        assertThatThrownBy(() -> csvImporterBuilder.build().doImport(fullImport(), false))
                .isInstanceOf(FileImporter.CsvImportException.class)
                .hasCauseInstanceOf(DirectoryNotEmptyException.class)
                .hasMessageContaining("Database already exist. Re-run with `--overwrite-destination`");
        assertThatCode(() -> csvImporterBuilder.withForce(true).build().doImport(fullImport(), false))
                .doesNotThrowAnyException();
    }

    @Test
    void tracePageCacheAccessOnCsvImport() throws IOException {
        Path reportLocation = testDir.file("the_report");
        Path inputFile = writeFileWithLines("foobar.csv", "foo;bar;baz");
        Config config = dbConfig();

        var cacheTracer = new DefaultPageCacheTracer();
        FileImporter fileImporter = importerBuilder()
                .withDatabaseConfig(config)
                .withReportFile(reportLocation.toAbsolutePath())
                .withStdOut(NullPrintStream.INSTANCE)
                .withStdErr(NullPrintStream.INSTANCE)
                .withPageCacheTracer(cacheTracer)
                .withCursorContextFactory(
                        new CursorContextFactory(cacheTracer, new FixedVersionContextSupplier(BASE_TX_ID)))
                .addNodeFiles(emptySet(), new FileGroup(new FileGroup.NumberedFile(0, inputFile.toAbsolutePath())))
                .build();

        fileImporter.doImport(fullImport(), false);

        long pins = cacheTracer.pins();
        assertThat(pins).isPositive();
        assertThat(cacheTracer.unpins()).isEqualTo(pins);
        assertThat(cacheTracer.hits()).isPositive().isLessThanOrEqualTo(pins);
        assertThat(cacheTracer.faults()).isPositive().isLessThanOrEqualTo(pins);
    }

    @Test
    void shouldEnforceBadTolerance() throws IOException {
        // given
        var nodes = writeFileWithLines("nodes.csv", ":ID", "abc", "abc", "abc", "abc", "abc", "abc");
        var importer = importerBuilder()
                .withDatabaseConfig(Config.defaults(GraphDatabaseSettings.neo4j_home, testDir.homePath()))
                .withStdOut(NullPrintStream.INSTANCE)
                .withStdErr(NullPrintStream.INSTANCE)
                .withReportFile(testDir.file("report.txt"))
                .addNodeFiles(emptySet(), new FileGroup(new FileGroup.NumberedFile(0, nodes.toAbsolutePath())))
                .withBadTolerance(4)
                .withSkipDuplicateNodes(true)
                .build();

        // when
        assertThatThrownBy(() -> importer.doImport(fullImport(), false))
                .hasRootCauseInstanceOf(InputException.class)
                .hasMessageContaining("Too many bad entries");
    }

    @ParameterizedTest
    @MethodSource
    void shouldPreventImportIfVectorDataExists(VectorDataContext context) throws IOException {
        var nodeFile = context.writeVectorData(testDir);

        var importerBuilder = importerBuilder()
                .withDatabaseConfig(Config.defaults(GraphDatabaseSettings.neo4j_home, testDir.homePath()))
                .withFileInputType(FileInputType.PARQUET)
                .withStdOut(NullPrintStream.INSTANCE)
                .withStdErr(NullPrintStream.INSTANCE)
                .withReportFile(testDir.file("report.txt"))
                .addNodeFiles(emptySet(), new FileGroup(new FileGroup.NumberedFile(0, nodeFile.toAbsolutePath())))
                .withBadTolerance(4)
                .withSkipDuplicateNodes(true);
        context.configure(importerBuilder);
        var importer = importerBuilder.build();

        var throwableAssert = assertThatThrownBy(() -> importer.doImport(fullImport(), false));
        context.assertException(throwableAssert);
    }

    private Config dbConfig() {
        return Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, testDir.homePath())
                .set(GraphDatabaseSettings.logical_log_rotation_threshold, kibiBytes(256))
                .build();
    }

    static Stream<Named<VectorDataContext>> shouldPreventImportIfVectorDataExists() {
        return Stream.of(Named.of("csv", CSV_VECTOR_DATA_CONTEXT), Named.of("parquet", PARQUET_VECTOR_DATA_CONTEXT));
    }

    private interface VectorDataContext {
        Path writeVectorData(TestDirectory testDirectory) throws IOException;

        void configure(FileImporter.Builder builder);

        void assertException(AbstractThrowableAssert<?, ? extends Throwable> throwableAssert);
    }

    private static final VectorDataContext CSV_VECTOR_DATA_CONTEXT = new VectorDataContext() {
        @Override
        public Path writeVectorData(TestDirectory testDirectory) throws IOException {
            var path = testDirectory.file("embeddings.csv");
            try (var out = new PrintStream(testDirectory.getFileSystem().openAsOutputStream(path, false))) {
                out.println(":ID,\"embedding:vector{coordinateType:float,dimensions:2}\"");
                out.println("1,1;23");
            }
            return path;
        }

        @Override
        public void configure(FileImporter.Builder builder) {
            builder.withFileInputType(FileInputType.CSV);
        }

        @Override
        public void assertException(AbstractThrowableAssert<?, ? extends Throwable> throwableAssert) {
            // CSV can only check after having processed the headers. But, at the same time, we also
            // sample data already, which also provides for an early abort, but with a different exception.
            // Here, we get the more concrete message that aligned does not support vectors.
            throwableAssert
                    .isInstanceOf(FileImporter.CsvImportException.class)
                    .hasMessageContaining("storing properties of type vector is not supported in aligned store format");
        }
    };

    private static final FileImporterTest.VectorDataContext PARQUET_VECTOR_DATA_CONTEXT = new VectorDataContext() {
        @Override
        public Path writeVectorData(TestDirectory testDirectory) throws IOException {
            var path = testDirectory.file("embeddings.parquet");
            var fields = List.<Type>of(
                    Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                    Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType())
                            .named("embedding:vector{coordinateType:float,dimensions:2}"));
            var data = Collections.singletonList(new Object[] {1, "1;23"});

            try (var writer = ParquetWriter.writeFile(
                    new MessageType("something", fields), path.toFile(), (record, valueWriter) -> {
                        var recordData = (Object[]) record;
                        for (int i = 0; i < fields.size(); i++) {
                            Type type = fields.get(i);
                            Object value = recordData[i];
                            if (value != null) {
                                valueWriter.write(type.getName(), value);
                            }
                        }
                    })) {
                for (Object[] datum : data) {
                    writer.write(datum);
                }
            }
            return path;
        }

        @Override
        public void configure(FileImporter.Builder builder) {
            builder.withFileInputType(FileInputType.PARQUET);
        }

        @Override
        public void assertException(AbstractThrowableAssert<?, ? extends Throwable> throwableAssert) {
            // Parquet can check early, just be looking at the headers. It will not process any data and get the
            // more generic exception from FileImporter.
            throwableAssert
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("Provided input is known to contain vector value data, which is not supported by the"
                            + " target storage engine.");
        }
    };

    private Path writeFileWithLines(String fileName, String... lines) throws IOException {
        var path = testDir.file(fileName);
        Files.write(path, List.of(lines), Charset.defaultCharset());
        return path;
    }

    private ImportCommand.Full fullImport() {
        return new ImportCommand.Full(new ExecutionContext(
                testDir.homePath(),
                testDir.homePath().resolve("conf"),
                NullPrintStream.INSTANCE,
                NullPrintStream.INSTANCE,
                testDir.getFileSystem()));
    }

    private FileImporter.Builder importerBuilder() {
        return importerBuilder(databaseLayout);
    }

    private FileImporter.Builder importerBuilder(DatabaseLayout layout) {
        return FileImporter.builder()
                .withStorageEngineFactory(StorageEngineFactory.defaultStorageEngine())
                .withDatabaseLayout(layout)
                .withFileSystem(testDir.getFileSystem());
    }
}
