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
package org.neo4j.kernel.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsFile;
import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsString;
import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newFailedDiagnosticsSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DiagnosticsReporterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Test
    void dumpFiles() throws Exception {
        DiagnosticsReporter reporter = setupDiagnosticsReporter();

        Path destination = testDirectory.file("logs.zip");

        reporter.dump(Collections.singleton("logs"), destination, mock(DiagnosticsReporterProgress.class), true);

        // Verify content
        verifyContent(destination);
    }

    @Test
    void shouldContinueAfterError() throws Exception {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        MyProvider myProvider = new MyProvider(fileSystem);
        reporter.registerOfflineProvider(myProvider);

        myProvider.addFile("logs/a.txt", createNewFileWithContent("a.txt", "file a"));

        Path destination = testDirectory.file("logs.zip");
        Set<String> classifiers = new HashSet<>();
        classifiers.add("logs");
        classifiers.add("fail");
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            PrintStream out = new PrintStream(baos);
            NonInteractiveProgress progress = new NonInteractiveProgress(out, false);

            reporter.dump(classifiers, destination, progress, true);

            assertThat(baos)
                    .hasToString(String.format("1/2 fail.txt%n" + "%n"
                            + "Error: Failed to write fail.txt%n"
                            + "2/2 logs/a.txt%n"
                            + "....................  20%%%n"
                            + "....................  40%%%n"
                            + "....................  60%%%n"
                            + "....................  80%%%n"
                            + ".................... 100%%%n%n"));
        }

        // Verify content
        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath());

        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            List<String> fileA = Files.readAllLines(fs.getPath("logs/a.txt"));
            assertEquals(1, fileA.size());
            assertEquals("file a", fileA.get(0));
        }
    }

    @Test
    void supportPathsWithSpaces() throws IOException {
        DiagnosticsReporter reporter = setupDiagnosticsReporter();

        Path destination = testDirectory.file("log files.zip");

        reporter.dump(Collections.singleton("logs"), destination, mock(DiagnosticsReporterProgress.class), true);

        verifyContent(destination);
    }

    @Test
    void collectAndDumpAuthenticatedSources() throws IOException {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        MyAuthenticatedProvider provider = new MyAuthenticatedProvider();
        reporter.registerAuthenticatedProvider(provider);

        assertThat(reporter.getAuthenticatedClassifiers()).contains("authenticated");
        assertThat(reporter.getAvailableClassifiers()).contains("authenticated");
        assertThat(reporter.describeAuthenticatedClassifier("authenticated"))
                .isEqualTo("the authenticated description");

        RecordingConnection connection = new RecordingConnection();
        reporter.collectAuthenticatedSources(Collections.singleton("authenticated"), connection);
        assertThat(connection.queries).containsExactly("CALL example()");

        Path destination = testDirectory.file("authenticated.zip");
        reporter.dump(
                Collections.singleton("authenticated"), destination, mock(DiagnosticsReporterProgress.class), true);

        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath());
        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            List<String> lines = Files.readAllLines(fs.getPath("authenticated/result.txt"));
            assertEquals(1, lines.size());
            assertEquals("hello", lines.get(0));
        }
    }

    @Test
    void writesManifestReflectingSuccessAndFailure() throws Exception {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        MyProvider myProvider = new MyProvider(fileSystem);
        reporter.registerOfflineProvider(myProvider);
        myProvider.addFile("logs/a.txt", createNewFileWithContent("a.txt", "file a"));

        Path destination = testDirectory.file("report.zip");
        Set<String> classifiers = new HashSet<>();
        classifiers.add("logs");
        classifiers.add("fail");
        DiagnosticsReportInfo info =
                new DiagnosticsReportInfo("my-host", OffsetDateTime.of(2026, 7, 23, 14, 15, 30, 0, ZoneOffset.UTC));
        reporter.dump(classifiers, destination, mock(DiagnosticsReporterProgress.class), true, info);

        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath());
        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            Path manifest = fs.getPath(DiagnosticsReportManifest.FILE_NAME);
            assertThat(manifest).exists();
            JsonNode root = MAPPER.readTree(Files.readString(manifest));

            assertThat(root.get("schemaVersion").asText()).isEqualTo("1.0");
            assertThat(root.get("neo4jVersion").isTextual()).isTrue();
            // host and time passed in, matching what the file name would be built from
            assertThat(root.get("hostname").asText()).isEqualTo("my-host");
            assertThat(root.get("timestamp").asText()).isEqualTo("2026-07-23T14:15:30Z");

            JsonNode classifierNodes = root.get("classifiers");
            // the failing classifier only had a failing source
            JsonNode fail = classifier(classifierNodes, "fail");
            assertThat(fail.get("status").asText()).isEqualTo("FAILED");
            assertThat(fail.get("sources").get(0).get("path").asText()).isEqualTo("fail.txt");
            // the logs classifier had a single source that was written successfully
            JsonNode logs = classifier(classifierNodes, "logs");
            assertThat(logs.get("status").asText()).isEqualTo("SUCCESS");
            assertThat(logs.get("sources").get(0).get("path").asText()).isEqualTo("logs/a.txt");
        }
    }

    @Test
    void manifestReflectsAggregateStatusFromCollectionFailures() throws Exception {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        // A string source whose supplier throws fails while being written into the archive - the same path a real
        // collection failure takes - so it is recorded as FAILED in the manifest.
        Supplier<String> boom = () -> {
            throw new RuntimeException("boom");
        };
        reporter.registerSource("ok", newDiagnosticsString("ok/one.txt", () -> "1")); // all succeed -> SUCCESS
        reporter.registerSource("partial", newDiagnosticsString("partial/good.txt", () -> "ok")); // mixed -> PARTIAL
        reporter.registerSource("partial", newDiagnosticsString("partial/bad.txt", boom));
        reporter.registerSource("broken", newDiagnosticsString("broken/a.txt", boom)); // all fail -> FAILED
        reporter.registerSource("broken", newDiagnosticsString("broken/b.txt", boom));

        Path destination = testDirectory.file("report.zip");
        reporter.dump(Set.of("ok", "partial", "broken"), destination, mock(DiagnosticsReporterProgress.class), true);

        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath());
        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            JsonNode classifiers = MAPPER.readTree(Files.readString(fs.getPath(DiagnosticsReportManifest.FILE_NAME)))
                    .get("classifiers");

            // Each classifier's aggregate status reflects the outcomes of its sources.
            assertThat(classifier(classifiers, "ok").get("status").asText()).isEqualTo("SUCCESS");
            assertThat(classifier(classifiers, "partial").get("status").asText())
                    .isEqualTo("PARTIAL");
            assertThat(classifier(classifiers, "broken").get("status").asText()).isEqualTo("FAILED");

            // Per-source outcomes, including the captured error message for a failure.
            JsonNode partialSources = classifier(classifiers, "partial").get("sources");
            assertThat(partialSources.get(0).get("path").asText()).isEqualTo("partial/good.txt");
            assertThat(partialSources.get(0).get("status").asText()).isEqualTo("SUCCESS");
            assertThat(partialSources.get(1).get("path").asText()).isEqualTo("partial/bad.txt");
            assertThat(partialSources.get(1).get("status").asText()).isEqualTo("FAILED");
            assertThat(partialSources.get(1).get("error").asText()).isEqualTo("boom");
        }
    }

    @Test
    void eagerlyFailedSourceIsRecordedInTheManifestAndOmittedFromTheArchive() throws Exception {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        // Sources whose collection already failed before the archive was opened - the shape produced by an
        // authenticated provider whose query failed while the connection was still open.
        String error = "Failed to run 'SHOW DATABASES YIELD *': connection refused";
        reporter.registerSource("mixed", newDiagnosticsString("mixed/good.json", () -> "[]"));
        reporter.registerSource("mixed", newFailedDiagnosticsSource("mixed/bad.json", error));
        reporter.registerSource("eager", newFailedDiagnosticsSource("eager/only.json", error));

        Path destination = testDirectory.file("report.zip");
        reporter.dump(Set.of("mixed", "eager"), destination, mock(DiagnosticsReporterProgress.class), true);

        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath());
        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            JsonNode classifiers = MAPPER.readTree(Files.readString(fs.getPath(DiagnosticsReportManifest.FILE_NAME)))
                    .get("classifiers");

            // The failure is attributed to the classifier and carries the message captured at collection time.
            JsonNode eager = classifier(classifiers, "eager");
            assertThat(eager.get("status").asText()).isEqualTo("FAILED");
            assertThat(eager.get("sources").get(0).get("path").asText()).isEqualTo("eager/only.json");
            assertThat(eager.get("sources").get(0).get("status").asText()).isEqualTo("FAILED");
            assertThat(eager.get("sources").get(0).get("error").asText()).isEqualTo(error);

            // A classifier that collected some of its sources is only partially failed.
            JsonNode mixed = classifier(classifiers, "mixed");
            assertThat(mixed.get("status").asText()).isEqualTo("PARTIAL");

            // Nothing is written for a failed source, so no empty entry is left behind in the archive.
            assertThat(Files.exists(fs.getPath("eager/only.json"))).isFalse();
            assertThat(Files.exists(fs.getPath("mixed/bad.json"))).isFalse();
            assertThat(Files.readString(fs.getPath("mixed/good.json"))).isEqualTo("[]");
        }
    }

    @Test
    void reportsProgressErrorForAnEagerlyFailedSource() throws Exception {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        reporter.registerSource("eager", newFailedDiagnosticsSource("eager/only.json", "connection refused"));

        Path destination = testDirectory.file("report.zip");
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            NonInteractiveProgress progress = new NonInteractiveProgress(new PrintStream(baos), false);

            reporter.dump(Collections.singleton("eager"), destination, progress, true);

            assertThat(baos.toString()).contains("Error: Failed to write eager/only.json");
        }
    }

    @Test
    void groupsSourcesOfTheSameClassifierFromDifferentProviders() throws Exception {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        MyProvider first = new MyProvider(fileSystem);
        first.addFile("logs/a.txt", createNewFileWithContent("a.txt", "file a"));
        MyProvider second = new MyProvider(fileSystem);
        second.addFile("logs/b.txt", createNewFileWithContent("b.txt", "file b"));
        reporter.registerOfflineProvider(first);
        reporter.registerOfflineProvider(second);
        reporter.registerSource("logs", newDiagnosticsString("logs/c.txt", () -> "c"));

        Path destination = testDirectory.file("report.zip");
        reporter.dump(Collections.singleton("logs"), destination, mock(DiagnosticsReporterProgress.class), true);

        // All three sources belong to 'logs', so the manifest lists that classifier once with all of them under it.
        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath());
        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            JsonNode classifiers = MAPPER.readTree(Files.readString(fs.getPath(DiagnosticsReportManifest.FILE_NAME)))
                    .get("classifiers");
            assertThat(classifiers).hasSize(1);
            assertThat(classifier(classifiers, "logs").get("sources")).hasSize(3);
        }
    }

    private static JsonNode classifier(JsonNode classifiers, String name) {
        for (JsonNode classifier : classifiers) {
            if (name.equals(classifier.get("name").asText())) {
                return classifier;
            }
        }
        throw new AssertionError("No classifier named '" + name + "' in " + classifiers);
    }

    private Path createNewFileWithContent(String name, String content) throws IOException {
        Path file = testDirectory.file(name);
        Files.write(file, content.getBytes());
        return file;
    }

    private DiagnosticsReporter setupDiagnosticsReporter() throws IOException {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        MyProvider myProvider = new MyProvider(fileSystem);
        reporter.registerOfflineProvider(myProvider);

        myProvider.addFile("logs/a.txt", createNewFileWithContent("a.txt", "file a"));
        myProvider.addFile("logs/b.txt", createNewFileWithContent("b.txt", "file b"));
        return reporter;
    }

    private static void verifyContent(Path destination) throws IOException {
        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath());

        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            List<String> fileA = Files.readAllLines(fs.getPath("logs/a.txt"));
            assertEquals(1, fileA.size());
            assertEquals("file a", fileA.get(0));

            List<String> fileB = Files.readAllLines(fs.getPath("logs/b.txt"));
            assertEquals(1, fileB.size());
            assertEquals("file b", fileB.get(0));
        }
    }

    private static class MyProvider extends DiagnosticsOfflineReportProvider {
        private final FileSystemAbstraction fs;
        private final List<DiagnosticsReportSource> logFiles = new ArrayList<>();

        MyProvider(FileSystemAbstraction fs) {
            super("my-provider", "logs");
            this.fs = fs;
        }

        void addFile(String destination, Path file) {
            logFiles.add(newDiagnosticsFile(destination, fs, file));
        }

        @Override
        public void init(FileSystemAbstraction fs, Config config, Set<String> databaseNames) {}

        @Override
        public List<DiagnosticsReportSource> provideSources(Set<String> classifiers) {
            List<DiagnosticsReportSource> sources = new ArrayList<>();
            if (classifiers.contains("fail")) {
                sources.add(new FailingSource());
            }
            if (classifiers.contains("logs")) {
                sources.addAll(logFiles);
            }

            return sources;
        }
    }

    private static class MyAuthenticatedProvider extends DiagnosticsAuthenticatedReportProvider {
        MyAuthenticatedProvider() {
            super("authenticated");
        }

        @Override
        public void init(FileSystemAbstraction fs, Config config, Set<String> databaseNames) {}

        @Override
        protected String describe(String classifier) {
            return "the authenticated description";
        }

        @Override
        protected Map<String, List<DiagnosticsReportSource>> provideSources(
                Set<String> classifiers, DiagnosticsLiveConnection connection) {
            if (!classifiers.contains("authenticated")) {
                return Map.of();
            }
            DiagnosticsQueryResult result = connection.execute(null, "CALL example()");
            String content =
                    String.valueOf(result.rows().get(0).get(result.columns().get(0)));
            return Map.of("authenticated", List.of(newDiagnosticsString("authenticated/result.txt", () -> content)));
        }
    }

    private static class RecordingConnection implements DiagnosticsLiveConnection {
        private final List<String> queries = new ArrayList<>();

        @Override
        public DiagnosticsQueryResult execute(String database, String query) {
            queries.add(query);
            return new DiagnosticsQueryResult(List.of("greeting"), List.of(Map.of("greeting", "hello")));
        }

        @Override
        public void close() {}
    }

    private static class FailingSource implements DiagnosticsReportSource {

        @Override
        public String destinationPath() {
            return "fail.txt";
        }

        @Override
        public InputStream newInputStream() {
            throw new RuntimeException("You had it coming...");
        }

        @Override
        public long estimatedSize() {
            return 0;
        }
    }
}
