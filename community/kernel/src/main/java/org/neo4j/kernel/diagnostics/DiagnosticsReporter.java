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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsReportManifest.ClassifierResult;
import org.neo4j.kernel.diagnostics.DiagnosticsReportManifest.SourceResult;
import org.neo4j.service.Services;

public class DiagnosticsReporter {
    private record ClassifierSource(String classifier, DiagnosticsReportSource source) {}

    private final List<DiagnosticsOfflineReportProvider> providers = new ArrayList<>();
    private final List<DiagnosticsAuthenticatedReportProvider> authenticatedProviders = new ArrayList<>();
    private final Set<String> availableClassifiers = new TreeSet<>();
    private final Map<String, List<DiagnosticsReportSource>> additionalSources = new HashMap<>();

    public void registerOfflineProvider(DiagnosticsOfflineReportProvider provider) {
        providers.add(provider);
        availableClassifiers.addAll(provider.getFilterClassifiers());
    }

    public void registerAuthenticatedProvider(DiagnosticsAuthenticatedReportProvider provider) {
        authenticatedProviders.add(provider);
        availableClassifiers.addAll(provider.getFilterClassifiers());
    }

    public void registerSource(String classifier, DiagnosticsReportSource source) {
        availableClassifiers.add(classifier);
        additionalSources.computeIfAbsent(classifier, c -> new ArrayList<>()).add(source);
    }

    public void dump(
            Set<String> classifiers,
            Path destination,
            DiagnosticsReporterProgress progress,
            boolean ignoreDiskSpaceCheck)
            throws IOException {
        dump(classifiers, destination, progress, ignoreDiskSpaceCheck, DiagnosticsReportInfo.local());
    }

    public void dump(
            Set<String> classifiers,
            Path destination,
            DiagnosticsReporterProgress progress,
            boolean ignoreDiskSpaceCheck,
            DiagnosticsReportInfo info)
            throws IOException {
        final List<ClassifierSource> sources = getAllSources(classifiers);
        final Path destinationDir = createDirectories(destination.getParent());

        if (!ignoreDiskSpaceCheck) {
            estimateSizeAndCheckAvailableDiskSpace(destination, sources, destinationDir);
        }

        if (progress == null) {
            progress = DiagnosticsReporterProgress.EMPTY;
        }

        progress.setTotalSteps(sources.size());
        try (ZipOutputStream zip =
                new ZipOutputStream(new BufferedOutputStream(newOutputStream(destination, CREATE_NEW, WRITE)), UTF_8)) {
            List<ClassifierResult> results = writeDiagnostics(zip, sources, progress);
            writeManifest(zip, new DiagnosticsReportManifest(info.hostName(), info.timestamp(), results));
        }
    }

    private static List<ClassifierResult> writeDiagnostics(
            ZipOutputStream zip, List<ClassifierSource> sources, DiagnosticsReporterProgress progress) {
        final Map<String, ClassifierResult> resultsByClassifier = new LinkedHashMap<>();
        int step = 0;
        final byte[] buf = new byte[(int) kibiBytes(8)]; // same as default buf size in buffered streams
        for (ClassifierSource classifierSource : sources) {
            final DiagnosticsReportSource source = classifierSource.source();
            final List<SourceResult> sourceResults = resultsByClassifier
                    .computeIfAbsent(classifierSource.classifier(), c -> new ClassifierResult(c, new ArrayList<>()))
                    .sources();
            ++step;
            progress.started(step, source.destinationPath());
            try (InputStream rawInput = source.newInputStream();
                    InputStream input = new ProgressAwareInputStream(
                            new BufferedInputStream(rawInput),
                            source.estimatedSize(),
                            progress == DiagnosticsReporterProgress.EMPTY ? null : progress::percentChanged)) {
                final ZipEntry entry = new ZipEntry(source.destinationPath());
                zip.putNextEntry(entry);

                int chunkSize;
                while ((chunkSize = input.read(buf)) >= 0) {
                    zip.write(buf, 0, chunkSize);
                }

                zip.closeEntry();
            } catch (Exception e) {
                progress.error("Failed to write " + source.destinationPath(), e);
                sourceResults.add(
                        new SourceResult(source.destinationPath(), CollectionStatus.FAILED, describeError(e)));
                continue;
            }
            progress.finished();
            sourceResults.add(new SourceResult(source.destinationPath(), CollectionStatus.SUCCESS, null));
        }
        return List.copyOf(resultsByClassifier.values());
    }

    private static void writeManifest(ZipOutputStream zip, DiagnosticsReportManifest manifest) throws IOException {
        final ZipEntry entry = new ZipEntry(DiagnosticsReportManifest.FILE_NAME);
        zip.putNextEntry(entry);
        zip.write(manifest.toJson().getBytes(UTF_8));
        zip.closeEntry();
    }

    private static String describeError(Exception e) {
        return e.getMessage() != null ? e.getMessage() : e.getClass().getName();
    }

    private List<ClassifierSource> getAllSources(Set<String> classifiers) {
        final boolean all = classifiers.contains("all");

        // Providers are queried one classifier at a time so that every source can be attributed to one in the
        // manifest; a provider only produces sources for the classifiers it is asked for, so the union is the same
        // as querying it with the full set at once.
        final List<ClassifierSource> allSources = new ArrayList<>();
        providers.forEach(provider -> {
            for (String classifier : all ? provider.getFilterClassifiers() : classifiers) {
                provider.getDiagnosticsSources(Set.of(classifier))
                        .forEach(source -> allSources.add(new ClassifierSource(classifier, source)));
            }
        });
        additionalSources.forEach((classifier, sources) -> {
            if (all || classifiers.contains(classifier)) {
                sources.forEach(source -> allSources.add(new ClassifierSource(classifier, source)));
            }
        });
        return allSources;
    }

    private static void estimateSizeAndCheckAvailableDiskSpace(
            Path destination, List<ClassifierSource> sources, Path destinationDir) {
        final long estimatedFinalSize = sources.stream()
                .mapToLong(classifierSource -> classifierSource.source().estimatedSize())
                .sum();
        final long freeSpace = destinationDir.toFile().getFreeSpace();
        if (estimatedFinalSize > freeSpace) {
            throw new RuntimeException(format(
                    "Free available disk space for %s is %s, worst case estimate is %s. To ignore add '--force' to the command.",
                    destination.getFileName(), bytesToString(freeSpace), bytesToString(estimatedFinalSize)));
        }
    }

    public Set<String> getAvailableClassifiers() {
        return availableClassifiers;
    }

    public void registerAllOfflineProviders(Config config, FileSystemAbstraction fs, Set<String> databaseNames) {
        for (DiagnosticsOfflineReportProvider provider : Services.loadAll(DiagnosticsOfflineReportProvider.class)) {
            provider.init(fs, config, databaseNames);
            registerOfflineProvider(provider);
        }
    }

    public void registerAllAuthenticatedProviders(Config config, FileSystemAbstraction fs, Set<String> databaseNames) {
        for (DiagnosticsAuthenticatedReportProvider provider :
                Services.loadAll(DiagnosticsAuthenticatedReportProvider.class)) {
            provider.init(fs, config, databaseNames);
            registerAuthenticatedProvider(provider);
        }
    }

    /**
     * @return the set of classifiers that require a live connection to the running DBMS.
     */
    public Set<String> getAuthenticatedClassifiers() {
        final Set<String> classifiers = new TreeSet<>();
        authenticatedProviders.forEach(provider -> classifiers.addAll(provider.getFilterClassifiers()));
        return classifiers;
    }

    /**
     * Runs all authenticated providers against the given live connection and registers the sources they produce. Providers
     * gather their data eagerly within this call, so the connection may be closed once it returns.
     */
    public void collectAuthenticatedSources(Set<String> classifiers, DiagnosticsLiveConnection connection) {
        for (DiagnosticsAuthenticatedReportProvider provider : authenticatedProviders) {
            provider.getDiagnosticsSources(classifiers, connection)
                    .forEach((classifier, sources) -> sources.forEach(source -> registerSource(classifier, source)));
        }
    }

    /**
     * @return a description of an authenticated classifier, or {@code null} if it is not provided by any authenticated provider.
     */
    public String describeAuthenticatedClassifier(String classifier) {
        for (DiagnosticsAuthenticatedReportProvider provider : authenticatedProviders) {
            if (provider.getFilterClassifiers().contains(classifier)) {
                return provider.describeClassifier(classifier);
            }
        }
        return null;
    }
}
