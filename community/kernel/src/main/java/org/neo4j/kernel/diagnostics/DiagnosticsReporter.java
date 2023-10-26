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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.service.Services;

public class DiagnosticsReporter {
    private final List<DiagnosticsOfflineReportProvider> providers = new ArrayList<>();
    private final Set<String> availableClassifiers = new TreeSet<>();
    private final Map<String, List<DiagnosticsReportSource>> additionalSources = new HashMap<>();

    public void registerOfflineProvider(DiagnosticsOfflineReportProvider provider) {
        providers.add(provider);
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
        final List<DiagnosticsReportSource> sources = getAllSources(classifiers);
        final Path destinationDir = createDirectories(destination.getParent());

        if (!ignoreDiskSpaceCheck) {
            estimateSizeAndCheckAvailableDiskSpace(destination, sources, destinationDir);
        }

        progress.setTotalSteps(sources.size());
        try (ZipOutputStream zip =
                new ZipOutputStream(new BufferedOutputStream(newOutputStream(destination, CREATE_NEW, WRITE)), UTF_8)) {
            writeDiagnostics(zip, sources, progress);
        }
    }

    private static void writeDiagnostics(
            ZipOutputStream zip, List<DiagnosticsReportSource> sources, DiagnosticsReporterProgress progress) {
        int step = 0;
        final byte[] buf = new byte[(int) kibiBytes(8)]; // same as default buf size in buffered streams
        for (DiagnosticsReportSource source : sources) {
            ++step;
            progress.started(step, source.destinationPath());
            try (InputStream rawInput = source.newInputStream();
                    InputStream input = new ProgressAwareInputStream(
                            new BufferedInputStream(rawInput), source.estimatedSize(), progress::percentChanged)) {
                final ZipEntry entry = new ZipEntry(source.destinationPath());
                zip.putNextEntry(entry);

                int chunkSize;
                while ((chunkSize = input.read(buf)) >= 0) {
                    zip.write(buf, 0, chunkSize);
                }

                zip.closeEntry();
            } catch (Exception e) {
                progress.error("Failed to write " + source.destinationPath(), e);
                continue;
            }
            progress.finished();
        }
    }

    private List<DiagnosticsReportSource> getAllSources(Set<String> classifiers) {
        final List<DiagnosticsReportSource> allSources = new ArrayList<>();
        providers.forEach(provider -> allSources.addAll(provider.getDiagnosticsSources(classifiers)));
        additionalSources.forEach((classifier, sources) -> {
            if (classifiers.contains("all") || classifiers.contains(classifier)) {
                allSources.addAll(sources);
            }
        });
        return allSources;
    }

    private static void estimateSizeAndCheckAvailableDiskSpace(
            Path destination, List<DiagnosticsReportSource> sources, Path destinationDir) {
        final long estimatedFinalSize = sources.stream()
                .mapToLong(DiagnosticsReportSource::estimatedSize)
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
}
