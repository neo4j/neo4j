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
package org.neo4j.kernel.diagnostics.providers;

import static org.neo4j.io.ByteUnit.bytesToString;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.NamedDiagnosticsProvider;
import org.neo4j.internal.helpers.Format;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.NativeIndexFileFilter;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class StoreFilesDiagnostics extends NamedDiagnosticsProvider {
    private final StorageEngineFactory storageEngineFactory;
    private final FileSystemAbstraction fs;
    private final DatabaseLayout databaseLayout;
    private final DeviceMapper deviceMapper;

    public StoreFilesDiagnostics(
            StorageEngineFactory storageEngineFactory,
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            DeviceMapper deviceMapper) {
        super("Store files");
        this.storageEngineFactory = storageEngineFactory;
        this.fs = fs;
        this.databaseLayout = databaseLayout;
        this.deviceMapper = deviceMapper;
    }

    @Override
    public void dump(DiagnosticsLogger logger) {
        logger.log(getDiskSpace(databaseLayout));
        logger.log(
                "Storage files stored on file store: " + deviceMapper.describePath(databaseLayout.databaseDirectory()));
        logger.log("Storage files: (filename : modification date - size)");
        MappedFileCounter mappedCounter = new MappedFileCounter();
        long totalSize = logStoreFiles(logger, "  ", databaseLayout.databaseDirectory(), mappedCounter);
        logger.log("Storage summary: ");
        logger.log("  Total size of store: " + bytesToString(totalSize));
        logger.log("  Total size of mapped files: " + bytesToString(mappedCounter.getSize()));
    }

    private long logStoreFiles(DiagnosticsLogger logger, String prefix, Path dir, MappedFileCounter mappedCounter) {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        Path[] files = FileUtils.listPaths(dir);
        if (files == null) {
            logger.log(prefix + "<INACCESSIBLE>");
            return 0;
        }
        long total = 0;

        // Sort by name
        List<Path> fileList = Arrays.asList(files);
        fileList.sort(Comparator.comparing(Path::getFileName));

        for (Path file : fileList) {
            long size = 0;
            String filename = file.getFileName().toString();
            if (Files.isDirectory(file)) {
                logger.log(prefix + filename + ":");
                size = logStoreFiles(logger, prefix + "  ", file, mappedCounter);
                filename = "- Total";
            } else {
                try {
                    size = Files.size(file);
                } catch (IOException ignored) {
                    // Preserve behaviour of File.length()
                }
                mappedCounter.addFile(file);
            }

            String fileModificationDate = getFileModificationDate(file);
            String bytes = bytesToString(size);
            String fileInformation = String.format("%s%s: %s - %s", prefix, filename, fileModificationDate, bytes);
            logger.log(fileInformation);

            total += size;
        }
        return total;
    }

    private static String getFileModificationDate(Path file) {
        try {
            Instant instant = Files.getLastModifiedTime(file).toInstant();
            return Format.date(instant);
        } catch (IOException e) {
            return "<UNKNOWN>";
        }
    }

    private static String getDiskSpace(DatabaseLayout databaseLayout) {
        String header = "Disk space on partition (Total / Free / Free %%):";
        try {
            Path directory = databaseLayout.databaseDirectory();
            FileStore fileStore = Files.getFileStore(directory);
            long free = fileStore.getUnallocatedSpace();
            long total = fileStore.getTotalSpace();
            long percentage = total != 0 ? (free * 100 / total) : 0;
            return String.format(header + " %s / %s / %s", total, free, percentage);
        } catch (IOException e) {
            return header + " Unable to determine disk space on the partition";
        }
    }

    private class MappedFileCounter {
        private final Set<Path> mappedCandidates = new HashSet<>();
        private long size;
        private final Predicate<Path> mappedIndexFilter;

        MappedFileCounter() {
            try {
                mappedCandidates.addAll(storageEngineFactory.listStorageFiles(fs, databaseLayout));
            } catch (IOException e) {
                // Hmm, there was no storage here
            }
            mappedIndexFilter = new NativeIndexFileFilter(databaseLayout.databaseDirectory());
        }

        void addFile(Path file) {
            if (canBeManagedByPageCache(file) || mappedIndexFilter.test(file)) {
                try {
                    size += Files.size(file);
                } catch (IOException ignored) {
                    // Preserve behaviour of File.length()
                }
            }
        }

        public long getSize() {
            return size;
        }

        /**
         * Returns whether or not store file by given file name should be managed by the page cache.
         *
         * @param storeFile file of the store file to check.
         * @return Returns whether or not store file by given file name should be managed by the page cache.
         */
        boolean canBeManagedByPageCache(Path storeFile) {
            return mappedCandidates.contains(storeFile);
        }
    }
}
