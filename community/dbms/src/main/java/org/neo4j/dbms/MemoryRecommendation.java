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
package org.neo4j.dbms;

import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.io.ByteUnit.ONE_GIBI_BYTE;
import static org.neo4j.io.ByteUnit.ONE_KIBI_BYTE;
import static org.neo4j.io.ByteUnit.ONE_MEBI_BYTE;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.function.ToDoubleFunction;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.impl.index.storage.FailureStorage;
import org.neo4j.kernel.internal.IndexFileFilter;
import org.neo4j.storageengine.api.StorageEngineFactory;

public final class MemoryRecommendation {
    public static final String MEMORY = "--memory";
    // Fields: {System Memory in GiBs; OS memory reserve in GiBs; JVM Heap memory in GiBs}.
    // And the page cache gets what's left, though always at least 100 MiB.
    // Heap never goes beyond 31 GiBs.
    private static final Bracket[] datapoints = {
        new Bracket(0.01, 0.007, 0.002),
        new Bracket(1.0, 0.65, 0.3),
        new Bracket(2.0, 1, 0.5),
        new Bracket(4.0, 1.5, 2),
        new Bracket(6.0, 2, 3),
        new Bracket(8.0, 2.5, 3.5),
        new Bracket(10.0, 3, 4),
        new Bracket(12.0, 3.5, 4.5),
        new Bracket(16.0, 4, 5),
        new Bracket(24.0, 6, 8),
        new Bracket(32.0, 8, 12),
        new Bracket(64.0, 12, 24),
        new Bracket(128.0, 16, 31),
        new Bracket(256.0, 20, 31),
        new Bracket(512.0, 24, 31),
        new Bracket(1024.0, 30, 31),
    };

    private MemoryRecommendation() {}

    public static String bytesToString(double bytes) {
        double gibi1 = ONE_GIBI_BYTE;
        double mebi1 = ONE_MEBI_BYTE;
        double mebi100 = 100 * mebi1;
        double kibi1 = ONE_KIBI_BYTE;
        double kibi100 = 100 * kibi1;
        if (bytes >= gibi1) {
            double gibibytes = bytes / gibi1;
            double modMebi = bytes % gibi1;
            if (modMebi >= mebi100) {
                return format(ROOT, "%dm", Math.round(bytes / mebi100) * 100);
            } else {
                return format(ROOT, "%.0fg", gibibytes);
            }
        } else if (bytes >= mebi1) {
            double mebibytes = bytes / mebi1;
            double modKibi = bytes % mebi1;
            if (modKibi >= kibi100) {
                return format(ROOT, "%dk", Math.round(bytes / kibi100) * 100);
            } else {
                return format(ROOT, "%.0fm", mebibytes);
            }
        } else {
            // For kibibytes there's no need to bother with decimals, just print a rough figure rounded upwards
            double kibiBytes = bytes / kibi1;
            return format(ROOT, "%dk", (long) Math.ceil(kibiBytes));
        }
    }

    public static long recommendOsMemory(long totalMemoryBytes) {
        Brackets brackets = findMemoryBrackets(totalMemoryBytes);
        return brackets.recommend(Bracket::osMemory);
    }

    public static long recommendHeapMemory(long totalMemoryBytes) {
        Brackets brackets = findMemoryBrackets(totalMemoryBytes);
        return brackets.recommend(Bracket::heapMemory);
    }

    public static long recommendTxStateMemory(Config config, long heapMemoryBytes) {
        switch (config.get(tx_state_memory_allocation)) {
            case OFF_HEAP:
                long recommendation = heapMemoryBytes / 4;
                recommendation = Math.max(mebiBytes(128), recommendation);
                recommendation = Math.min(gibiBytes(8), recommendation);
                return recommendation;
            case ON_HEAP:
                return 0;
            default:
                throw new IllegalArgumentException("Unsupported type of memory allocation.");
        }
    }

    public static long recommendPageCacheMemory(long totalMemoryBytes, long offHeapMemory) {
        long osMemory = recommendOsMemory(totalMemoryBytes);
        long heapMemory = recommendHeapMemory(totalMemoryBytes);
        long recommendation = totalMemoryBytes - osMemory - heapMemory - offHeapMemory;
        recommendation = Math.max(mebiBytes(8), recommendation);
        recommendation = Math.min(tebiBytes(16), recommendation);
        return recommendation;
    }

    public static long recommendPageCacheMemory(long totalMemoryBytes) {
        return recommendPageCacheMemory(totalMemoryBytes, 0L);
    }

    private static Brackets findMemoryBrackets(long totalMemoryBytes) {
        double totalMemoryGB = ((double) totalMemoryBytes) / ((double) gibiBytes(1));
        Bracket lower = null;
        Bracket upper = null;
        for (int i = 1; i < datapoints.length; i++) {
            if (totalMemoryGB < datapoints[i].totalMemory) {
                lower = datapoints[i - 1];
                upper = datapoints[i];
                break;
            }
        }
        if (lower == null) {
            lower = datapoints[datapoints.length - 1];
            upper = datapoints[datapoints.length - 1];
        }
        return new Brackets(totalMemoryGB, lower, upper);
    }

    public static DirectoryStream.Filter<Path> wrapIndexFilter(
            IndexFileFilter indexFileFilter, FileSystemAbstraction fs) {
        return file -> {
            if (fs.isDirectory(file)) {
                // Always go down directories
                return true;
            }
            if (file.getFileName().toString().equals(FailureStorage.DEFAULT_FAILURE_FILE_NAME)) {
                // Never include failure-storage files
                return false;
            }

            return indexFileFilter.test(file);
        };
    }

    public static long sumStoreFiles(DatabaseLayout databaseLayout, FileSystemAbstraction fs) {
        return StorageEngineFactory.selectStorageEngine(fs, databaseLayout)
                .map(factory -> {
                    try {
                        long total = 0L;
                        for (Path path : factory.listStorageFiles(fs, databaseLayout)) {
                            total += fs.getFileSize(path);
                        }
                        return total;
                    } catch (IOException e) {
                        return 0L;
                    }
                })
                .orElse(0L);
    }

    public static long sumIndexFiles(Path file, DirectoryStream.Filter<Path> filter, FileSystemAbstraction fs)
            throws IOException {
        long total = 0;
        if (fs.isDirectory(file)) {
            Path[] children = fs.listFiles(file, filter);
            if (children != null) {
                for (Path child : children) {
                    total += sumIndexFiles(child, filter, fs);
                }
            }
        } else if (fs.fileExists(file)) {
            total += fs.getFileSize(file);
        }
        return total;
    }

    private record Bracket(double totalMemory, double osMemory, double heapMemory) {}

    private record Brackets(double totalMemoryGB, Bracket lower, Bracket upper) {
        private double differenceFactor() {
            if (lower == upper) {
                return 0;
            }
            return (totalMemoryGB - lower.totalMemory) / (upper.totalMemory - lower.totalMemory);
        }

        private long recommend(ToDoubleFunction<Bracket> parameter) {
            double factor = differenceFactor();
            double lowerParam = parameter.applyAsDouble(lower);
            double upperParam = parameter.applyAsDouble(upper);
            double diff = upperParam - lowerParam;
            double recommend = lowerParam + (diff * factor);
            return mebiBytes((long) (recommend * 1024.0));
        }
    }
}
