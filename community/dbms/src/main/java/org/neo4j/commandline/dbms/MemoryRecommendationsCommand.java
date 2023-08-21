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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.MEMORY;
import static org.neo4j.configuration.BootloaderSettings.additional_jvm;
import static org.neo4j.configuration.BootloaderSettings.initial_heap_size;
import static org.neo4j.configuration.BootloaderSettings.max_heap_size;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.io.ByteUnit.ONE_GIBI_BYTE;
import static org.neo4j.io.ByteUnit.ONE_KIBI_BYTE;
import static org.neo4j.io.ByteUnit.ONE_MEBI_BYTE;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.baseSchemaIndexFolder;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.api.impl.index.storage.FailureStorage;
import org.neo4j.kernel.internal.NativeIndexFileFilter;
import org.neo4j.storageengine.api.StorageEngineFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = "memory-recommendation",
        header = "Print Neo4j heap and pagecache memory settings recommendations.",
        description = "Print heuristic memory setting recommendations for the Neo4j JVM heap and pagecache. "
                + "The heuristic is based on the total memory of the system the command is running on, or on the "
                + "amount of memory specified with the " + MEMORY + " argument. "
                + "The heuristic assumes that the system is dedicated to running Neo4j. "
                + "If this is not the case, then use the " + MEMORY + " argument to specify how much memory can be "
                + "expected to be dedicated to Neo4j. "
                + "The output is formatted such that it can be copy-pasted into the neo4j.conf file.")
public class MemoryRecommendationsCommand extends AbstractAdminCommand {
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

    static final String MEMORY = "--memory";

    @Option(
            names = MEMORY,
            paramLabel = "<size>",
            converter = Converters.ByteUnitConverter.class,
            description =
                    "Recommend memory settings with respect to the given amount of memory, instead of the total memory of the system running the command. "
                            + "Valid units are: k, K, m, M, g, G.")
    private Long memory;

    @Option(
            names = "--docker",
            fallbackValue = "true",
            description = "The recommended memory settings are produced in the form of environment variables "
                    + "that can be directly passed to a Neo4j docker container. The recommended use is to save "
                    + "the generated environment variables to a file and pass the file to a docker container "
                    + "using the '--env-file' docker option.")
    private boolean dockerOutput;

    public MemoryRecommendationsCommand(ExecutionContext ctx) {
        super(ctx);
    }

    static long recommendOsMemory(long totalMemoryBytes) {
        Brackets brackets = findMemoryBrackets(totalMemoryBytes);
        return brackets.recommend(Bracket::osMemory);
    }

    static long recommendHeapMemory(long totalMemoryBytes) {
        Brackets brackets = findMemoryBrackets(totalMemoryBytes);
        return brackets.recommend(Bracket::heapMemory);
    }

    static long recommendTxStateMemory(Config config, long heapMemoryBytes) {
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

    static long recommendPageCacheMemory(long totalMemoryBytes, long offHeapMemory) {
        long osMemory = recommendOsMemory(totalMemoryBytes);
        long heapMemory = recommendHeapMemory(totalMemoryBytes);
        long recommendation = totalMemoryBytes - osMemory - heapMemory - offHeapMemory;
        recommendation = Math.max(mebiBytes(8), recommendation);
        recommendation = Math.min(tebiBytes(16), recommendation);
        return recommendation;
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

    static String bytesToString(double bytes) {
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

    @Override
    protected void execute() throws IOException {
        if (memory == null) {
            memory = OsBeanUtil.getTotalPhysicalMemory();
        }
        Path configFile = ctx.confDir().resolve(Config.DEFAULT_CONFIG_FILE_NAME);
        Config config = getConfig(configFile);

        final long offHeapMemory = recommendTxStateMemory(config, memory);
        String os = bytesToString(recommendOsMemory(memory));
        String heap = bytesToString(recommendHeapMemory(memory));
        String pagecache = bytesToString(recommendPageCacheMemory(memory, offHeapMemory));
        String txState = bytesToString(offHeapMemory);

        Path databasesRoot = config.get(databases_root_path);
        Neo4jLayout storeLayout = Neo4jLayout.of(config);
        Collection<DatabaseLayout> layouts = storeLayout.databaseLayouts();
        long pageCacheSize = pageCacheSize(layouts);
        long luceneSize = luceneSize(layouts);

        print("# Memory settings recommendation:");
        print("#");
        print("# Assuming the system is dedicated to running Neo4j and has " + ByteUnit.bytesToString(memory)
                + " of memory,");
        print("# we recommend a heap size of around " + heap + ", and a page cache of around " + pagecache + ",");
        print("# and that about " + os + " is left for the operating system, and the native memory");
        print("# needed by Lucene and Netty.");
        print("#");
        print("# Tip: If the indexing storage use is high, e.g. there are many indexes or most");
        print("# data indexed, then it might advantageous to leave more memory for the");
        print("# operating system.");
        print("#");
        print("# Tip: Depending on the workload type you may want to increase the amount");
        print("# of off-heap memory available for storing transaction state.");
        print("# For instance, in case of large write-intensive transactions");
        print("# increasing it can lower GC overhead and thus improve performance.");
        print("# On the other hand, if vast majority of transactions are small or read-only");
        print("# then you can decrease it and increase page cache instead.");
        print("#");
        print("# Tip: The more concurrent transactions your workload has and the more updates");
        print("# they do, the more heap memory you will need. However, don't allocate more");
        print("# than 31g of heap, since this will disable pointer compression, also known as");
        print("# \"compressed oops\", in the JVM and make less effective use of the heap.");
        print("#");
        print("# Tip: Setting the initial and the max heap size to the same value means the");
        print("# JVM will never need to change the heap size. Changing the heap size otherwise");
        print("# involves a full GC, which is desirable to avoid.");
        print("#");
        print("# Based on the above, the following memory settings are recommended:");
        printSetting(initial_heap_size, heap);
        printSetting(max_heap_size, heap);
        printSetting(pagecache_memory, pagecache);
        if (offHeapMemory != 0) {
            printSetting(tx_state_max_off_heap_memory, txState);
        }
        print("#");
        print("# It is also recommended turning out-of-memory errors into full crashes,");
        print("# instead of allowing a partially crashed database to continue running:");
        printSetting(additional_jvm, "-XX:+ExitOnOutOfMemoryError");
        print("#");
        print("# The numbers below have been derived based on your current databases located at: '" + databasesRoot
                + "'.");
        print("# They can be used as an input into more detailed memory analysis.");
        print("# Total size of lucene indexes in all databases: " + bytesToString(luceneSize));
        print("# Total size of data and native indexes in all databases: " + bytesToString(pageCacheSize));
    }

    private void printSetting(Setting<?> setting, String value) {
        if (!dockerOutput) {
            print(setting.name() + "=" + value);
        } else {
            var nameWithFixedUnderscores = setting.name().replaceAll("_", "__");
            var nameWithFixedUnderscoresAndDots = nameWithFixedUnderscores.replace('.', '_');
            print("NEO4J_" + nameWithFixedUnderscoresAndDots + "='" + value + "'");
        }
    }

    private long pageCacheSize(Collection<DatabaseLayout> layouts) throws IOException {
        long sum = 0L;
        for (DatabaseLayout layout : layouts) {
            sum += getDatabasePageCacheSize(layout);
        }
        return sum;
    }

    private long getDatabasePageCacheSize(DatabaseLayout layout) throws IOException {
        return sumStoreFiles(layout)
                + sumIndexFiles(
                        baseSchemaIndexFolder(layout.databaseDirectory()),
                        getNativeIndexFileFilter(layout.databaseDirectory(), false));
    }

    private long luceneSize(Collection<DatabaseLayout> layouts) throws IOException {
        long sum = 0L;
        for (DatabaseLayout layout : layouts) {
            sum += getDatabaseLuceneSize(layout);
        }
        return sum;
    }

    private long getDatabaseLuceneSize(DatabaseLayout databaseLayout) throws IOException {
        Path databaseDirectory = databaseLayout.databaseDirectory();
        return sumIndexFiles(
                baseSchemaIndexFolder(databaseDirectory), getNativeIndexFileFilter(databaseDirectory, true));
    }

    private DirectoryStream.Filter<Path> getNativeIndexFileFilter(Path storeDir, boolean inverse) {
        Predicate<Path> nativeIndexFilter = new NativeIndexFileFilter(storeDir);
        return file -> {
            if (ctx.fs().isDirectory(file)) {
                // Always go down directories
                return true;
            }
            if (file.getFileName().toString().equals(FailureStorage.DEFAULT_FAILURE_FILE_NAME)) {
                // Never include failure-storage files
                return false;
            }

            return inverse != nativeIndexFilter.test(file);
        };
    }

    private long sumStoreFiles(DatabaseLayout databaseLayout) {
        FileSystemAbstraction fileSystem = ctx.fs();
        return StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout)
                .map(factory -> {
                    try {
                        long total = 0L;
                        for (Path path : factory.listStorageFiles(fileSystem, databaseLayout)) {
                            total += fileSystem.getFileSize(path);
                        }
                        return total;
                    } catch (IOException e) {
                        return 0L;
                    }
                })
                .orElse(0L);
    }

    private long sumIndexFiles(Path file, DirectoryStream.Filter<Path> filter) throws IOException {
        long total = 0;
        final var fs = ctx.fs();
        if (fs.isDirectory(file)) {
            Path[] children = fs.listFiles(file, filter);
            if (children != null) {
                for (Path child : children) {
                    total += sumIndexFiles(child, filter);
                }
            }
        } else if (fs.fileExists(file)) {
            total += fs.getFileSize(file);
        }
        return total;
    }

    private Config getConfig(Path configFile) {
        if (!ctx.fs().fileExists(configFile)) {
            throw new CommandFailedException("Unable to find config file, tried: " + configFile.toAbsolutePath());
        }
        Config config = Config.newBuilder()
                .fromFile(configFile)
                .set(GraphDatabaseSettings.neo4j_home, ctx.homeDir().toAbsolutePath())
                .commandExpansion(allowCommandExpansion)
                .build();
        ConfigUtils.disableAllConnectors(config);
        return config;
    }

    private void print(String text) {
        ctx.out().println(text);
    }

    private record Bracket(double totalMemory, double osMemory, double heapMemory) {}

    private record Brackets(double totalMemoryGB, Bracket lower, Bracket upper) {
        private double differenceFactor() {
            if (lower == upper) {
                return 0;
            }
            return (totalMemoryGB - lower.totalMemory) / (upper.totalMemory - lower.totalMemory);
        }

        public long recommend(ToDoubleFunction<Bracket> parameter) {
            double factor = differenceFactor();
            double lowerParam = parameter.applyAsDouble(lower);
            double upperParam = parameter.applyAsDouble(upper);
            double diff = upperParam - lowerParam;
            double recommend = lowerParam + (diff * factor);
            return mebiBytes((long) (recommend * 1024.0));
        }
    }
}
