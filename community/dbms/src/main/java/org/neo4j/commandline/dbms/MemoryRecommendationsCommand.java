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

import static org.neo4j.configuration.BootloaderSettings.additional_jvm;
import static org.neo4j.configuration.BootloaderSettings.initial_heap_size;
import static org.neo4j.configuration.BootloaderSettings.max_heap_size;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.dbms.MemoryRecommendation.MEMORY;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.baseSchemaIndexFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.MemoryRecommendation;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.os.OsBeanUtil;
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

    @Override
    protected void execute() throws IOException {
        if (memory == null) {
            memory = OsBeanUtil.getTotalPhysicalMemory();
        }
        Path configFile = ctx.confDir().resolve(Config.DEFAULT_CONFIG_FILE_NAME);
        Config config = getConfig(configFile);

        final long offHeapMemory = MemoryRecommendation.recommendTxStateMemory(config, memory);
        String os = MemoryRecommendation.bytesToString(MemoryRecommendation.recommendOsMemory(memory));
        String heap = MemoryRecommendation.bytesToString(MemoryRecommendation.recommendHeapMemory(memory));
        String pageCache = MemoryRecommendation.bytesToString(
                MemoryRecommendation.recommendPageCacheMemory(memory, offHeapMemory));
        String txState = MemoryRecommendation.bytesToString(offHeapMemory);

        Path databasesRoot = config.get(databases_root_path);
        Neo4jLayout storeLayout = Neo4jLayout.of(config);
        Collection<DatabaseLayout> layouts = storeLayout.databaseLayouts();
        long pageCacheSize = pageCacheSize(layouts);
        long luceneSize = luceneSize(layouts);

        print("# Memory settings recommendation:");
        print("#");
        print("# Assuming the system is dedicated to running Neo4j and has " + ByteUnit.bytesToString(memory)
                + " of memory,");
        print("# we recommend a heap size of around " + heap + ", and a page cache of around " + pageCache + ",");
        print("# and that about " + os + " is left for the operating system, and the native memory");
        print("# needed by Lucene and Netty.");
        print("#");
        print("# Tip: If the indexing storage use is high, e.g. there are many indexes or most");
        print("# data indexed, then it might advantageous to leave more memory for the");
        print("# operating system.");
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
        printSetting(pagecache_memory, pageCache);
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
        print("# Total size of lucene indexes in all databases: " + MemoryRecommendation.bytesToString(luceneSize));
        print("# Total size of data and native indexes in all databases: "
                + MemoryRecommendation.bytesToString(pageCacheSize));
    }

    private void printSetting(Setting<?> setting, String value) {
        if (!dockerOutput) {
            print(setting.name() + "=" + value);
        } else {
            var nameWithFixedUnderscores = setting.name().replace("_", "__");
            var nameWithFixedUnderscoresAndDots = nameWithFixedUnderscores.replace('.', '_');
            print("NEO4J_" + nameWithFixedUnderscoresAndDots + "='" + value + "'");
        }
    }

    private long pageCacheSize(Collection<DatabaseLayout> layouts) throws IOException {
        long sum = 0L;
        for (DatabaseLayout layout : layouts) {
            sum += getDatabasePageCacheSize(layout, ctx.fs());
        }
        return sum;
    }

    private long getDatabasePageCacheSize(DatabaseLayout layout, FileSystemAbstraction fs) throws IOException {
        return MemoryRecommendation.sumStoreFiles(layout, fs)
                + MemoryRecommendation.sumIndexFiles(
                        baseSchemaIndexFolder(layout.databaseDirectory()),
                        MemoryRecommendation.getNativeIndexFileFilter(layout.databaseDirectory(), false, fs),
                        fs);
    }

    private long luceneSize(Collection<DatabaseLayout> layouts) throws IOException {
        long sum = 0L;
        for (DatabaseLayout layout : layouts) {
            sum += getDatabaseLuceneSize(layout, ctx.fs());
        }
        return sum;
    }

    private long getDatabaseLuceneSize(DatabaseLayout databaseLayout, FileSystemAbstraction fs) throws IOException {
        Path databaseDirectory = databaseLayout.databaseDirectory();
        return MemoryRecommendation.sumIndexFiles(
                baseSchemaIndexFolder(databaseDirectory),
                MemoryRecommendation.getNativeIndexFileFilter(databaseDirectory, true, fs),
                fs);
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
}
