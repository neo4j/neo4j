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
package org.neo4j.batchimport.api;

import static org.neo4j.configuration.ToolingMemoryCalculations.NO_MONITOR;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.util.FeatureToggles.getInteger;

import java.nio.file.Path;
import org.neo4j.configuration.ToolingMemoryCalculations;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;

/**
 * Configuration for a an importer, mostly how and how much resources are used.
 */
public sealed interface Configuration permits Configuration.DefaultConfiguration, Configuration.Overridden {

    /**
     * Size of batches that are being sent through pipelines for processing.
     */
    int batchSize();

    /**
     * @return size of batches (in number of entities) to pass around in the internal data processing.
     * This is different from {@link #batchSize()} mostly in the sens of {@link #batchSize()} being used
     * on a record level (and in aligned etc. format), whereas this method is used when batching full
     * entities for processing (and in block format).
     */
    int entityBatchSize();

    /**
     * For statistics the average processing time is based on total processing time divided by
     * number of batches processed. A total average is probably not that interesting so this configuration
     * option specifies how many of the latest processed batches counts in the equation above.
     */
    int movingAverageSize();

    /**
     * Rough max number of processors (CPU cores) simultaneously used in total by importer at any given time.
     * This value should be set while taking the necessary IO threads into account; the page cache and the operating
     * system will require a couple of threads between them, to handle the IO workload the importer generates.
     * Defaults to the value provided by the {@link Runtime#availableProcessors() jvm}. There's a discrete
     * number of threads that needs to be used just to get the very basics of the import working,
     * so for that reason there's no lower bound to this value.
     *   "Processor" in the context of the batch importer is different from "thread" since when discovering
     * how many processors are fully in use there's a calculation where one thread takes up 0 < fraction <= 1
     * of a processor.
     */
    int maxNumberOfWorkerThreads();

    /**
     * The maximum number of batches that will be queued in processor steps. The total number of "alive" batches
     * will roughly be this value + number of processors assigned to the step that has the most processors assigned.
     */
    int maxQueueSize();

    /**
     * @return max memory to use for import cache data structures while importing.
     * This should exclude the memory acquired by this JVM. By default this returns total physical
     * memory on the machine it's running on minus the max memory of this JVM. The value is sampled
     * once on initialization.
     * @throws UnsupportedOperationException if available memory couldn't be determined.
     */
    long maxOffHeapMemory();

    /**
     * @return whether or not to do sequential flushing of the page cache in the during stages which
     * import nodes and relationships. Having this {@code true} will reduce random I/O and make most
     * writes happen in this single background thread and will greatly benefit hardware which generally
     * benefits from single sequential writer.
     */
    boolean sequentialBackgroundFlushing();

    /**
     * Controls whether or not to write records in parallel. Multiple threads writing records in parallel
     * doesn't necessarily mean concurrent I/O because writing is separate from page cache eviction/flushing.
     * <p/>
     * This setting is set to true by default.
     */
    boolean parallelRecordWrites();

    /**
     * Controls whether or not to read records in parallel in stages where there's no record writing.
     * Enabling this may result in multiple pages being read from underlying storage concurrently.
     * <p/>
     * This setting is set to true by default.
     */
    boolean parallelRecordReads();

    /**
     * Controls whether or not to read records in parallel in stages where there's concurrent record writing.
     * Enabling will probably increase concurrent I/O to a point which reduces performance if underlying storage
     * isn't great at concurrent I/O, especially if also {@link #parallelRecordWrites()} is enabled.
     */
    boolean highIO();

    /**
     * Wheter or not identity of nodes referred to by relationships should be checked strict.
     * A strict check means that the id of the referred node must match not only by hash but
     * also by value. This will detect non-existing nodes that happen to have the same id hash
     * as an existing node during relationship import.
     */
    boolean strictNodeCheck();

    /**
     * @return index related configurations.
     */
    IndexConfig indexConfig();

    /**
     * @return whether to defragment internal stores as part of the import. This type of defragmentation does not affect
     * entity IDs.
     */
    boolean defragmentInternalStores();

    /**
     * In incremental import when identifying relationships, if multiple relationships are found
     * then if {@code true} then all matching relationships should be updated, otherwise if {@code false}
     * then if there are multiple matching relationships report it instead.
     */
    boolean updateAllMatchingRelationships();

    /**
     * For importers that can run multiple passes where each pass imports data from a range of node IDs,
     * this method can be provided form the outside to force the number of ranges to let the importer
     * go through.
     * @return number of ranges to force the importer to run with, or {@code -1} if no specific number
     * of ranges is forcefully set.
     */
    int forcedNumberOfNodeIdRanges();

    /**
     * @return a {@link PageCache} which, if non-null, should be used instead of creating a new one.
     */
    ExternallyManagedPageCache providedPageCache();

    Path tempDirectory(Path databaseDirectory);

    /**
     * Helps calculation of memory consumption for index population. This value is an indication of number of
     * importers operating concurrently within the same import, each potentially having its own set of indexes to build.
     */
    int indexPopulationBufferCountAmplification();

    /**
     * Enables performance monitoring instrumentation, will be a NO-OP if none of {@code
     * instrumentationCaptureJFRs()} {@code instrumentationCaptureThreadDumps} are set to true
     */
    boolean enableInstrumentation();

    /**
     * Enables JFR captures when {@code ImportPerformanceInstrumentation} notices significant performance loss
     * Setting is ignored if {@code enableInstrumentation} is {@code false}
     */
    boolean instrumentationCaptureJFRs();

    /**
     * Enables Thread dump captures when {@code ImportPerformanceInstrumentation} notices significant performance loss.
     * Setting is ignored if {@code enableInstrumentation} is {@code false}
     */
    boolean instrumentationCaptureThreadDumps();

    /**
     * Enables JFR capture for the full duration of the import
     */
    boolean captureProfile();

    /**
     * If not null, provides an overridden path to put the JFRs generated from {@link #captureProfile()};
     */
    Path captureProfileResultPath();

    /**
     * Directory for capturing any context information for this specific import, or {@code null} if this import has
     * no such directory.
     */
    Path contextDirectory();

    int intermediaryBufferSize();

    final class DefaultConfiguration implements Configuration {
        private static final int DEFAULT_BATCH_SIZE = getInteger(Configuration.class, "DEFAULT_BATCH_SIZE", 10_000);
        private static final int DEFAULT_ENTITY_BATCH_SIZE = 500;
        private static final int DEFAULT_MAX_MEMORY_PERCENT = 90;
        private static final int NUM_AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
        private static final long MAX_OFF_HEAP_MEMORY = new ToolingMemoryCalculations(NO_MONITOR)
                .calculateMaxAvailableOffHeapMemoryFromPercent(DEFAULT_MAX_MEMORY_PERCENT);

        private DefaultConfiguration() {}

        @Override
        public int batchSize() {
            return DEFAULT_BATCH_SIZE;
        }

        @Override
        public int entityBatchSize() {
            return DEFAULT_ENTITY_BATCH_SIZE;
        }

        @Override
        public int movingAverageSize() {
            return 100;
        }

        public int maxNumberOfWorkerThreads() {
            return NUM_AVAILABLE_PROCESSORS;
        }

        @Override
        public int maxQueueSize() {
            return maxNumberOfWorkerThreads();
        }

        public long maxOffHeapMemory() {
            return MAX_OFF_HEAP_MEMORY;
        }

        @Override
        public boolean sequentialBackgroundFlushing() {
            return true;
        }

        @Override
        public boolean parallelRecordWrites() {
            return true;
        }

        @Override
        public boolean parallelRecordReads() {
            return true;
        }

        @Override
        public boolean highIO() {
            return true;
        }

        @Override
        public boolean strictNodeCheck() {
            return false;
        }

        @Override
        public IndexConfig indexConfig() {
            return IndexConfig.create();
        }

        @Override
        public boolean defragmentInternalStores() {
            return true;
        }

        @Override
        public boolean updateAllMatchingRelationships() {
            return false;
        }

        @Override
        public int forcedNumberOfNodeIdRanges() {
            return -1;
        }

        @Override
        public ExternallyManagedPageCache providedPageCache() {
            return null;
        }

        @Override
        public Path tempDirectory(Path databaseDirectory) {
            return databaseDirectory.resolve("temp");
        }

        @Override
        public int indexPopulationBufferCountAmplification() {
            return 1;
        }

        @Override
        public boolean enableInstrumentation() {
            return true;
        }

        @Override
        public boolean instrumentationCaptureJFRs() {
            return true;
        }

        @Override
        public boolean instrumentationCaptureThreadDumps() {
            return false;
        }

        @Override
        public boolean captureProfile() {
            return false;
        }

        @Override
        public Path captureProfileResultPath() {
            return null;
        }

        @Override
        public Path contextDirectory() {
            return null;
        }

        @Override
        public int intermediaryBufferSize() {
            return (int) mebiBytes(10);
        }
    }

    Configuration DEFAULT = new DefaultConfiguration();

    non-sealed class Overridden implements Configuration {
        private final Configuration defaults;

        public Overridden(Configuration defaults) {
            this.defaults = defaults;
        }

        @Override
        public int movingAverageSize() {
            return defaults.movingAverageSize();
        }

        @Override
        public boolean sequentialBackgroundFlushing() {
            return defaults.sequentialBackgroundFlushing();
        }

        @Override
        public int batchSize() {
            return defaults.batchSize();
        }

        @Override
        public int entityBatchSize() {
            return defaults.entityBatchSize();
        }

        @Override
        public int maxNumberOfWorkerThreads() {
            return defaults.maxNumberOfWorkerThreads();
        }

        @Override
        public boolean parallelRecordWrites() {
            return defaults.parallelRecordWrites();
        }

        @Override
        public boolean parallelRecordReads() {
            return defaults.parallelRecordReads();
        }

        @Override
        public boolean highIO() {
            return defaults.highIO();
        }

        @Override
        public long maxOffHeapMemory() {
            return defaults.maxOffHeapMemory();
        }

        @Override
        public boolean defragmentInternalStores() {
            return defaults.defragmentInternalStores();
        }

        @Override
        public int forcedNumberOfNodeIdRanges() {
            return defaults.forcedNumberOfNodeIdRanges();
        }

        @Override
        public boolean updateAllMatchingRelationships() {
            return defaults.updateAllMatchingRelationships();
        }

        @Override
        public ExternallyManagedPageCache providedPageCache() {
            return defaults.providedPageCache();
        }

        @Override
        public IndexConfig indexConfig() {
            return defaults.indexConfig();
        }

        @Override
        public Path tempDirectory(Path databaseDirectory) {
            return defaults.tempDirectory(databaseDirectory);
        }

        @Override
        public int maxQueueSize() {
            // Note: preserves the semantics of the default class.
            return maxNumberOfWorkerThreads();
        }

        @Override
        public boolean strictNodeCheck() {
            return defaults.strictNodeCheck();
        }

        @Override
        public int indexPopulationBufferCountAmplification() {
            return defaults.indexPopulationBufferCountAmplification();
        }

        @Override
        public boolean enableInstrumentation() {
            return defaults.enableInstrumentation();
        }

        @Override
        public boolean instrumentationCaptureJFRs() {
            return defaults.instrumentationCaptureJFRs();
        }

        @Override
        public boolean instrumentationCaptureThreadDumps() {
            return defaults.instrumentationCaptureThreadDumps();
        }

        @Override
        public boolean captureProfile() {
            return defaults.captureProfile();
        }

        @Override
        public Path captureProfileResultPath() {
            return defaults.captureProfileResultPath();
        }

        @Override
        public Path contextDirectory() {
            return defaults.contextDirectory();
        }

        @Override
        public int intermediaryBufferSize() {
            return defaults.intermediaryBufferSize();
        }
    }

    static Configuration withBatchSize(Configuration config, int batchSize) {
        return new Overridden(config) {
            @Override
            public int batchSize() {
                return batchSize;
            }
        };
    }
}
