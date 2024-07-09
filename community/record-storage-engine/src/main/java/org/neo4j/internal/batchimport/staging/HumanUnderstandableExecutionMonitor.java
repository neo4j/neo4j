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
package org.neo4j.internal.batchimport.staging;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.internal.batchimport.ImportMemoryCalculator.defensivelyPadMemoryEstimate;
import static org.neo4j.internal.batchimport.ImportMemoryCalculator.estimatedCacheSize;
import static org.neo4j.internal.batchimport.cache.GatheringMemoryStatsVisitor.totalMemoryUsageOf;
import static org.neo4j.internal.helpers.Format.count;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.internal.helpers.Format.localDate;
import static org.neo4j.internal.helpers.collection.Iterables.last;
import static org.neo4j.io.ByteUnit.bytesToString;

import java.io.PrintStream;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.batchimport.CountGroupsStage;
import org.neo4j.internal.batchimport.DataImporter;
import org.neo4j.internal.batchimport.DataStatistics;
import org.neo4j.internal.batchimport.IdMapperPreparationStage;
import org.neo4j.internal.batchimport.NodeDegreeCountStage;
import org.neo4j.internal.batchimport.RelationshipGroupStage;
import org.neo4j.internal.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.internal.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.cache.PageCacheArrayFactoryMonitor;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.batchimport.stats.Stat;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;

/**
 * Prints progress you can actually understand, with capabilities to on demand print completely incomprehensible
 * details only understandable to a select few.
 */
public class HumanUnderstandableExecutionMonitor implements ExecutionMonitor {
    public enum ImportStage {
        nodeImport("Node import"),
        relationshipImport("Relationship import"),
        linking("Relationship linking"),
        postProcessing("Post processing");

        private final String description;

        ImportStage(String description) {
            this.description = description;
        }

        String descriptionWithOrdinal() {
            return format("(%d/%d) %s", ordinal() + 1, values().length, description);
        }

        String description() {
            return description;
        }
    }

    private static final String ESTIMATED_REQUIRED_MEMORY_USAGE = "Estimated required memory usage";
    private static final String ESTIMATED_DISK_SPACE_USAGE = "Estimated disk space usage";
    private static final String ESTIMATED_NUMBER_OF_RELATIONSHIP_PROPERTIES =
            "Estimated number of relationship properties";
    private static final String ESTIMATED_NUMBER_OF_RELATIONSHIPS = "Estimated number of relationships";
    private static final String ESTIMATED_NUMBER_OF_NODE_PROPERTIES = "Estimated number of node properties";
    private static final String ESTIMATED_NUMBER_OF_NODES = "Estimated number of nodes";

    private final Monitor monitor;
    private final PrintStream out;
    private final PrintStream err;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final WeightedExternalProgressReporter externalProgressIndicator;
    private DependencyResolver dependencyResolver;
    private PageCacheArrayFactoryMonitor pageCacheArrayFactoryMonitor;

    // progress of current stage
    private double externalProgressNodeWeight;
    private double externalProgressRelationshipWeight;
    private ProgressListener progressListener;
    private long lastReportedProgress;
    private ImportStage currentStage;
    private long stageStartTime;

    public HumanUnderstandableExecutionMonitor(Monitor monitor, PrintStream out, PrintStream err) {
        this.monitor = monitor;
        this.out = out;
        this.err = err;
        this.progressMonitorFactory = ProgressMonitorFactory.textual(out, true, 10, 5, 20);
        this.externalProgressIndicator = new WeightedExternalProgressReporter(monitor);
    }

    @Override
    public void initialize(DependencyResolver dependencyResolver) {
        this.dependencyResolver = dependencyResolver;
        Input.Estimates estimates = dependencyResolver.resolveDependency(Input.Estimates.class);
        BatchingNeoStores neoStores = dependencyResolver.resolveDependency(BatchingNeoStores.class);
        IdMapper idMapper = dependencyResolver.resolveDependency(IdMapper.class);
        pageCacheArrayFactoryMonitor = dependencyResolver.resolveDependency(PageCacheArrayFactoryMonitor.class);

        long biggestCacheMemory = estimatedCacheSize(
                neoStores,
                NodeRelationshipCache.memoryEstimation(estimates.numberOfNodes()),
                idMapper.memoryEstimation(estimates.numberOfNodes()));
        out.println();
        out.println(stageHeader(
                "Import starting",
                ESTIMATED_NUMBER_OF_NODES,
                count(estimates.numberOfNodes()),
                ESTIMATED_NUMBER_OF_NODE_PROPERTIES,
                count(estimates.numberOfNodeProperties()),
                ESTIMATED_NUMBER_OF_RELATIONSHIPS,
                count(estimates.numberOfRelationships()),
                ESTIMATED_NUMBER_OF_RELATIONSHIP_PROPERTIES,
                count(estimates.numberOfRelationshipProperties()),
                ESTIMATED_DISK_SPACE_USAGE,
                bytesToString(nodesDiskUsage(estimates, neoStores)
                        + relationshipsDiskUsage(estimates, neoStores)
                        + estimates.sizeOfNodeProperties()
                        + estimates.sizeOfRelationshipProperties()),
                ESTIMATED_REQUIRED_MEMORY_USAGE,
                bytesToString(biggestCacheMemory)));
        out.println();

        long numNodes = Math.max(1, estimates.numberOfNodes());
        long numRelationships = Math.max(1, estimates.numberOfRelationships());
        long totalEntities = numNodes + numRelationships * 2;
        externalProgressNodeWeight = (((double) numNodes / totalEntities)) * 0.8D;
        externalProgressRelationshipWeight = (((double) numRelationships / totalEntities)) * 0.8D;
    }

    private static long baselineMemoryRequirement(BatchingNeoStores neoStores) {
        return totalMemoryUsageOf(neoStores);
    }

    private static long nodesDiskUsage(Input.Estimates estimates, BatchingNeoStores neoStores) {
        return // node store
        estimates.numberOfNodes() * neoStores.getNodeStore().getRecordSize()
                +
                // label index (1 byte per label is not a terrible estimate)
                estimates.numberOfNodeLabels();
    }

    private static long relationshipsDiskUsage(Input.Estimates estimates, BatchingNeoStores neoStores) {
        return estimates.numberOfRelationships()
                * neoStores.getRelationshipStore().getRecordSize()
                * (neoStores.usesDoubleRelationshipRecordUnits() ? 2 : 1);
    }

    @Override
    public void start(StageExecution execution) {
        // Divide into 4 progress stages:
        if (execution.getStageName().equals(DataImporter.NODE_IMPORT_NAME)) {
            // Import nodes:
            // - import nodes
            // - prepare id mapper
            initializeNodeImport(
                    dependencyResolver.resolveDependency(Input.Estimates.class),
                    dependencyResolver.resolveDependency(IdMapper.class),
                    dependencyResolver.resolveDependency(BatchingNeoStores.class));
        } else if (execution.getStageName().equals(DataImporter.RELATIONSHIP_IMPORT_NAME)) {
            endPrevious();

            // Import relationships:
            // - import relationships
            initializeRelationshipImport(
                    dependencyResolver.resolveDependency(Input.Estimates.class),
                    dependencyResolver.resolveDependency(IdMapper.class),
                    dependencyResolver.resolveDependency(BatchingNeoStores.class));
        } else if (execution.getStageName().equals(NodeDegreeCountStage.NAME)) {
            endPrevious();

            // Link relationships:
            // - read node degrees
            // - backward linking
            // - node relationship linking
            // - forward linking
            initializeLinking(
                    dependencyResolver.resolveDependency(BatchingNeoStores.class),
                    dependencyResolver.resolveDependency(DataStatistics.class));
        } else if (execution.getStageName().equals(CountGroupsStage.NAME)) {
            endPrevious();

            // Misc:
            // - relationship group defragmentation
            // - counts store
            initializeMisc(
                    dependencyResolver.resolveDependency(BatchingNeoStores.class),
                    dependencyResolver.resolveDependency(DataStatistics.class));
        } else if (includeStage(execution)) {
            lastReportedProgress = 0;
            progressListener.mark('-');
        }
    }

    private void endPrevious() {
        if (progressListener != null) {
            progressListener.close();
        }
        if (currentStage != null) {
            out.printf(
                    "%s COMPLETED in %s%n%n",
                    currentStage.description(), duration(currentTimeMillis() - stageStartTime));
        }
    }

    private void initializeNodeImport(Input.Estimates estimates, IdMapper idMapper, BatchingNeoStores neoStores) {
        long numberOfNodes = estimates.numberOfNodes();
        // A difficulty with the goal here is that we don't know how much work there is to be done in id mapper
        // preparation stage.
        // In addition to nodes themselves and SPLIT,SORT,DETECT there may be RESOLVE,SORT,DEDUPLICATE too, if there are
        // collisions
        long goal = idMapper.needsPreparation()
                ? numberOfNodes + weighted(IdMapperPreparationStage.NAME, numberOfNodes * 4)
                : numberOfNodes;

        startStage(
                ImportStage.nodeImport,
                goal,
                externalProgressNodeWeight,
                ESTIMATED_NUMBER_OF_NODES,
                count(numberOfNodes),
                ESTIMATED_DISK_SPACE_USAGE,
                bytesToString(
                        // node store
                        nodesDiskUsage(estimates, neoStores)
                                +
                                // property store(s)
                                estimates.sizeOfNodeProperties()),
                ESTIMATED_REQUIRED_MEMORY_USAGE,
                bytesToString(baselineMemoryRequirement(neoStores)
                        + defensivelyPadMemoryEstimate(idMapper.memoryEstimation(numberOfNodes))));
    }

    private void initializeRelationshipImport(
            Input.Estimates estimates, IdMapper idMapper, BatchingNeoStores neoStores) {
        long numberOfRelationships = estimates.numberOfRelationships();
        startStage(
                ImportStage.relationshipImport,
                numberOfRelationships,
                externalProgressRelationshipWeight,
                ESTIMATED_NUMBER_OF_RELATIONSHIPS,
                count(numberOfRelationships),
                ESTIMATED_DISK_SPACE_USAGE,
                bytesToString(relationshipsDiskUsage(estimates, neoStores) + estimates.sizeOfRelationshipProperties()),
                ESTIMATED_REQUIRED_MEMORY_USAGE,
                bytesToString(baselineMemoryRequirement(neoStores) + totalMemoryUsageOf(idMapper)));
    }

    private void initializeLinking(BatchingNeoStores neoStores, DataStatistics distribution) {
        // The reason the highId of the relationship store is used, as opposed to actual number of imported
        // relationships
        // is that the stages underneath operate on id ranges, not knowing which records are actually in use.
        long relationshipRecordIdCount =
                neoStores.getRelationshipStore().getIdGenerator().getHighId();
        // The progress counting of linking stages is special anyway, in that it uses the "progress" stats key,
        // which is based on actual number of relationships, not relationship ids.
        long actualRelationshipCount = distribution.getRelationshipCount();

        // node degrees
        // start/end forwards, see RelationshipLinkingProgress
        // start/end backwards, see RelationshipLinkingProgress
        long goal = relationshipRecordIdCount + actualRelationshipCount * 2 + actualRelationshipCount * 2;
        startStage(
                ImportStage.linking,
                goal,
                externalProgressRelationshipWeight,
                ESTIMATED_REQUIRED_MEMORY_USAGE,
                bytesToString(baselineMemoryRequirement(neoStores)
                        + defensivelyPadMemoryEstimate(
                                NodeRelationshipCache.memoryEstimation(distribution.getNodeCount()))));
    }

    private void initializeMisc(BatchingNeoStores neoStores, DataStatistics distribution) {
        long actualNodeCount = distribution.getNodeCount();
        // The reason the highId of the relationship store is used, as opposed to actual number of imported
        // relationships
        // is that the stages underneath operate on id ranges, not knowing which records are actually in use.
        var relStore = neoStores.getRelationshipStore();
        long relationshipRecordIdCount = relStore.getIdGenerator().getHighId();
        long groupCount =
                neoStores.getTemporaryRelationshipGroupStore().getIdGenerator().getHighId();
        // Count groups
        // Write groups
        // Node --> Group
        // Node counts
        // Relationship counts
        long goal = groupCount + groupCount + groupCount + actualNodeCount + relationshipRecordIdCount;
        startStage(
                ImportStage.postProcessing,
                goal,
                0.2,
                ESTIMATED_REQUIRED_MEMORY_USAGE,
                bytesToString(baselineMemoryRequirement(neoStores)));
    }

    private void updateProgress(long progress) {
        // OK so format goes something like 5 groups of 10 dots per line, which is 5%, i.e. 50 dots for 5%, i.e. 1000
        // dots for 100%,
        // i.e. granularity is 1/1000
        long diff = progress - lastReportedProgress;
        progressListener.add(diff);
        lastReportedProgress = progress;
    }

    private void printPageCacheAllocationWarningIfUsed() {
        String allocation = pageCacheArrayFactoryMonitor.pageCacheAllocationOrNull();
        if (allocation != null) {
            err.println();
            err.println("WARNING:");
            err.println(allocation);
        }
    }

    private void startStage(ImportStage stage, long goal, double externalProgressWeight, Object... data) {
        stageStartTime = currentTimeMillis();
        currentStage = stage;
        progressListener = progressMonitorFactory.singlePart(
                stageHeader(stage.descriptionWithOrdinal(), data),
                goal,
                externalProgressIndicator.next(externalProgressWeight));
        currentStage = stage;
    }

    private String stageHeader(String description, Object... data) {
        var header = new StringBuilder(description).append(" ").append(localDate());
        if (data.length > 0) {
            for (int i = 0; i < data.length; ) {
                header.append(format("%n  %s: %s", data[i++], data[i++]));
            }
        }
        return header.toString();
    }

    @Override
    public void end(StageExecution execution, long totalTimeMillis) {}

    @Override
    public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
        endPrevious();
        externalProgressIndicator.close();

        out.println();
        out.printf(
                "IMPORT %s in %s. %s%n",
                successful ? "DONE" : "FAILED", duration(totalTimeMillis), additionalInformation);
    }

    @Override
    public long checkIntervalMillis() {
        return 200;
    }

    @Override
    public void check(StageExecution execution) {
        if (includeStage(execution)) {
            updateProgress(progressOf(execution));
        }
    }

    private static boolean includeStage(StageExecution execution) {
        String name = execution.getStageName();
        return !name.equals(RelationshipGroupStage.NAME)
                && !name.equals(SparseNodeFirstRelationshipStage.NAME)
                && !name.equals(ScanAndCacheGroupsStage.NAME);
    }

    private static double weightOf(String stageName) {
        if (stageName.equals(IdMapperPreparationStage.NAME)) {
            return 0.5D;
        }
        return 1;
    }

    private static long weighted(String stageName, long progress) {
        return (long) (progress * weightOf(stageName));
    }

    private static long progressOf(StageExecution execution) {
        // First see if there's a "progress" stat
        Stat progressStat = findProgressStat(execution.steps());
        if (progressStat != null) {
            return weighted(execution.getStageName(), progressStat.asLong());
        }

        // No, then do the generic progress calculation by looking at "done_batches"
        long doneBatches =
                last(execution.steps()).stats().stat(Keys.done_batches).asLong();
        int batchSize = execution.getConfig().batchSize();
        return weighted(execution.getStageName(), doneBatches * batchSize);
    }

    private static Stat findProgressStat(Iterable<Step<?>> steps) {
        for (Step<?> step : steps) {
            Stat stat = step.stats().stat(Keys.progress);
            if (stat != null) {
                return stat;
            }
        }
        return null;
    }
}
