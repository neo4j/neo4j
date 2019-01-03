/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.TimeZone;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.unsafe.impl.batchimport.CountGroupsStage;
import org.neo4j.unsafe.impl.batchimport.DataImporter;
import org.neo4j.unsafe.impl.batchimport.DataStatistics;
import org.neo4j.unsafe.impl.batchimport.IdMapperPreparationStage;
import org.neo4j.unsafe.impl.batchimport.NodeDegreeCountStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipGroupStage;
import org.neo4j.unsafe.impl.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.unsafe.impl.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.PageCacheArrayFactoryMonitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static java.lang.Integer.min;
import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.count;
import static org.neo4j.helpers.Format.date;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.collection.Iterables.last;
import static org.neo4j.unsafe.impl.batchimport.ImportMemoryCalculator.defensivelyPadMemoryEstimate;
import static org.neo4j.unsafe.impl.batchimport.ImportMemoryCalculator.estimatedCacheSize;
import static org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor.totalMemoryUsageOf;

/**
 * Prints progress you can actually understand, with capabilities to on demand print completely incomprehensible
 * details only understandable to a select few.
 */
public class HumanUnderstandableExecutionMonitor implements ExecutionMonitor
{
    public interface Monitor
    {
        void progress( ImportStage stage, int percent );
    }

    public static final Monitor NO_MONITOR = ( stage, percent ) -> {};

    public interface ExternalMonitor
    {
        boolean somethingElseBrokeMyNiceOutput();
    }

    static final ExternalMonitor NO_EXTERNAL_MONITOR = () -> false;

    enum ImportStage
    {
        nodeImport,
        relationshipImport,
        linking,
        postProcessing
    }

    private static final String ESTIMATED_REQUIRED_MEMORY_USAGE = "Estimated required memory usage";
    private static final String ESTIMATED_DISK_SPACE_USAGE = "Estimated disk space usage";
    private static final String ESTIMATED_NUMBER_OF_RELATIONSHIP_PROPERTIES = "Estimated number of relationship properties";
    private static final String ESTIMATED_NUMBER_OF_RELATIONSHIPS = "Estimated number of relationships";
    private static final String ESTIMATED_NUMBER_OF_NODE_PROPERTIES = "Estimated number of node properties";
    private static final String ESTIMATED_NUMBER_OF_NODES = "Estimated number of nodes";
    private static final int DOT_GROUP_SIZE = 10;
    private static final int DOT_GROUPS_PER_LINE = 5;
    private static final int PERCENTAGES_PER_LINE = 5;

    private final Monitor monitor;
    private final ExternalMonitor externalMonitor;
    private DependencyResolver dependencyResolver;
    private boolean newInternalStage;
    private PageCacheArrayFactoryMonitor pageCacheArrayFactoryMonitor;

    // progress of current stage
    private long goal;
    private long stashedProgress;
    private long progress;
    private ImportStage currentStage;
    private long lastReportTime;

    HumanUnderstandableExecutionMonitor( Monitor monitor, ExternalMonitor externalMonitor )
    {
        this.monitor = monitor;
        this.externalMonitor = externalMonitor;
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        this.dependencyResolver = dependencyResolver;
        Estimates estimates = dependencyResolver.resolveDependency( Estimates.class );
        BatchingNeoStores neoStores = dependencyResolver.resolveDependency( BatchingNeoStores.class );
        IdMapper idMapper = dependencyResolver.resolveDependency( IdMapper.class );
        NodeRelationshipCache nodeRelationshipCache = dependencyResolver.resolveDependency( NodeRelationshipCache.class );
        pageCacheArrayFactoryMonitor = dependencyResolver.resolveDependency( PageCacheArrayFactoryMonitor.class );

        long biggestCacheMemory = estimatedCacheSize( neoStores,
                nodeRelationshipCache.memoryEstimation( estimates.numberOfNodes() ),
                idMapper.memoryEstimation( estimates.numberOfNodes() ) );
        printStageHeader( "Import starting",
                ESTIMATED_NUMBER_OF_NODES, count( estimates.numberOfNodes() ),
                ESTIMATED_NUMBER_OF_NODE_PROPERTIES, count( estimates.numberOfNodeProperties() ),
                ESTIMATED_NUMBER_OF_RELATIONSHIPS, count( estimates.numberOfRelationships() ),
                ESTIMATED_NUMBER_OF_RELATIONSHIP_PROPERTIES, count( estimates.numberOfRelationshipProperties() ),
                ESTIMATED_DISK_SPACE_USAGE, bytes(
                        nodesDiskUsage( estimates, neoStores ) +
                        relationshipsDiskUsage( estimates, neoStores ) +
                        estimates.sizeOfNodeProperties() + estimates.sizeOfRelationshipProperties() ),
                ESTIMATED_REQUIRED_MEMORY_USAGE, bytes( biggestCacheMemory ) );
        System.out.println();
    }

    private static long baselineMemoryRequirement( BatchingNeoStores neoStores )
    {
        return totalMemoryUsageOf( neoStores );
    }

    private static long nodesDiskUsage( Estimates estimates, BatchingNeoStores neoStores )
    {
        return  // node store
                estimates.numberOfNodes() * neoStores.getNodeStore().getRecordSize() +
                // label index (1 byte per label is not a terrible estimate)
                estimates.numberOfNodeLabels();
    }

    private static long relationshipsDiskUsage( Estimates estimates, BatchingNeoStores neoStores )
    {
        return estimates.numberOfRelationships() * neoStores.getRelationshipStore().getRecordSize() *
                (neoStores.usesDoubleRelationshipRecordUnits() ? 2 : 1);
    }

    @Override
    public void start( StageExecution execution )
    {
        // Divide into 4 progress stages:
        if ( execution.getStageName().equals( DataImporter.NODE_IMPORT_NAME ) )
        {
            // Import nodes:
            // - import nodes
            // - prepare id mapper
            initializeNodeImport(
                    dependencyResolver.resolveDependency( Input.Estimates.class ),
                    dependencyResolver.resolveDependency( IdMapper.class ),
                    dependencyResolver.resolveDependency( BatchingNeoStores.class ) );
        }
        else if ( execution.getStageName().equals( DataImporter.RELATIONSHIP_IMPORT_NAME ) )
        {
            endPrevious();

            // Import relationships:
            // - import relationships
            initializeRelationshipImport(
                    dependencyResolver.resolveDependency( Input.Estimates.class ),
                    dependencyResolver.resolveDependency( IdMapper.class ),
                    dependencyResolver.resolveDependency( BatchingNeoStores.class ) );
        }
        else if ( execution.getStageName().equals( NodeDegreeCountStage.NAME ) )
        {
            endPrevious();

            // Link relationships:
            // - read node degrees
            // - backward linking
            // - node relationship linking
            // - forward linking
            initializeLinking(
                    dependencyResolver.resolveDependency( BatchingNeoStores.class ),
                    dependencyResolver.resolveDependency( NodeRelationshipCache.class ),
                    dependencyResolver.resolveDependency( DataStatistics.class ) );
        }
        else if ( execution.getStageName().equals( CountGroupsStage.NAME ) )
        {
            endPrevious();

            // Misc:
            // - relationship group defragmentation
            // - counts store
            initializeMisc(
                    dependencyResolver.resolveDependency( BatchingNeoStores.class ),
                    dependencyResolver.resolveDependency( DataStatistics.class ) );
        }
        else if ( includeStage( execution ) )
        {
            stashedProgress += progress;
            progress = 0;
            newInternalStage = true;
        }
        lastReportTime = currentTimeMillis();
    }

    private void endPrevious()
    {
        updateProgress( goal );
    }

    private void initializeNodeImport( Estimates estimates, IdMapper idMapper, BatchingNeoStores neoStores )
    {
        long numberOfNodes = estimates.numberOfNodes();
        printStageHeader( "(1/4) Node import",
                ESTIMATED_NUMBER_OF_NODES, count( numberOfNodes ),
                ESTIMATED_DISK_SPACE_USAGE, bytes(
                        // node store
                        nodesDiskUsage( estimates, neoStores ) +
                        // property store(s)
                        estimates.sizeOfNodeProperties() ),
                ESTIMATED_REQUIRED_MEMORY_USAGE, bytes(
                        baselineMemoryRequirement( neoStores ) +
                        defensivelyPadMemoryEstimate( idMapper.memoryEstimation( numberOfNodes ) ) ) );

        // A difficulty with the goal here is that we don't know how much woek there is to be done in id mapper preparation stage.
        // In addition to nodes themselves and SPLIT,SORT,DETECT there may be RESOLVE,SORT,DEDUPLICATE too, if there are collisions
        long goal = idMapper.needsPreparation()
                ? numberOfNodes + weighted( IdMapperPreparationStage.NAME, numberOfNodes * 4 )
                : numberOfNodes;
        initializeProgress( goal, ImportStage.nodeImport );
    }

    private void initializeRelationshipImport( Estimates estimates, IdMapper idMapper, BatchingNeoStores neoStores )
    {
        long numberOfRelationships = estimates.numberOfRelationships();
        printStageHeader( "(2/4) Relationship import",
                ESTIMATED_NUMBER_OF_RELATIONSHIPS, count( numberOfRelationships ),
                ESTIMATED_DISK_SPACE_USAGE, bytes(
                        relationshipsDiskUsage( estimates, neoStores ) +
                        estimates.sizeOfRelationshipProperties() ),
                ESTIMATED_REQUIRED_MEMORY_USAGE, bytes(
                        baselineMemoryRequirement( neoStores ) +
                        totalMemoryUsageOf( idMapper ) ) );
        initializeProgress( numberOfRelationships, ImportStage.relationshipImport );
    }

    private void initializeLinking( BatchingNeoStores neoStores,
            NodeRelationshipCache nodeRelationshipCache, DataStatistics distribution )
    {
        printStageHeader( "(3/4) Relationship linking",
                ESTIMATED_REQUIRED_MEMORY_USAGE, bytes(
                        baselineMemoryRequirement( neoStores ) +
                        defensivelyPadMemoryEstimate( nodeRelationshipCache.memoryEstimation( distribution.getNodeCount() ) ) ) );
        // The reason the highId of the relationship store is used, as opposed to actual number of imported relationships
        // is that the stages underneath operate on id ranges, not knowing which records are actually in use.
        long relationshipRecordIdCount = neoStores.getRelationshipStore().getHighId();
        // The progress counting of linking stages is special anyway, in that it uses the "progress" stats key,
        // which is based on actual number of relationships, not relationship ids.
        long actualRelationshipCount = distribution.getRelationshipCount();
        initializeProgress(
                relationshipRecordIdCount +   // node degrees
                actualRelationshipCount * 2 + // start/end forwards, see RelationshipLinkingProgress
                actualRelationshipCount * 2,  // start/end backwards, see RelationshipLinkingProgress
                ImportStage.linking
                );
    }

    private void initializeMisc( BatchingNeoStores neoStores, DataStatistics distribution )
    {
        printStageHeader( "(4/4) Post processing",
                ESTIMATED_REQUIRED_MEMORY_USAGE, bytes( baselineMemoryRequirement( neoStores ) ) );
        long actualNodeCount = distribution.getNodeCount();
        // The reason the highId of the relationship store is used, as opposed to actual number of imported relationships
        // is that the stages underneath operate on id ranges, not knowing which records are actually in use.
        long relationshipRecordIdCount = neoStores.getRelationshipStore().getHighId();
        long groupCount = neoStores.getTemporaryRelationshipGroupStore().getHighId();
        initializeProgress(
                groupCount +                 // Count groups
                groupCount +                 // Write groups
                groupCount +                 // Node --> Group
                actualNodeCount +            // Node counts
                relationshipRecordIdCount,   // Relationship counts
                ImportStage.postProcessing
                );
    }

    private void initializeProgress( long goal, ImportStage stage )
    {
        this.goal = goal;
        this.stashedProgress = 0;
        this.progress = 0;
        this.currentStage = stage;
        this.newInternalStage = false;
    }

    private void updateProgress( long progress )
    {
        // OK so format goes something like 5 groups of 10 dots per line, which is 5%, i.e. 50 dots for 5%, i.e. 1000 dots for 100%,
        // i.e. granularity is 1/1000

        int maxDot = dotOf( goal );
        int currentProgressDot = dotOf( stashedProgress + this.progress );
        int currentLine = currentProgressDot / dotsPerLine();
        int currentDotOnLine = currentProgressDot % dotsPerLine();

        int progressDot = min( maxDot, dotOf( stashedProgress + progress ) );
        int line = progressDot / dotsPerLine();
        int dotOnLine = progressDot % dotsPerLine();

        while ( currentLine < line || (currentLine == line && currentDotOnLine < dotOnLine) )
        {
            int target = currentLine < line ? dotsPerLine() : dotOnLine;
            printDots( currentDotOnLine, target );
            currentDotOnLine = target;

            if ( currentLine < line || currentDotOnLine == dotsPerLine() )
            {
                int percentage = percentage( currentLine );
                System.out.println( format( "%4d%% ∆%s", percentage, durationSinceLastReport() ) );
                monitor.progress( currentStage, percentage );
                currentLine++;
                if ( currentLine == lines() )
                {
                    System.out.println();
                }
                currentDotOnLine = 0;
            }
        }

        this.progress = max( this.progress, progress );
    }

    private String durationSinceLastReport()
    {
        long diff = currentTimeMillis() - lastReportTime;
        lastReportTime = currentTimeMillis();
        return duration( diff );
    }

    private static int percentage( int line )
    {
        return (line + 1) * PERCENTAGES_PER_LINE;
    }

    private void printDots( int from, int target )
    {
        int current = from;
        while ( current < target )
        {
            if ( current > 0 && current % DOT_GROUP_SIZE == 0 )
            {
                System.out.print( ' ' );
            }
            char dotChar = '.';
            if ( newInternalStage )
            {
                newInternalStage = false;
                dotChar = '-';
            }
            System.out.print( dotChar );
            current++;

            printPageCacheAllocationWarningIfUsed();
        }
    }

    private void printPageCacheAllocationWarningIfUsed()
    {
        String allocation = pageCacheArrayFactoryMonitor.pageCacheAllocationOrNull();
        if ( allocation != null )
        {
            System.err.println();
            System.err.println( "WARNING:" );
            System.err.println( allocation );
        }
    }

    private int dotOf( long progress )
    {
        // calculated here just to reduce amount of state kept in this instance
        int dots = dotsPerLine() * lines();
        double dotSize = goal / (double) dots;

        return (int) (progress / dotSize);
    }

    private static int lines()
    {
        return 100 / PERCENTAGES_PER_LINE;
    }

    private static int dotsPerLine()
    {
        return DOT_GROUPS_PER_LINE * DOT_GROUP_SIZE;
    }

    private void printStageHeader( String name, Object... data )
    {
        System.out.println( name + " " + date( TimeZone.getDefault() ) );
        if ( data.length > 0 )
        {
            for ( int i = 0; i < data.length; )
            {
                System.out.println( "  " + data[i++] + ": " + data[i++] );
            }
        }
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
    }

    @Override
    public void done( boolean successful, long totalTimeMillis, String additionalInformation )
    {
        endPrevious();

        System.out.println();
        System.out.println( format( "IMPORT %s in %s. %s", successful ? "DONE" : "FAILED", duration( totalTimeMillis ), additionalInformation ) );
    }

    @Override
    public long nextCheckTime()
    {
        return currentTimeMillis() + 200;
    }

    @Override
    public void check( StageExecution execution )
    {
        reprintProgressIfNecessary();
        if ( includeStage( execution ) )
        {
            updateProgress( progressOf( execution ) );
        }
    }

    private void reprintProgressIfNecessary()
    {
        if ( externalMonitor.somethingElseBrokeMyNiceOutput() )
        {
            long prevProgress = this.progress;
            long prevStashedProgress = this.stashedProgress;
            this.progress = 0;
            this.stashedProgress = 0;
            updateProgress( prevProgress + prevStashedProgress );
            this.progress = prevProgress;
            this.stashedProgress = prevStashedProgress;
        }
    }

    private static boolean includeStage( StageExecution execution )
    {
        String name = execution.getStageName();
        return !name.equals( RelationshipGroupStage.NAME ) &&
               !name.equals( SparseNodeFirstRelationshipStage.NAME ) &&
               !name.equals( ScanAndCacheGroupsStage.NAME );
    }

    private static double weightOf( String stageName )
    {
        if ( stageName.equals( IdMapperPreparationStage.NAME ) )
        {
            return 0.5D;
        }
        return 1;
    }

    private static long weighted( String stageName, long progress )
    {
        return (long) (progress * weightOf( stageName ));
    }

    private static long progressOf( StageExecution execution )
    {
        // First see if there's a "progress" stat
        Stat progressStat = findProgressStat( execution.steps() );
        if ( progressStat != null )
        {
            return weighted( execution.getStageName(), progressStat.asLong() );
        }

        // No, then do the generic progress calculation by looking at "done_batches"
        long doneBatches = last( execution.steps() ).stats().stat( Keys.done_batches ).asLong();
        int batchSize = execution.getConfig().batchSize();
        return weighted( execution.getStageName(), doneBatches * batchSize );
    }

    private static Stat findProgressStat( Iterable<Step<?>> steps )
    {
        for ( Step<?> step : steps )
        {
            Stat stat = step.stats().stat( Keys.progress );
            if ( stat != null )
            {
                return stat;
            }
        }
        return null;
    }
}
