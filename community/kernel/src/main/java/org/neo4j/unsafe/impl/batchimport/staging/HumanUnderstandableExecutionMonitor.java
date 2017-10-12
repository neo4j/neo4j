/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.PrintStream;
import java.util.TimeZone;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.unsafe.impl.batchimport.CountGroupsStage;
import org.neo4j.unsafe.impl.batchimport.IdMapperPreparationStage;
import org.neo4j.unsafe.impl.batchimport.NodeDegreeCountStage;
import org.neo4j.unsafe.impl.batchimport.NodeFirstGroupStage;
import org.neo4j.unsafe.impl.batchimport.NodeStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipGroupStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipStage;
import org.neo4j.unsafe.impl.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.unsafe.impl.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static java.lang.Integer.min;
import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.count;
import static org.neo4j.helpers.Format.date;
import static org.neo4j.helpers.collection.Iterables.last;

/**
 * Prints progress you can actually understand, with capabilities to on demand print completely incomprehensible
 * details only understandable to a select few.
 */
public class HumanUnderstandableExecutionMonitor implements ExecutionMonitor
{
    private static final int DOT_GROUP_SIZE = 10;
    private static final int DOT_GROUPS_PER_LINE = 5;
    private static final int PERCENTAGES_PER_LINE = 5;

    // assigned later on
    private final PrintStream out;
    private DependencyResolver dependencyResolver;
    private long actualNodeCount;
    private long actualRelationshipCount;

    // progress of current stage
    private long goal;
    private long stashedProgress;
    private long progress;

    public HumanUnderstandableExecutionMonitor( PrintStream out )
    {
        this.out = out;
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public void start( StageExecution execution )
    {
        // Divide into 4 progress stages:
        if ( execution.getStageName().equals( NodeStage.NAME ) )
        {
            // Import nodes:
            // - import nodes
            // - prepare id mapper
            initializeNodeImport(
                    dependencyResolver.resolveDependency( Input.Estimates.class ),
                    dependencyResolver.resolveDependency( IdMapper.class ) );
        }
        else if ( execution.getStageName().equals( RelationshipStage.NAME ) )
        {
            endPrevious();

            // Import relationships:
            // - import relationships
            initializeRelationshipImport( dependencyResolver.resolveDependency( Input.Estimates.class ) );
        }
        else if ( execution.getStageName().equals( NodeDegreeCountStage.NAME ) )
        {
            endPrevious();

            // Link relationships:
            // - read node degrees
            // - backward linking
            // - node relationship linking
            // - forward linking
            initializeLinking();
        }
        else if ( execution.getStageName().equals( CountGroupsStage.NAME ) )
        {
            endPrevious();

            // Misc:
            // - relationship group defragmentation
            // - counts store
            initializeMisc( dependencyResolver.resolveDependency( NeoStores.class ) );
        }
        else if ( includeStage( execution ) )
        {
            stashedProgress += progress;
            progress = 0;
        }
    }

    private void endPrevious()
    {
        updateProgress( goal ); // previous ended
        // TODO print some end stats for this stage?
    }

    private void initializeNodeImport( Estimates estimates, IdMapper idMapper )
    {
        // TODO with the estimates... should there be a way of continuously re-estimating those,
        //      or will Estimates actually do that internally?
        // Let's say that import will stands for 70% of the dots,
        // preparing the ID mapper 30% (depending on the amount of collisions).
        long numberOfNodes = estimates.numberOfNodes();
        // TODO how to handle UNKNOWN?
        printStageHeader( "(1/4) Node import",
                "number of nodes", count( numberOfNodes ),
                // TODO disk usage
                "memory", "+" + bytes( padIdMapperMemoryEstimate( idMapper.calculateMemoryUsage( numberOfNodes ) ) ) );

        // A difficulty with the goal here is that we don't know how much woek there is to be done in id mapper preparation stage.
        // In addition to nodes themselves and SPLIT,SORT,DETECT there may be RESOLVE,SORT,DEDUPLICATE too, if there are collisions
        // TODO have some way of weighting progress of some stages vs. others, like ID mapper preparation stages could weight less than node stage
        long goal = idMapper.needsPreparation()
                ? (long) (numberOfNodes + weighted( IdMapperPreparationStage.NAME, numberOfNodes * 4 ))
                : numberOfNodes;
        initializeProgress( goal );
    }

    private void initializeRelationshipImport( Estimates estimates )
    {
        long numberOfRelationships = estimates.numberOfRelationships();
        // TODO how to handle UNKNOWN?
        printStageHeader( "(2/4) Relationship import",
                // TODO disk usage
                "number of relationships", count( numberOfRelationships ) );
        initializeProgress( numberOfRelationships );
    }

    private void initializeLinking()
    {
        printStageHeader( "(3/4) Relationship linking" );
        initializeProgress( actualRelationshipCount * 3 ); // node degrees + forwards and backwards, ignore the other stages
    }

    private void initializeMisc( NeoStores stores )
    {
        printStageHeader( "(4/4) Post processing" );
        // written groups + node counts + relationship counts
        initializeProgress(
                stores.getRelationshipGroupStore().getHighId() +
                actualNodeCount +
                actualRelationshipCount );
    }

    private static long padIdMapperMemoryEstimate( long calculateMemoryUsage )
    {
        // TODO this estimate is w/o collisions, add 10% to be defensive?
        return (long) (calculateMemoryUsage * 1.1);
    }

    private void initializeProgress( long goal )
    {
        this.goal = goal;
        this.stashedProgress = 0;
        this.progress = 0;
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
            if ( currentLine < line )
            {
                currentDotOnLine = printDots( currentDotOnLine, dotsPerLine() );
            }
            else
            {
                currentDotOnLine = printDots( currentDotOnLine, dotOnLine );
            }

            if ( currentLine < line || currentDotOnLine == dotsPerLine() )
            {
                out.println( format( " %s", linePercentage( currentLine ) ) );
                currentLine++;
                if ( currentLine == lines() )
                {
                    out.println();
                }
                currentDotOnLine = 0;
            }
        }

        // TODO not quite right
        this.progress = max( this.progress, progress );
    }

    private static String linePercentage( int line )
    {
        int percentage = (line + 1) * PERCENTAGES_PER_LINE;
        return percentage + "%";
    }

    private int printDots( int current, int target )
    {
        while ( current < target )
        {
            if ( current > 0 && current % DOT_GROUP_SIZE == 0 )
            {
                out.print( " " );
            }
            out.print( "." );
            current++;
        }
        return current;
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
        out.println( name + " " + date( TimeZone.getDefault() ) );
        if ( data.length > 0 )
        {
            out.println( "Estimates:" );
            for ( int i = 0; i < data.length; )
            {
                out.println( "  " + data[i++] + ": " + data[i++] );
            }
        }
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        if ( execution.getStageName().equals( NodeStage.NAME ) )
        {
            actualNodeCount = progressOf( execution );
        }
        else if ( execution.getStageName().equals( RelationshipStage.NAME ) )
        {
            actualRelationshipCount = progressOf( execution );
        }
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        // TODO print something profound
        endPrevious();
    }

    @Override
    public long nextCheckTime()
    {
        return currentTimeMillis() + 200;
    }

    @Override
    public void check( StageExecution execution )
    {
        if ( includeStage( execution ) )
        {
            updateProgress( progressOf( execution ) );
        }
    }

    private static boolean includeStage( StageExecution execution )
    {
        return !execution.getStageName().equals( RelationshipGroupStage.NAME ) &&
               !execution.getStageName().equals( SparseNodeFirstRelationshipStage.NAME ) &&
               !execution.getStageName().equals( CountGroupsStage.NAME ) &&
               !execution.getStageName().equals( ScanAndCacheGroupsStage.NAME ) &&
               !execution.getStageName().equals( NodeFirstGroupStage.NAME );
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
        Step<?> last = last( execution.steps() );
        long doneBatches = last.stats().stat( Keys.done_batches ).asLong();
        int batchSize = execution.getConfig().batchSize();
        long progress = weighted( execution.getStageName(), doneBatches * batchSize );
        return progress;
    }
}
