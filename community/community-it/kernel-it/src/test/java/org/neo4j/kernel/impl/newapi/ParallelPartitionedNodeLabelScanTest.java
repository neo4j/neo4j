/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.test.Race;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@ExtendWith( SoftAssertionsExtension.class )
public class ParallelPartitionedNodeLabelScanTest extends KernelAPIReadTestBase<ReadTestSupport>
{
    // We need a lot of nodes for the underlying GBPTree to have multiple subtrees to create partitions over
    static final int NUMBER_OF_NODES = 100_000;
    // Maximum number of partitions that can be created over this data
    // This number has been discovered experimentally.
    static final int MAX_NUMBER_OF_PARTITIONS = 7;
    private static final Map<Label,Integer> LABEL_IDS = new HashMap<>();
    private static final Map<Label,Set<Long>> LABEL_NODES = Arrays.stream( Label.values() ).collect( Collectors.toMap( l -> l, l -> new HashSet<>() ) );

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        var labels = Label.values();
        try ( var tx = beginTransaction() )
        {
            var tokenWrite = tx.tokenWrite();
            for ( var label : labels )
            {
                LABEL_IDS.put( label, tokenWrite.labelGetOrCreateForName( label.toString() ) );
            }

            var write = tx.dataWrite();
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                var node = write.nodeCreate();
                var label = labels[i % labels.length];
                write.nodeAddLabel( node, LABEL_IDS.get( label ) );
                LABEL_NODES.get( label ).add( node );
            }
            tx.commit();
        }
        catch ( KernelException e )
        {
            throw new AssertionError( e );
        }
    }

    @Override
    public ReadTestSupport newTestSupport()
    {
        return new ReadTestSupport();
    }

    @Test
    void shouldScanSubsetOfEntriesWithSinglePartition()
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        try ( var nodes = cursors.allocateNodeLabelIndexCursor( NULL ) )
        {
            var session = nodeLabelIndexSession();
            for ( var label : Label.values() )
            {
                var scan = read.nodeLabelScan( session, new TokenPredicate( LABEL_IDS.get( label ) ), MAX_NUMBER_OF_PARTITIONS );
                softly.assertThat( scan.getNumberOfPartitions() ).as( "number of partitions" )
                      .isGreaterThan( 0 )
                      .isLessThanOrEqualTo( MAX_NUMBER_OF_PARTITIONS );

                var found = new HashSet<Long>();
                scan.reservePartition( nodes );

                while ( nodes.next() )
                {
                    softly.assertThat( found.add( nodes.nodeReference() ) ).as( "no duplicate" ).isTrue();
                }

                softly.assertThat( LABEL_NODES.get( label ) ).as( "all labels found, are '%s'", label ).containsAll( found );
            }
        }
    }

    @Test
    void shouldCreateNoMorePartitionsThanPossible()
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        singleThreadedCheck( 1 + MAX_NUMBER_OF_PARTITIONS );
    }

    @Test
    void shouldHandlePartitionsGreaterThanNumberOfNodes()
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        singleThreadedCheck( 2 * NUMBER_OF_NODES );
    }

    @ParameterizedTest( name = "numberOfPartitions={0}" )
    @MethodSource( "rangeFromOneToMaxPartitions" )
    void shouldScanAllEntriesWithGivenNumberOfPartitionsSingleThreaded( int desiredNumberOfPartitions )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        singleThreadedCheck( desiredNumberOfPartitions );
    }

    @ParameterizedTest
    @ValueSource( ints = {-1, 0} )
    void shouldThrowOnNonPositivePartitions( int desiredNumberOfPartitions ) throws IndexNotFoundKernelException
    {
        var session = nodeLabelIndexSession();
        for ( var label : Label.values() )
        {
            assertThatExceptionOfType( IllegalArgumentException.class )
                    .as( "desired number of partitions must be positive" )
                    .isThrownBy( () -> read.nodeLabelScan( session, new TokenPredicate( LABEL_IDS.get( label ) ), desiredNumberOfPartitions ) )
                    .withMessageContainingAll( "Expected positive", "value" );
        }
    }

    @ParameterizedTest( name = "numberOfPartitions={0}" )
    @MethodSource( "rangeFromOneToMaxPartitions" )
    void shouldScanMultiplePartitionsInParallelWithSameNumberOfThreads( int desiredNumberOfPartitions )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        multiThreadedCheck( desiredNumberOfPartitions, desiredNumberOfPartitions );
    }

    @ParameterizedTest( name = "numberOfThreads={0}" )
    @MethodSource( "rangeFromOneToMaxPartitions" )
    void shouldScanMultiplePartitionsInParallelWithFewerThreads( int numberOfThreads )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        multiThreadedCheck( MAX_NUMBER_OF_PARTITIONS, numberOfThreads );
    }

    private TokenReadSession nodeLabelIndexSession() throws IndexNotFoundKernelException
    {
        var indexes = schemaRead.index( SchemaDescriptors.forAnyEntityTokens( EntityType.NODE ) );
        assertThat( indexes.hasNext() ).as( "NLI exists" ).isTrue();
        var nli = indexes.next();
        softly.assertThat( indexes.hasNext() ).as( "only one NLI exists" ).isFalse();
        return read.tokenReadSession( nli );
    }

    private void singleThreadedCheck( int desiredNumberOfPartitions )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        try ( var nodes = cursors.allocateNodeLabelIndexCursor( NULL ) )
        {
            var session = nodeLabelIndexSession();
            for ( var label : Label.values() )
            {
                var scan = read.nodeLabelScan( session, new TokenPredicate( LABEL_IDS.get( label ) ), desiredNumberOfPartitions );
                softly.assertThat( scan.getNumberOfPartitions() ).as( "number of partitions" )
                      .isGreaterThan( 0 )
                      .isLessThanOrEqualTo( desiredNumberOfPartitions );

                var found = new HashSet<Long>();
                while ( scan.reservePartition( nodes ) )
                {
                    while ( nodes.next() )
                    {
                        softly.assertThat( found.add( nodes.nodeReference() ) ).as( "no duplicate" ).isTrue();
                    }
                }

                softly.assertThat( found ).as( "all '%s' labels found, and no other", label )
                      .containsExactlyInAnyOrderElementsOf( LABEL_NODES.get( label ) );
            }
        }
    }

    void multiThreadedCheck( int desiredNumberOfPartitions, int numberOfThreads )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        var session = nodeLabelIndexSession();
        for ( var label : Label.values() )
        {
            var scan = read.nodeLabelScan( session, new TokenPredicate( LABEL_IDS.get( label ) ), desiredNumberOfPartitions );
            softly.assertThat( scan.getNumberOfPartitions() ).as( "number of partitions" )
                  .isGreaterThan( 0 )
                  .isLessThanOrEqualTo( desiredNumberOfPartitions );

            var allFound = Collections.synchronizedSet( new HashSet<Long>() );
            var race = new Race();
            for ( int i = 0; i < numberOfThreads; i++ )
            {
                var nodes = cursors.allocateNodeLabelIndexCursor( NULL );
                race.addContestant( () ->
                {
                    try
                    {
                        var found = new HashSet<Long>();
                        while ( scan.reservePartition( nodes ) )
                        {
                            while ( nodes.next() )
                            {
                                softly.assertThat( found.add( nodes.nodeReference() ) ).as( "no duplicate" ).isTrue();
                            }
                        }
                        found.forEach( l -> softly.assertThat( allFound.add( l ) ).as( "no duplicates" ).isTrue() );
                    }
                    finally
                    {
                        nodes.close();
                    }
                } );
            }
            race.goUnchecked();
            softly.assertThat( allFound ).as( "all '%s' labels found, and no other", label )
                  .containsExactlyInAnyOrderElementsOf( LABEL_NODES.get( label ) );
        }
    }

    private static IntStream rangeFromOneToMaxPartitions()
    {
        return IntStream.rangeClosed( 1, MAX_NUMBER_OF_PARTITIONS );
    }

    private enum Label implements org.neo4j.graphdb.Label
    {
        FOO,
        BAR,
        BAZ,
    }
}
