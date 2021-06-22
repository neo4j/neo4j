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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.PartitionedScanFactory;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.ScanQuery;
import org.neo4j.test.Race;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith( {SoftAssertionsExtension.class, RandomExtension.class} )
@ImpermanentDbmsExtension
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
abstract class PartitionedScanTestSuite<SCAN_QUERY extends ScanQuery<?>, CURSOR extends Cursor>
{
    @Inject
    private GraphDatabaseService db;
    @Inject
    protected RandomRule random;

    @InjectSoftAssertions
    protected SoftAssertions softly;

    abstract EntityIdsMatchingScanQuery<SCAN_QUERY> setupDatabase();

    protected EntityIdsMatchingScanQuery<SCAN_QUERY> entityIdsMatchingScanQuery;
    protected int maxNumberOfPartitions;
    protected PartitionedScanFactory<SCAN_QUERY,CURSOR> factory;

    PartitionedScanTestSuite( TestSuite<SCAN_QUERY,CURSOR> testSuite )
    {
        factory = testSuite.getFactory();
    }

    @BeforeAll
    protected void setup()
    {
        entityIdsMatchingScanQuery = setupDatabase();
        maxNumberOfPartitions = calculateMaxNumberOfPartitions( entityIdsMatchingScanQuery.scanQueries() );
    }

    protected final KernelTransaction beginTx()
    {
        return ((TransactionImpl) db.beginTx()).kernelTransaction();
    }

    @ParameterizedTest
    @ValueSource( ints = {-1, 0} )
    final void shouldThrowOnNonPositivePartitions( int desiredNumberOfPartitions ) throws KernelException
    {
        try ( var tx = beginTx() )
        {
            final var query = entityIdsMatchingScanQuery.iterator().next().getKey();

            // given  an invalid desiredNumberOfPartitions
            // when   partition scan constructed
            // then   IllegalArgumentException should be thrown
            softly.assertThatThrownBy( () -> factory.partitionedScan( tx, query, desiredNumberOfPartitions ),
                                       "desired number of partitions must be positive" )
                  .isInstanceOf( IllegalArgumentException.class )
                  .hasMessageContainingAll( "Expected positive", "value" );
        }
    }

    @Test
    final void shouldThrowOnConstructionWithTransactionState() throws KernelException
    {
        try ( var tx = beginTx() )
        {
            // given  transaction state
            createState( tx );
            softly.assertThat( tx.dataRead().transactionStateHasChanges() ).as( "transaction state" ).isTrue();

            final var query = entityIdsMatchingScanQuery.iterator().next().getKey();

            // when   partitioned scan constructed
            // then   IllegalStateException should be thrown
            softly.assertThatThrownBy( () -> factory.partitionedScan( tx, query, Integer.MAX_VALUE ),
                                       "should throw on construction of scan, with transaction state" )
                  .isInstanceOf( IllegalStateException.class )
                  .hasMessage( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
        }
    }

    @Test
    final void shouldThrowOnInitialReserveWithTransactionStateAfterConstruction() throws KernelException
    {
        try ( var tx = beginTx();
              var entities = factory.getCursor( tx ) )
        {
            final var query = entityIdsMatchingScanQuery.iterator().next().getKey();

            // given  no transaction state
            // when   partitioned scan constructed
            // then   should not throw
            final var scan = factory.partitionedScan( tx, query, Integer.MAX_VALUE );

            // given  transaction state
            createState( tx );
            softly.assertThat( tx.dataRead().transactionStateHasChanges() ).as( "transaction state" ).isTrue();

            // when   partitioned reserved
            // then   IllegalStateException should be thrown
            softly.assertThatThrownBy( () -> scan.reservePartition( entities ),
                                       "should throw on reserving partition, with transaction state" )
                  .isInstanceOf( IllegalStateException.class )
                  .hasMessage( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
        }
    }

    protected static void createState( KernelTransaction tx ) throws InvalidTransactionTypeKernelException
    {
        tx.dataWrite().nodeCreate();
    }

    abstract static class WithoutData<SCAN_QUERY extends ScanQuery<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite<SCAN_QUERY,CURSOR>
    {
        WithoutData( TestSuite<SCAN_QUERY,CURSOR> testSuite )
        {
            super( testSuite );
        }

        @Test
        final void shouldHandleEmptyDatabase() throws KernelException
        {
            try ( var tx = beginTx();
                  var entities = factory.getCursor( tx ) )
            {
                for ( var entry : entityIdsMatchingScanQuery )
                {
                    final var query = entry.getKey();
                    // given  an empty database
                    // when   scanning
                    final var scan = factory.partitionedScan( tx, query, Integer.MAX_VALUE );
                    while ( scan.reservePartition( entities ) )
                    {
                        // then   no data should be found, and should not throw
                        softly.assertThat( entities.next() ).as( "no data should be found for %s", query ).isFalse();
                    }
                }
            }
        }
    }

    abstract static class WithData<SCAN_QUERY extends ScanQuery<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite<SCAN_QUERY,CURSOR>
    {
        WithData( TestSuite<SCAN_QUERY,CURSOR> testSuite )
        {
            super( testSuite );
        }

        @Override
        @BeforeAll
        protected void setup()
        {
            // given  setting up the database
            // when   the maximum number of partitions is calculated
            super.setup();
            // then   there should be at least enough to test partitioning
            assertThat( maxNumberOfPartitions ).as( "max number of partitions is enough to test partitions" ).isGreaterThan( 1 );
        }

        @Test
        final void shouldThrowOnReserveWithLaterTransactionStateAfterConstruction() throws KernelException
        {
            try ( var tx = beginTx();
                  var entities = factory.getCursor( tx ) )
            {
                var query = entityIdsMatchingScanQuery.iterator().next().getKey();
                for ( var entry : entityIdsMatchingScanQuery )
                {
                    if ( entry.getValue().size() > entityIdsMatchingScanQuery.getOrCreate( query ).size() )
                    {
                        query = entry.getKey();
                    }
                }

                // given  no transaction state
                // when   partitioned scan constructed
                // then   should not throw
                final var scan = factory.partitionedScan( tx, query, maxNumberOfPartitions );

                // then   should have at least 2 partitions for test; if this fails, the tree isn't large enough
                softly.assertThat( scan.getNumberOfPartitions() ).as( "number of partitions" )
                      .isGreaterThanOrEqualTo( 2 );

                // when   partition reserved
                // then   should not throw
                softly.assertThatCode( () -> scan.reservePartition( entities ) )
                      .as( "should not throw on reserve partition, with no transaction state" )
                      .doesNotThrowAnyException();

                // given  transaction state
                createState( tx );
                softly.assertThat( tx.dataRead().transactionStateHasChanges() ).as( "transaction state" ).isTrue();

                // when   partition reserved
                // then   IllegalStateException should be thrown
                softly.assertThatThrownBy( () -> scan.reservePartition( entities ),
                                           "should throw on reserving partition, with transaction state" )
                      .isInstanceOf( IllegalStateException.class )
                      .hasMessage( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
            }
        }

        @Test
        final void shouldScanSubsetOfEntriesWithSinglePartition() throws KernelException
        {
            try ( var tx = beginTx();
                  var entities = factory.getCursor( tx ) )
            {
                for ( var entry : entityIdsMatchingScanQuery )
                {
                    final var query = entry.getKey();
                    final var expectedMatches = entry.getValue();

                    // given  a database with entries
                    // when   partitioning the scan
                    final var scan = factory.partitionedScan( tx, query, maxNumberOfPartitions );

                    // then   the number of partitions can be less, but no more than the max number of partitions
                    softly.assertThat( scan.getNumberOfPartitions() ).as( "number of partitions" )
                          .isGreaterThan( 0 )
                          .isLessThanOrEqualTo( maxNumberOfPartitions );

                    // given  a partition
                    final var found = new HashSet<Long>();
                    scan.reservePartition( entities );
                    while ( entities.next() )
                    {
                        // when   inspecting the found entities
                        // then   there should be no duplicates
                        softly.assertThat( found.add( factory.getEntityReference( entities ) ) ).as( "no duplicate" ).isTrue();
                    }

                    // then   the entities found should be a subset of all entities that would have matched that query
                    softly.assertThat( expectedMatches )
                          .as( "subset of all matches for %s", query )
                          .containsAll( found );
                }
            }
        }

        @Test
        final void shouldCreateNoMorePartitionsThanPossible() throws KernelException
        {
            singleThreadedCheck( Integer.MAX_VALUE );
        }

        @ParameterizedTest( name = "numberOfPartitions={0}" )
        @MethodSource( "rangeFromOneToMaxPartitions" )
        final void shouldScanAllEntriesWithGivenNumberOfPartitionsSingleThreaded( int desiredNumberOfPartitions ) throws KernelException
        {
            singleThreadedCheck( desiredNumberOfPartitions );
        }

        @ParameterizedTest( name = "numberOfPartitions={0}" )
        @MethodSource( "rangeFromOneToMaxPartitions" )
        final void shouldScanMultiplePartitionsInParallelWithSameNumberOfThreads( int desiredNumberOfPartitions ) throws KernelException
        {
            multiThreadedCheck( desiredNumberOfPartitions, desiredNumberOfPartitions );
        }

        @ParameterizedTest( name = "numberOfThreads={0}" )
        @MethodSource( "rangeFromOneToMaxPartitions" )
        final void shouldScanMultiplePartitionsInParallelWithFewerThreads( int numberOfThreads ) throws KernelException
        {
            multiThreadedCheck( maxNumberOfPartitions, numberOfThreads );
        }

        private void singleThreadedCheck( int desiredNumberOfPartitions ) throws KernelException
        {
            try ( var tx = beginTx();
                  var entities = factory.getCursor( tx ) )
            {
                for ( var entry : entityIdsMatchingScanQuery )
                {
                    final var query = entry.getKey();
                    final var expectedMatches = entry.getValue();

                    // given  a database with entries
                    // when   partitioning the scan
                    final var scan = factory.partitionedScan( tx, query, desiredNumberOfPartitions );

                    // then   the number of partitions can be less, but no more than the desired number of partitions
                    softly.assertThat( scan.getNumberOfPartitions() ).as( "number of partitions" )
                          .isGreaterThan( 0 )
                          .isLessThanOrEqualTo( desiredNumberOfPartitions )
                          .isLessThanOrEqualTo( maxNumberOfPartitions );

                    // given  each partition
                    final var found = new HashSet<Long>();
                    while ( scan.reservePartition( entities ) )
                    {
                        while ( entities.next() )
                        {
                            // when   inspecting the found entities
                            // then   there should be no duplicates
                            softly.assertThat( found.add( factory.getEntityReference( entities ) ) ).as( "no duplicate" ).isTrue();
                        }
                    }

                    // then   all the entities with matching the query should be found
                    softly.assertThat( found ).as( "only the expected data found matching %s", query )
                          .containsExactlyInAnyOrderElementsOf( expectedMatches );
                }
            }
        }

        private void multiThreadedCheck( int desiredNumberOfPartitions, int numberOfThreads ) throws KernelException
        {
            try ( var tx = beginTx() )
            {
                for ( var entry : entityIdsMatchingScanQuery )
                {
                    final var query = entry.getKey();
                    final var expectedMatches = entry.getValue();

                    // given  a database with entries
                    // when   partitioning the scan
                    final var scan = factory.partitionedScan( tx, query, desiredNumberOfPartitions );

                    // then   the number of partitions can be less, but no more than the desired number of partitions
                    softly.assertThat( scan.getNumberOfPartitions() ).as( "number of partitions" )
                          .isGreaterThan( 0 )
                          .isLessThanOrEqualTo( desiredNumberOfPartitions )
                          .isLessThanOrEqualTo( maxNumberOfPartitions );

                    // given  each partition distributed over multiple threads
                    final var allFound = Collections.synchronizedSet( new HashSet<Long>() );
                    final var race = new Race();
                    for ( int i = 0; i < numberOfThreads; i++ )
                    {
                        var entities = factory.getCursor( tx );
                        race.addContestant( () ->
                        {
                            try
                            {
                                final var found = new HashSet<Long>();
                                while ( scan.reservePartition( entities ) )
                                {
                                    while ( entities.next() )
                                    {
                                        // when   inspecting the found entities
                                        // then   there should be no duplicates within the partition
                                        softly.assertThat( found.add( factory.getEntityReference( entities ) ) )
                                              .as( "no duplicate" )
                                              .isTrue();
                                    }
                                }

                                // then   there should be no duplicates amongst any of the partitions
                                found.forEach( s -> softly.assertThat( allFound.add( s ) ).as( "no duplicates" ).isTrue() );
                            }
                            finally
                            {
                                entities.close();
                            }
                        } );
                    }
                    race.goUnchecked();

                    // then   all the entities with matching the query should be found
                    softly.assertThat( allFound ).as( "only the expected data found matching %s", query )
                          .containsExactlyInAnyOrderElementsOf( expectedMatches );
                }
            }
        }

        private IntStream rangeFromOneToMaxPartitions()
        {
            return IntStream.rangeClosed( 1, maxNumberOfPartitions );
        }
    }

    protected String getTokenIndexName( EntityType entityType )
    {
        try ( var tx = beginTx() )
        {
            final var indexes = tx.schemaRead().index( SchemaDescriptors.forAnyEntityTokens( entityType ) );
            assertThat( indexes.hasNext() ).as( "%s based token index exists", entityType ).isTrue();
            final var index = indexes.next();
            assertThat( indexes.hasNext() ).as( "only one %s based token index exists", entityType ).isFalse();
            return index.getName();
        }
        catch ( TransactionFailureException e )
        {
            throw new AssertionError( String.format( "failed to get %s based token index", entityType ), e );
        }
    }

    protected void createIndexes( Iterable<IndexPrototype> indexPrototypes )
    {
        try ( var tx = beginTx() )
        {
            final var schemaWrite = tx.schemaWrite();
            for ( var indexPrototype : indexPrototypes )
            {
                schemaWrite.indexCreate( indexPrototype );
            }
            tx.commit();
        }
        catch ( KernelException e )
        {
            throw new AssertionError( "failed to create indexes", e );
        }

        try ( var tx = beginTx() )
        {
            new SchemaImpl( tx ).awaitIndexesOnline( 1, TimeUnit.HOURS );
        }
        catch ( TransactionFailureException e )
        {
            throw new AssertionError( "failed waiting for indexes to come online", e );
        }
    }

    protected int calculateMaxNumberOfPartitions( Iterable<SCAN_QUERY> scanQueries )
    {
        var maxNumberOfPartitions = 0;
        try ( var tx = beginTx() )
        {
            for ( var query : scanQueries )
            {
                maxNumberOfPartitions = Math.max( maxNumberOfPartitions, factory.partitionedScan( tx, query, Integer.MAX_VALUE ).getNumberOfPartitions() );
            }
        }
        catch ( KernelException e )
        {
            throw new AssertionError( "failed to calculated max number of partitions", e );
        }

        return maxNumberOfPartitions;
    }

    protected static class EntityIdsMatchingScanQuery<SCAN_QUERY extends ScanQuery<?>>
            implements Iterable<Map.Entry<SCAN_QUERY,Set<Long>>>
    {
        private final Map<SCAN_QUERY,Set<Long>> matches = new HashMap<>();

        final Set<Long> getOrCreate( SCAN_QUERY scanQuery )
        {
            return matches.computeIfAbsent( scanQuery, sq -> new HashSet<>() );
        }

        final Set<Long> addOrReplace( SCAN_QUERY scanQuery, Set<Long> entityIds )
        {
            return matches.put( scanQuery, entityIds );
        }

        final Set<SCAN_QUERY> scanQueries()
        {
            return matches.keySet();
        }

        @Override
        public Iterator<Map.Entry<SCAN_QUERY,Set<Long>>> iterator()
        {
            return matches.entrySet().iterator();
        }
    }

    protected final <TAG> List<Integer> createTags( int numberOfTags, PartitionedScanFactories.Tag<TAG> tagFactory )
    {
        List<Integer> tagIds;
        try ( var tx = beginTx() )
        {
            tagIds = tagFactory.generateAndCreateIds( tx, numberOfTags );
            tx.commit();
        }
        catch ( KernelException e )
        {
            throw new AssertionError( String.format( "failed to create %ss in database", tagFactory.name() ), e );
        }
        return tagIds;
    }

    protected interface ScanQuery<QUERY>
    {
        String indexName();

        QUERY get();
    }

    interface TestSuite<SCAN_QUERY extends ScanQuery<?>, CURSOR extends Cursor>
    {
        PartitionedScanFactory<SCAN_QUERY,CURSOR> getFactory();
    }
}
