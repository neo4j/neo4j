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
package org.neo4j.kernel.impl.transaction.state;

import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntPredicate;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.TestPropertyScanConsumer;
import org.neo4j.kernel.impl.transaction.state.storeview.TestTokenScanConsumer;
import org.neo4j.kernel.impl.transaction.state.storeview.NodeStoreScan;
import org.neo4j.lock.LockService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.block.factory.primitive.IntPredicates.alwaysTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.collection.PrimitiveArrays.intersect;
import static org.neo4j.collection.PrimitiveArrays.intsToLongs;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( RandomExtension.class )
class NodeStoreScanTest
{
    private static final String KEY_NAME = "name";
    private static final String KEY_AGE = "age";

    @Inject
    private RandomRule random;

    private final LockService locks = mock( LockService.class, RETURNS_MOCKS );
    private final StubStorageCursors cursors = new StubStorageCursors();
    private final int[] allPossibleLabelIds = {1, 2, 3, 4};
    private int nameKeyId;
    private int ageKeyId;
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @BeforeEach
    void setUp() throws KernelException
    {
        nameKeyId = cursors.propertyKeyTokenHolder().getOrCreateId( KEY_NAME );
        ageKeyId = cursors.propertyKeyTokenHolder().getOrCreateId( KEY_AGE );
    }

    @Test
    void shouldScanOverRelevantNodesAllLabelsAndAllProperties()
    {
        shouldScanOverRelevantNodes( allPossibleLabelIds, alwaysTrue() );
    }

    @Test
    void shouldScanOverRelevantNodesAllLabelsAndSomeProperties()
    {
        shouldScanOverRelevantNodes( allPossibleLabelIds, key -> key == nameKeyId );
    }

    @Test
    void shouldScanOverRelevantNodesSomeLabelsAndAllProperties()
    {
        shouldScanOverRelevantNodes( randomLabels(), alwaysTrue() );
    }

    @Test
    void shouldScanOverRelevantNodesSomeLabelsAndSomeProperties()
    {
        shouldScanOverRelevantNodes( randomLabels(), key -> key == ageKeyId );
    }

    @Test
    void shouldSeeZeroProgressBeforeRunStarted()
    {
        // given
        NodeStoreScan scan = new NodeStoreScan( Config.defaults(), cursors, locks, mock( TokenScanConsumer.class ), mock( PropertyScanConsumer.class ),
                allPossibleLabelIds, k -> true, false, jobScheduler, NULL, INSTANCE );

        // when
        PopulationProgress progressBeforeStarted = scan.getProgress();

        // then
        assertThat( progressBeforeStarted.getCompleted() ).isZero();
    }

    private void shouldScanOverRelevantNodes( int[] labelFilter, IntPredicate propertyKeyFilter )
    {
        // given
        long total = 100;
        MutableLongSet expectedPropertyUpdatesNodes = new LongHashSet();
        MutableLongSet expectedTokenUpdatesNodes = new LongHashSet();
        for ( long id = 0; id < total; id++ )
        {
            StubStorageCursors.NodeData node = cursors.withNode( id );
            int[] labels = randomLabels();
            node.labels( intsToLongs( labels ) );
            Map<String,Value> properties = new HashMap<>();
            boolean passesPropertyFilter = false;
            if ( random.nextBoolean() )
            {
                properties.put( KEY_NAME, Values.of( "Node_" + id ) );
                passesPropertyFilter |= propertyKeyFilter.test( nameKeyId );
            }
            if ( random.nextBoolean() )
            {
                properties.put( KEY_AGE, Values.of( id ) );
                passesPropertyFilter |= propertyKeyFilter.test( ageKeyId );
            }
            node.properties( properties );
            if ( passesPropertyFilter && intersect( intsToLongs( labels ), intsToLongs( labelFilter ) ).length > 0 )
            {
                expectedPropertyUpdatesNodes.add( id );
            }
            if ( labels.length > 0 )
            {
                expectedTokenUpdatesNodes.add( id );
            }
        }

        // when
        var tokenConsumer = new TestTokenScanConsumer();
        var propertyConsumer = new TestPropertyScanConsumer();
        NodeStoreScan scan =
                new NodeStoreScan( Config.defaults(), cursors, locks, tokenConsumer, propertyConsumer, labelFilter, propertyKeyFilter, false, jobScheduler,
                        NULL, INSTANCE );
        assertThat( scan.getProgress().getCompleted() ).isZero();

        scan.run( StoreScan.NO_EXTERNAL_UPDATES );

        // then
        assertThat( LongSets.mutable.of( tokenConsumer.batches.stream().flatMap( Collection::stream ).mapToLong( record -> record.getEntityId() ).toArray() ) )
                .isEqualTo( expectedTokenUpdatesNodes );
        assertThat( LongSets.mutable.of( propertyConsumer.batches.stream()
                                                                 .flatMap( Collection::stream )
                                                                 .mapToLong( record -> record.getEntityId() )
                                                                 .toArray() ) )
                .isEqualTo( expectedPropertyUpdatesNodes );
    }

    private int[] randomLabels()
    {
        MutableIntList list = IntLists.mutable.empty();
        for ( int i = random.nextInt( allPossibleLabelIds.length ); i < allPossibleLabelIds.length; i += random.nextInt( 1, allPossibleLabelIds.length - 1 ) )
        {
            list.add( allPossibleLabelIds[i] );
        }
        return list.toArray();
    }
}
