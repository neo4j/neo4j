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
package org.neo4j.kernel.impl.newapi;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.KernelAPIReadTestBase;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.ValueTuple.COMPARATOR;

@SuppressWarnings( "FieldCanBeLocal" )
public class IndexProvidedOrderNativeBTree10Test extends KernelAPIReadTestBase<ReadTestSupport>
{
    private static int N_NODES = 10000;
    private static int N_ITERATIONS = 100;

    @Rule
    public RandomRule randomRule = new RandomRule();

    private TreeSet<NodeValueTuple> singlePropValues = new TreeSet<>( COMPARATOR );
    private TreeSet<NodeValueTuple> doublePropValues = new TreeSet<>( COMPARATOR );
    private ValueType[] targetedTypes;

    @Override
    public ReadTestSupport newTestSupport()
    {
        ReadTestSupport readTestSupport = new ReadTestSupport();
        readTestSupport.addSetting( GraphDatabaseSettings.default_schema_provider, GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName() );
        return readTestSupport;
    }

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( label( "Node" ) ).on( "prop" ).create();
            graphDb.schema().indexFor( label( "Node" ) ).on( "prop" ).on( "prip" ).create();
            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexesOnline( 5, MINUTES );
            tx.success();
        }

        RandomValues randomValues = randomRule.randomValues();

        ValueType[] allExceptNonOrderable = RandomValues.excluding(
                ValueType.STRING,
                ValueType.STRING_ARRAY,
                ValueType.GEOGRAPHIC_POINT,
                ValueType.GEOGRAPHIC_POINT_ARRAY,
                ValueType.GEOGRAPHIC_POINT_3D,
                ValueType.GEOGRAPHIC_POINT_3D_ARRAY,
                ValueType.CARTESIAN_POINT,
                ValueType.CARTESIAN_POINT_ARRAY,
                ValueType.CARTESIAN_POINT_3D,
                ValueType.CARTESIAN_POINT_3D_ARRAY
        );
        targetedTypes = randomValues.selection( allExceptNonOrderable, 1, allExceptNonOrderable.length, false );
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < N_NODES; i++ )
            {
                Node node = graphDb.createNode( label( "Node" ) );
                Value propValue;
                Value pripValue;
                NodeValueTuple singleValue;
                NodeValueTuple doubleValue;
                do
                {
                    propValue = randomValues.nextValueOfTypes( targetedTypes );
                    pripValue = randomValues.nextValueOfTypes( targetedTypes );
                    singleValue = new NodeValueTuple( node.getId(), propValue );
                    doubleValue = new NodeValueTuple( node.getId(), propValue, pripValue );
                }
                while ( singlePropValues.contains( singleValue ) || doublePropValues.contains( doubleValue ) );
                singlePropValues.add( singleValue );
                doublePropValues.add( doubleValue );

                node.setProperty( "prop", propValue.asObject() );
                node.setProperty( "prip", pripValue.asObject() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldProvideResultInOrderIfCapable() throws KernelException
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );

        RandomValues randomValues = randomRule.randomValues();
        IndexReference index = schemaRead.index( label, prop );
        for ( int i = 0; i < N_ITERATIONS; i++ )
        {
            ValueType type = randomValues.among( targetedTypes );
            IndexOrder[] order = index.orderCapability( type.valueGroup.category() );
            for ( IndexOrder indexOrder : order )
            {
                if ( indexOrder == IndexOrder.NONE )
                {
                    continue;
                }
                NodeValueTuple from = new NodeValueTuple( Long.MIN_VALUE, randomValues.nextValueOfType( type ) );
                NodeValueTuple to = new NodeValueTuple( Long.MAX_VALUE, randomValues.nextValueOfType( type ) );
                if ( COMPARATOR.compare( from, to ) > 0 )
                {
                    NodeValueTuple tmp = from;
                    from = to;
                    to = tmp;
                }
                boolean fromInclusive = randomValues.nextBoolean();
                boolean toInclusive = randomValues.nextBoolean();
                IndexQuery.RangePredicate<?> range = IndexQuery.range( prop, from.getOnlyValue(), fromInclusive, to.getOnlyValue(), toInclusive );

                try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
                {
                    read.nodeIndexSeek( index, node, indexOrder, false, range );

                    List<Long> expectedIdsInOrder = expectedIdsInOrder( from, fromInclusive, to, toInclusive, indexOrder );
                    List<Long> actualIdsInOrder = new ArrayList<>();
                    while ( node.next() )
                    {
                        actualIdsInOrder.add( node.nodeReference() );
                    }

                    assertEquals( expectedIdsInOrder, actualIdsInOrder, "actual node ids not in same order as expected" );

                }
            }
        }
    }

    private List<Long> expectedIdsInOrder( NodeValueTuple from, boolean fromInclusive, NodeValueTuple to, boolean toInclusive, IndexOrder indexOrder )
    {
        List<Long> expectedIdsInOrder = singlePropValues.subSet( from, fromInclusive, to, toInclusive )
                .stream()
                .map( NodeValueTuple::nodeId )
                .collect( Collectors.toList() );
        if ( indexOrder == IndexOrder.DESCENDING )
        {
            Collections.reverse( expectedIdsInOrder );
        }
        return expectedIdsInOrder;
    }

    private class NodeValueTuple extends ValueTuple
    {
        private final long nodeId;

        private NodeValueTuple( long nodeId, Value... values )
        {
            super( values );
            this.nodeId = nodeId;
        }

        long nodeId()
        {
            return nodeId;
        }
    }
}
