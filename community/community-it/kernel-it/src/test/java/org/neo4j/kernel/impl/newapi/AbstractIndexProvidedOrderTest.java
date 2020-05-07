/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.values.storable.ValueTuple.COMPARATOR;

@SuppressWarnings( "FieldCanBeLocal" )
@ExtendWith( RandomExtension.class )
public abstract class AbstractIndexProvidedOrderTest extends KernelAPIReadTestBase<ReadTestSupport>
{
    private static final int N_NODES = 10000;
    private static final int N_ITERATIONS = 100;

    @Inject
    RandomRule randomRule;

    private TreeSet<NodeValueTuple> singlePropValues = new TreeSet<>( COMPARATOR );
    private TreeSet<NodeValueTuple> doublePropValues = new TreeSet<>( COMPARATOR );
    private ValueType[] targetedTypes;
    private IndexDescriptor indexNodeProp;

    @Override
    public ReadTestSupport newTestSupport()
    {
        ReadTestSupport readTestSupport = new ReadTestSupport();
        readTestSupport.addSetting( GraphDatabaseSettings.default_schema_provider, getSchemaIndex().providerName() );
        return readTestSupport;
    }

    abstract GraphDatabaseSettings.SchemaIndex getSchemaIndex();

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            indexNodeProp = unwrap( tx.schema().indexFor( label( "Node" ) ).on( "prop" ).create() );
            tx.schema().indexFor( label( "Node" ) ).on( "prop" ).on( "prip" ).create();
            tx.commit();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 5, MINUTES );
            tx.commit();
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
        targetedTypes = ensureHighEnoughCardinality( targetedTypes );
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < N_NODES; i++ )
            {
                Node node = tx.createNode( label( "Node" ) );
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
            tx.commit();
        }
    }

    private IndexDescriptor unwrap( IndexDefinition indexDefinition )
    {
        return ((IndexDefinitionImpl) indexDefinition).getIndexReference();
    }

    @Test
    void shouldProvideResultInOrderIfCapable() throws KernelException
    {
        int prop = token.propertyKey( "prop" );

        RandomValues randomValues = randomRule.randomValues();
        IndexReadSession index = read.indexReadSession( indexNodeProp );
        for ( int i = 0; i < N_ITERATIONS; i++ )
        {
            ValueType type = randomValues.among( targetedTypes );
            IndexOrder[] order = index.reference().getCapability().orderCapability( type.valueGroup.category() );
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

                try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor( NULL ) )
                {
                    read.nodeIndexSeek( index, node, constrained( indexOrder, false ), range );

                    List<Long> expectedIdsInOrder = expectedIdsInOrder( from, fromInclusive, to, toInclusive, indexOrder );
                    List<Long> actualIdsInOrder = new ArrayList<>();
                    while ( node.next() )
                    {
                        actualIdsInOrder.add( node.nodeReference() );
                    }

                    assertEquals( expectedIdsInOrder, actualIdsInOrder, "actual node ids not in same order as expected for value type " + type );

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

    /**
     * If targetedTypes only contain types that has very low cardinality, then add one random high cardinality value type to the array.
     * This is to prevent createTestGraph from looping forever when trying to generate unique values.
     */
    private ValueType[] ensureHighEnoughCardinality( ValueType[] targetedTypes )
    {
        ValueType[] lowCardinalityArray = new ValueType[]{ValueType.BOOLEAN, ValueType.BYTE, ValueType.BOOLEAN_ARRAY};
        List<ValueType> typesOfLowCardinality = new ArrayList<>( Arrays.asList( lowCardinalityArray ) );
        for ( ValueType targetedType : targetedTypes )
        {
            if ( !typesOfLowCardinality.contains( targetedType ) )
            {
                return targetedTypes;
            }
        }
        List<ValueType> result = new ArrayList<>( Arrays.asList( targetedTypes ) );
        ValueType highCardinalityType = randomRule.randomValues().among( RandomValues.excluding( lowCardinalityArray ) );
        result.add( highCardinalityType );
        return result.toArray( new ValueType[0] );
    }

    private static class NodeValueTuple extends ValueTuple
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
