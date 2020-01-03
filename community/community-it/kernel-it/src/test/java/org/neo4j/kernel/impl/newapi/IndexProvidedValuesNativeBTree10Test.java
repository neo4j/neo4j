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

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.KernelAPIReadTestBase;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

public class IndexProvidedValuesNativeBTree10Test extends KernelAPIReadTestBase<ReadTestSupport>
{
    @SuppressWarnings( "FieldCanBeLocal" )
    private static int N_NODES = 10000;

    @Rule
    public RandomRule randomRule = new RandomRule();

    private List<Value> singlePropValues = new ArrayList<>();
    private List<ValueTuple> doublePropValues = new ArrayList<>();

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

        try ( Transaction tx = graphDb.beginTx() )
        {
            RandomValues randomValues = randomRule.randomValues();

            ValueType[] allExceptNonSortable = RandomValues.excluding( ValueType.STRING, ValueType.STRING_ARRAY );

            for ( int i = 0; i < N_NODES; i++ )
            {
                Node node = graphDb.createNode( label( "Node" ) );
                Value propValue = randomValues.nextValueOfTypes( allExceptNonSortable );
                node.setProperty( "prop", propValue.asObject() );
                Value pripValue = randomValues.nextValueOfTypes( allExceptNonSortable );
                node.setProperty( "prip", pripValue.asObject() );

                singlePropValues.add( propValue );
                doublePropValues.add( ValueTuple.of( propValue, pripValue ) );
            }
            tx.success();
        }

        singlePropValues.sort( Values.COMPARATOR );
        doublePropValues.sort( ValueTuple.COMPARATOR  );
    }

    @Test
    public void shouldGetAllSinglePropertyValues() throws Exception
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            read.nodeIndexScan( index, node, IndexOrder.NONE, true );

            List<Value> values = new ArrayList<>();
            while ( node.next() )
            {
                values.add( node.propertyValue( 0 ) );
            }

            values.sort( Values.COMPARATOR );
            for ( int i = 0; i < singlePropValues.size(); i++ )
            {
                assertEquals( singlePropValues.get( i ), values.get( i ) );
            }
        }
    }

    @Test
    public void shouldGetAllDoublePropertyValues() throws Exception
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        int prip = token.propertyKey( "prip" );
        IndexReference index = schemaRead.index( label, prop, prip );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            read.nodeIndexScan( index, node, IndexOrder.NONE, true );

            List<ValueTuple> values = new ArrayList<>();
            while ( node.next() )
            {
                values.add( ValueTuple.of( node.propertyValue( 0 ), node.propertyValue( 1 ) ) );
            }

            values.sort( ValueTuple.COMPARATOR );
            for ( int i = 0; i < doublePropValues.size(); i++ )
            {
                assertEquals( doublePropValues.get( i ), values.get( i ) );
            }
        }
    }
}
