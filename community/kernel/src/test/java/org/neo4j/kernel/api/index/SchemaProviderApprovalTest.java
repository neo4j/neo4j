/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Function;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

/*
 * The purpose of this test class is to make sure all index providers produce the same results.
 *
 * Schema Indexes should always produce the same result as scanning all nodes and checking properties. By extending this
 * class in the index provider module, all value types will be checked against the index provider.
 */
@RunWith(value = Parameterized.class)
public abstract class SchemaProviderApprovalTest
{
    /*
    These are the values that will be checked. Searching
     */
    public enum TestValue
    {
        STRING_UPPER_A( "A" ),
        STRING_LOWER_A( "a" ),
        CHAR_UPPER_A( 'A' ),
        CHAR_LOWER_A( 'a' ),
        INT_42( 42 ),
        LONG_42( (long) 42 ),
        BYTE_42( (byte) 42 ),
        DOUBLE_42( (double) 42 ),
        SHORT_42( (short) 42 ),
        FLOAT_42( (float) 42 ),
        ARRAY_OF_INTS( new int[]{1, 2, 3} ),
        ARRAY_OF_DOUBLES( new double[]{1, 2, 3} ),
        ARRAY_OF_STRING( new String[]{"1", "2", "3"} );

        private final Object value;

        private TestValue( Object value )
        {
            this.value = value;
        }
    }

    private static Map<TestValue, Set<Object>> noIndexRun;
    private static Map<TestValue, Set<Object>> indexRun;

    private final TestValue currentValue;

    public SchemaProviderApprovalTest( TestValue value )
    {
        currentValue = value;
    }

    @Parameters
    public static Collection<Object[]> data()
    {
        Iterable<TestValue> testValues = asIterable( TestValue.values() );
        return asCollection( map( new Function<TestValue, Object[]>()
        {
            @Override
            public Object[] apply( TestValue testValue )
            {
                return new Object[]{testValue};
            }
        }, testValues ) );
    }

    @BeforeClass
    public static void init()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        for ( TestValue value : TestValue.values() )
        {
            createNode( db, PROPERTY_KEY, value.value );
        }

        noIndexRun = runFindByLabelAndProperty( db );
        createIndex( db );
        indexRun = runFindByLabelAndProperty( db );
        db.shutdown();
    }

    public static final String LABEL = "Person";
    public static final String PROPERTY_KEY = "name";
    public static final Function<Node, Object> PROPERTY_EXTRACTOR = new Function<Node, Object>()
    {
        @Override
        public Object apply( Node node )
        {
            return node.getProperty( PROPERTY_KEY );
        }
    };

    protected static void createIndex( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        db.schema().indexFor( label( LABEL ) ).on( PROPERTY_KEY ).create();
        tx.success();
        tx.finish();
        db.schema().awaitIndexesOnline( 10, SECONDS );
    }

    @Test
    public void test()
    {
        Set<Object> noIndexResult = asSet( noIndexRun.get( currentValue ) );
        Set<Object> indexResult = asSet( indexRun.get( currentValue ) );

        String errorMessage = currentValue.toString();

        assertEquals( errorMessage, noIndexResult, indexResult );
    }

    private static Map<TestValue, Set<Object>> runFindByLabelAndProperty( GraphDatabaseService db )
    {
        HashMap<TestValue, Set<Object>> results = new HashMap<>();
        for ( TestValue value : TestValue.values() )
        {
            addToResults( db, results, value );
        }
        return results;
    }

    private static Node createNode( GraphDatabaseService db, String propertyKey, Object value )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode( label( LABEL ) );
        node.setProperty( propertyKey, value );
        tx.success();
        tx.finish();
        return node;
    }

    private static void addToResults( GraphDatabaseService db, HashMap<TestValue, Set<Object>> results, TestValue value )
    {
        ResourceIterable<Node> foundNodes = db.findNodesByLabelAndProperty( label( LABEL ), PROPERTY_KEY, value.value );
        Set<Object> propertyValues = asSet( map( PROPERTY_EXTRACTOR, foundNodes ) );
        results.put( value, propertyValues );
    }
}
