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
package org.neo4j.kernel.api.index;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.createIndex;

/*
 * The purpose of this test class is to make sure all index providers produce the same results.
 *
 * Indexes should always produce the same result as scanning all nodes and checking properties. By extending this
 * class in the index provider module, all value types will be checked against the index provider.
 */
@RunWith( value = Parameterized.class )
public abstract class IndexProviderApprovalTest
{
    // These are the values that will be checked.
    public enum TestValue
    {
        BOOLEAN_TRUE( true ),
        BOOLEAN_FALSE( false ),
        STRING_TRUE( "true" ),
        STRING_FALSE( "false" ),
        STRING_UPPER_A( "A" ),
        STRING_LOWER_A( "a" ),
        CHAR_UPPER_A( 'A' ),
        CHAR_LOWER_A( 'a' ),
        INT_42( 42 ),
        LONG_42( (long) 42 ),
        LARGE_LONG_1( 4611686018427387905L ),
        LARGE_LONG_2( 4611686018427387907L ),
        BYTE_42( (byte) 42 ),
        DOUBLE_42( (double) 42 ),
        DOUBLE_42andAHalf( 42.5d ),
        SHORT_42( (short) 42 ),
        FLOAT_42( (float) 42 ),
        FLOAT_42andAHalf( 42.5f ),
        POINT_123456_GPS( Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) ),
        POINT_123456_CAR( Values.pointValue( CoordinateReferenceSystem.Cartesian, 123, 456 ) ),
        POINT_123456_GPS_3D( Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.3, 45.6, 78.9 ) ),
        POINT_123456_CAR_3D( Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 123, 456, 789 ) ),
        ARRAY_OF_INTS( new int[]{1, 2, 3} ),
        ARRAY_OF_LONGS( new long[]{1, 2, 3} ),
        ARRAY_OF_LARGE_LONGS_1( new long[] { 4611686018427387905L } ),
        ARRAY_OF_LARGE_LONGS_2( new long[] { 4611686018427387906L } ),
        ARRAY_OF_LARGE_LONGS_3( new Long[] { 4611686018425387907L } ),
        ARRAY_OF_LARGE_LONGS_4( new Long[] { 4611686018425387908L } ),
        ARRAY_OF_BOOL_LIKE_STRING( new String[]{"true", "false", "true"} ),
        ARRAY_OF_BOOLS( new boolean[]{true, false, true} ),
        ARRAY_OF_DOUBLES( new double[]{1, 2, 3} ),
        ARRAY_OF_STRING( new String[]{"1", "2", "3"} ),
        EMPTY_ARRAY_OF_INTS( new int[0] ),
        EMPTY_ARRAY_OF_LONGS( new long[0] ),
        EMPTY_ARRAY_OF_BOOLS( new boolean[0] ),
        EMPTY_ARRAY_OF_DOUBLES( new double[0] ),
        EMPTY_ARRAY_OF_STRING( new String[0] ),
        ONE( new String[]{"", "||"} ),
        OTHER( new String[]{"||", ""} ),
        ANOTHER_ARRAY_OF_STRING( new String[]{"1|2|3"} ),
        ARRAY_OF_CHAR( new char[]{'1', '2', '3'} ),
        ARRAY_OF_POINTS_GPS( new PointValue[]{Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 )} ),
        ARRAY_OF_POINTS_CAR( new PointValue[]{Values.pointValue( CoordinateReferenceSystem.Cartesian, 123, 456 )} ),
        ARRAY_OF_POINTS_GPS_3D( new PointValue[]{Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.3, 45.6, 78.9 )} ),
        ARRAY_OF_POINTS_CAR_3D( new PointValue[]{Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 123, 456, 789 )} );

        private final Object value;

        TestValue( Object value )
        {
            this.value = value;
        }
    }

    private static Map<TestValue, Set<Object>> noIndexRun;
    private static Map<TestValue, Set<Object>> indexRun;

    private final TestValue currentValue;

    public IndexProviderApprovalTest( TestValue value )
    {
        currentValue = value;
    }

    @Parameters( name = "{0}" )
    public static Collection<TestValue> data()
    {
        return Arrays.asList( TestValue.values() );
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
        createIndex( db, label( LABEL ), PROPERTY_KEY );
        indexRun = runFindByLabelAndProperty( db );
        db.shutdown();
    }

    public static final String LABEL = "Person";
    public static final String PROPERTY_KEY = "name";
    public static final Function<Node, Object> PROPERTY_EXTRACTOR = node ->
    {
        Object value = node.getProperty( PROPERTY_KEY );
        if ( value.getClass().isArray() )
        {
            return new ArrayEqualityObject( value );
        }
        return value;
    };

    @Test
    public void test()
    {
        Set<Object> noIndexResult = Iterables.asSet( noIndexRun.get( currentValue ) );
        Set<Object> indexResult = Iterables.asSet( indexRun.get( currentValue ) );

        String errorMessage = currentValue.toString();

        assertEquals( errorMessage, noIndexResult, indexResult );
    }

    private static Map<TestValue, Set<Object>> runFindByLabelAndProperty( GraphDatabaseService db )
    {
        HashMap<TestValue, Set<Object>> results = new HashMap<>();
        try ( Transaction tx = db.beginTx() )
        {
            for ( TestValue value : TestValue.values() )
            {
                addToResults( db, results, value );
            }
            tx.success();
        }
        return results;
    }

    private static Node createNode( GraphDatabaseService db, String propertyKey, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( LABEL ) );
            node.setProperty( propertyKey, value );
            tx.success();
            return node;
        }
    }

    private static void addToResults( GraphDatabaseService db, HashMap<TestValue, Set<Object>> results,
                                      TestValue value )
    {
        ResourceIterator<Node> foundNodes = db.findNodes( label( LABEL ), PROPERTY_KEY, value.value );
        Set<Object> propertyValues = asSet( Iterators.map( PROPERTY_EXTRACTOR, foundNodes ) );
        results.put( value, propertyValues );
    }

    private static class ArrayEqualityObject
    {
        private final Object array;

        ArrayEqualityObject( Object array )
        {
            this.array = array;
        }

        @Override
        public int hashCode()
        {
            return ArrayUtil.hashCode( array );
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof ArrayEqualityObject && ArrayUtil.equals( array, ((ArrayEqualityObject) obj).array );
        }

        @Override
        public String toString()
        {
            return Strings.prettyPrint( array );
        }
    }
}
