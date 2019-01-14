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
package org.neo4j.graphdb;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.array;

@RunWith( Parameterized.class )
public class IndexingCompositeQueryAcceptanceTest
{
    @ClassRule
    public static ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    @Rule
    public final TestName testName = new TestName();

    @Parameterized.Parameters
    public static List<Object[]> data()
    {
        return Arrays.asList(
                testCase( array( 2, 3 ), biIndexSeek, true ),
                testCase( array( 2, 3 ), biIndexSeek, false ),
                testCase( array( 2, 3, 4 ), triIndexSeek, true ),
                testCase( array( 2, 3, 4 ), triIndexSeek, false ),
                testCase( array( 2, 3, 4, 5, 6 ), mapIndexSeek, true ),
                testCase( array( 2, 3, 4, 5, 6 ), mapIndexSeek, false )
        );
    }

    @Parameterized.Parameter( 0 )
    public String[] keys;
    @Parameterized.Parameter( 1 )
    public Object[] values;
    @Parameterized.Parameter( 2 )
    public Object[][] nonMatching;
    @Parameterized.Parameter( 3 )
    public IndexSeek indexSeek;
    @Parameterized.Parameter( 4 )
    public boolean withIndex;

    private static Label LABEL = Label.label( "LABEL1" );
    private GraphDatabaseService db;

    @Before
    public void setup()
    {
        db = dbRule.getGraphDatabaseAPI();
        if ( withIndex )
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( LABEL ).on( keys[0] ).create();

                IndexCreator indexCreator = db.schema().indexFor( LABEL );
                for ( String key : keys )
                {
                    indexCreator = indexCreator.on( key );
                }
                indexCreator.create();
                tx.success();
            }

            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
                tx.success();
            }
        }
    }

    @After
    public void tearDown()
    {
        dbRule.shutdown();
    }

    @Test
    public void shouldSupportIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching );
        PrimitiveLongSet expected = createNodes( db, LABEL, values );

        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            collectNodes( found, indexSeek.findNodes( keys, values, db ) );
        }

        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldSupportIndexSeekBackwardsOrder()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching );
        PrimitiveLongSet expected = createNodes( db, LABEL, values );

        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        String[] reversedKeys = new String[keys.length];
        Object[] reversedValues = new Object[keys.length];
        for ( int i = 0; i < keys.length; i++ )
        {
            reversedValues[keys.length - 1 - i] = values[i];
            reversedKeys[keys.length - 1 - i] = keys[i];
        }
        try ( Transaction tx = db.beginTx() )
        {
            collectNodes( found, indexSeek.findNodes( reversedKeys, reversedValues, db ) );
        }

        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldIncludeNodesCreatedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0], nonMatching[1] );
        PrimitiveLongSet expected = createNodes( db, LABEL, values );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            expected.add( createNode( db, propertyMap( keys, values ), LABEL ).getId() );
            createNode( db, propertyMap( keys, nonMatching[2] ), LABEL );

            collectNodes( found, indexSeek.findNodes( keys, values, db ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldNotIncludeNodesDeletedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0] );
        PrimitiveLongSet toDelete = createNodes( db, LABEL, values, nonMatching[1], nonMatching[2] );
        PrimitiveLongSet expected = createNodes( db, LABEL, values );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            PrimitiveLongIterator deleting = toDelete.iterator();
            while ( deleting.hasNext() )
            {
                long id = deleting.next();
                db.getNodeById( id ).delete();
                expected.remove( id );
            }

            collectNodes( found, indexSeek.findNodes( keys, values, db ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldConsiderNodesChangedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0] );
        PrimitiveLongSet toChangeToMatch = createNodes( db, LABEL, nonMatching[1] );
        PrimitiveLongSet toChangeToNotMatch = createNodes( db, LABEL, values );
        PrimitiveLongSet expected = createNodes( db, LABEL, values );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            PrimitiveLongIterator toMatching = toChangeToMatch.iterator();
            while ( toMatching.hasNext() )
            {
                long id = toMatching.next();
                setProperties( id, values );
                expected.add( id );
            }
            PrimitiveLongIterator toNotMatching = toChangeToNotMatch.iterator();
            while ( toNotMatching.hasNext() )
            {
                long id = toNotMatching.next();
                setProperties( id, nonMatching[2] );
                expected.remove( id );
            }

            collectNodes( found, indexSeek.findNodes( keys, values, db ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    public PrimitiveLongSet createNodes( GraphDatabaseService db, Label label, Object[]... propertyValueTuples )
    {
        PrimitiveLongSet expected = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Object[] valueTuple : propertyValueTuples )
            {
                expected.add( createNode( db, propertyMap( keys, valueTuple ), label ).getId() );
            }
            tx.success();
        }
        return expected;
    }

    public static Map<String,Object> propertyMap( String[] keys, Object[] valueTuple )
    {
        Map<String,Object> propertyValues = new HashMap<>();
        for ( int i = 0; i < keys.length; i++ )
        {
            propertyValues.put( keys[i], valueTuple[i] );
        }
        return propertyValues;
    }

    public void collectNodes( PrimitiveLongSet bucket, ResourceIterator<Node> toCollect )
    {
        while ( toCollect.hasNext() )
        {
            bucket.add( toCollect.next().getId() );
        }
    }

    public Node createNode( GraphDatabaseService beansAPI, Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = beansAPI.beginTx() )
        {
            Node node = beansAPI.createNode( labels );
            for ( Map.Entry<String,Object> property : properties.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            tx.success();
            return node;
        }
    }

    public static Object[] testCase( Integer[] values, IndexSeek indexSeek, boolean withIndex )
    {
        Object[][] nonMatching = {plus( values, 1 ), plus( values, 2 ), plus( values, 3 )};
        String[] keys = Arrays.stream( values ).map( v -> "key" + v ).toArray( String[]::new );
        return new Object[]{keys, values, nonMatching, indexSeek, withIndex};
    }

    public static <T> Object[] plus( Integer[] values, int offset )
    {
        Object[] result = new Object[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = values[i] + offset;
        }
        return result;
    }

    private void setProperties( long id, Object[] values )
    {
        Node node = db.getNodeById( id );
        for ( int i = 0; i < keys.length; i++ )
        {
            node.setProperty( keys[i], values[i] );
        }
    }

    public interface IndexSeek
    {
        ResourceIterator<Node> findNodes( String[] keys, Object[] values, GraphDatabaseService db );
    }

    public static IndexSeek biIndexSeek =
            ( keys, values, db ) ->
            {
                assert keys.length == 2;
                assert values.length == 2;
                return db.findNodes( LABEL, keys[0], values[0], keys[1], values[1] );
            };

    public static IndexSeek triIndexSeek =
            ( keys, values, db ) ->
            {
                assert keys.length == 3;
                assert values.length == 3;
                return db.findNodes( LABEL, keys[0], values[0], keys[1], values[1], keys[2], values[2] );
            };

    public static IndexSeek mapIndexSeek =
            ( keys, values, db ) ->
            {
                return db.findNodes( LABEL, propertyMap( keys, values ) );
            };
}
