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
package org.neo4j.index;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.count;

@RunWith( Parameterized.class )
public class IndexTxStateLookupTest
{
    private static final String TRIGGER_LAZY = "this is supposed to be a really long property to trigger lazy loading";
    private static final Random random = new Random();

    @SuppressWarnings( "RedundantStringConstructorCall" )
    @Parameterized.Parameters( name = "store=<{0}> lookup=<{1}>" )
    public static Iterable<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        parameters.addAll( asList(
                new Object[]{new String( "name" ), new String( "name" )},
                new Object[]{7, 7L},
                new Object[]{9L, 9},
                new Object[]{2, 2.0},
                new Object[]{3L, 3.0},
                new Object[]{4, 4.0f},
                new Object[]{5L, 5.0f},
                new Object[]{12.0, 12},
                new Object[]{13.0, 13L},
                new Object[]{14.0f, 14},
                new Object[]{15.0f, 15L},
                new Object[]{2.5f, 2.5},
                new Object[]{16.25, 16.25f},
                new Object[]{stringArray( "a", "b", "c" ), charArray( 'a', 'b', 'c' )},
                new Object[]{charArray( 'd', 'e', 'f' ), stringArray( "d", "e", "f" )},
                new Object[]{splitStrings( TRIGGER_LAZY ), splitChars( TRIGGER_LAZY )},
                new Object[]{splitChars( TRIGGER_LAZY ), splitStrings( TRIGGER_LAZY )},
                new Object[]{stringArray( "foo", "bar" ), stringArray( "foo", "bar" )} ) );
        Class[] numberTypes = {byte.class, short.class, int.class, long.class, float.class, double.class};
        for ( Class lhs : numberTypes )
        {
            for ( Class rhs : numberTypes )
            {
                parameters.add( randomNumbers( 3, lhs, rhs ) );
                parameters.add( randomNumbers( 200, lhs, rhs ) );
            }
        }
        return parameters;
    }

    private static class NamedObject
    {
        private final Object object;
        private final String name;

        NamedObject( Object object, String name )
        {
            this.object = object;
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    private static NamedObject stringArray( String... items )
    {
        return new NamedObject( items, arrayToString( items ) );
    }

    private static NamedObject charArray( char... items )
    {
        return new NamedObject( items, arrayToString( items ) );
    }

    private static Object[] randomNumbers( int length, Class<?> lhsType, Class<?> rhsType )
    {
        Object lhs = Array.newInstance( lhsType, length );
        Object rhs = Array.newInstance( rhsType, length );
        for ( int i = 0; i < length; i++ )
        {
            int value = random.nextInt( 128 );
            Array.set( lhs, i, convert( value, lhsType ) );
            Array.set( rhs, i, convert( value, rhsType ) );
        }
        return new Object[]{
                new NamedObject( lhs, arrayToString( lhs ) ),
                new NamedObject( rhs, arrayToString( rhs ) )};
    }

    private static String arrayToString( Object arrayObject )
    {
        int length = Array.getLength( arrayObject );
        String type = arrayObject.getClass().getComponentType().getSimpleName();
        StringBuilder builder = new StringBuilder( "(" + type + ") {" );
        for ( int i = 0; i < length; i++ )
        {
            builder.append( i > 0 ? "," : "" ).append( Array.get( arrayObject, i ) );
        }
        return builder.append( "}" ).toString();
    }

    private static Object convert( int value, Class<?> type )
    {
        switch ( type.getName() )
        {
        case "byte":
            return (byte) value;
        case "short":
            return (short) value;
        case "int":
            return value;
        case "long":
            return (long) value;
        case "float":
            return (float) value;
        case "double":
            return (double) value;
        default:
            return value;
        }
    }

    private static NamedObject splitStrings( String string )
    {
        char[] chars = internalSplitChars( string );
        String[] result = new String[chars.length];
        for ( int i = 0; i < chars.length; i++ )
        {
            result[i] = Character.toString( chars[i] );
        }
        return stringArray( result );
    }

    private static char[] internalSplitChars( String string )
    {
        char[] result = new char[string.length()];
        string.getChars( 0, result.length, result, 0 );
        return result;
    }

    private static NamedObject splitChars( String string )
    {
        char[] result = internalSplitChars( string );
        return charArray( result );
    }

    @ClassRule
    public static final DatabaseRule db = new ImpermanentDatabaseRule();

    private final Object store;
    private final Object lookup;

    public IndexTxStateLookupTest( Object store, Object lookup )
    {
        this.store = realValue( store );
        this.lookup = realValue( lookup );
    }

    private Object realValue( Object object )
    {
        return object instanceof NamedObject ? ((NamedObject)object).object : object;
    }

    @BeforeClass
    public static void given()
    {
        // database with an index on `(:Node).prop`
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( "Node" ) ).on( "prop" ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }

    @Test
    public void lookupWithinTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            // when
            db.createNode( label( "Node" ) ).setProperty( "prop", store );

            // then
            assertEquals( 1, count( db.findNodes( label( "Node" ), "prop", lookup ) ) );

            // no need to actually commit this node
        }
    }

    @Test
    public void lookupWithinTransactionWithCacheEviction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            // when
            db.createNode( label( "Node" ) ).setProperty( "prop", store );

            // then
            assertEquals( 1, count( db.findNodes( label( "Node" ), "prop", lookup ) ) );

            // no need to actually commit this node
        }
    }

    @Test
    public void lookupWithoutTransaction()
    {
        // when
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            (node = db.createNode( label( "Node" ) )).setProperty( "prop", store );
            tx.success();
        }
        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, count( db.findNodes( label( "Node" ), "prop", lookup ) ) );
            tx.success();
        }
        deleteNode( node );
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            tx.success();
        }
    }

    @Test
    public void lookupWithoutTransactionWithCacheEviction()
    {
        // when
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            (node = db.createNode( label( "Node" ) )).setProperty( "prop", store );
            tx.success();
        }
        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, count( db.findNodes( label( "Node" ), "prop", lookup ) ) );
            tx.success();
        }
        deleteNode( node );
    }
}
