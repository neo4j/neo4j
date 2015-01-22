/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lookup;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Lookup.endsWith;
import static org.neo4j.graphdb.Lookup.equalTo;
import static org.neo4j.graphdb.Lookup.greaterThan;
import static org.neo4j.graphdb.Lookup.greaterThanOrEqualTo;
import static org.neo4j.graphdb.Lookup.lessThan;
import static org.neo4j.graphdb.Lookup.lessThanOrEqualTo;
import static org.neo4j.graphdb.Lookup.not;
import static org.neo4j.graphdb.Lookup.startsWith;

public class IndexSearchIT
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldSearchByPrefix() throws Exception
    {
        // given
        Label person = label( "Person" );
        createIndex( person, "name" );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( person ).setProperty( "name", "Alfred Nobel" );
            db.createNode( person ).setProperty( "name", "Karl-Alfred Sailor" );
            db.createNode( person ).setProperty( "name", "Karl-Oskar Nilsson" );
            db.createNode( person ).setProperty( "name", "Alfred Mann" );
            db.createNode( person ).setProperty( "name", "Oskar Persson" );

            tx.success();
        }

        // then
        assertQueryResult( person, "name", startsWith( "Alfred" ), "Alfred Mann", "Alfred Nobel" );
    }

    @Test
    public void shouldSearchBySuffix() throws Exception
    {
        // given
        Label person = label( "Person" );
        createIndex( person, "name" );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( person ).setProperty( "name", "Johan Svensson" );
            db.createNode( person ).setProperty( "name", "Mattias Persson" );
            db.createNode( person ).setProperty( "name", "Tobias Lindaaker" );
            db.createNode( person ).setProperty( "name", "Andres Taylor" );
            db.createNode( person ).setProperty( "name", "Peter Neubauer" );

            tx.success();
        }
        // then
        assertQueryResult( person, "name", endsWith( "sson" ), "Johan Svensson", "Mattias Persson" );
    }

    @Test
    public void shouldSearchByNumber() throws Exception
    {
        // given
        Label person = label( "Person" );
        createIndex( person, "age" );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( person ).setProperty( "age", 36 );
            db.createNode( person ).setProperty( "age", 20 );
            db.createNode( person ).setProperty( "age", 25 );
            db.createNode( person ).setProperty( "age", 22 );
            db.createNode( person ).setProperty( "age", 30 );
            db.createNode( person ).setProperty( "age", 22 );
            db.createNode( person ).setProperty( "age", 27 );

            tx.success();
        }

        // then
        assertQueryResult( person, "age", lessThan( 25 ), 20, 22, 22 );
        assertQueryResult( person, "age", lessThanOrEqualTo( 25 ), 20, 22, 22, 25 );
        assertQueryResult( person, "age", greaterThan( 25 ), 27, 30, 36 );
        assertQueryResult( person, "age", greaterThanOrEqualTo( 30 ), 30, 36 );

        assertQueryResult( person, "age", lessThan( 25 ).andGreaterThan( 21 ), 22, 22 );
        assertQueryResult( person, "age", lessThanOrEqualTo( 25 ).andGreaterThan( 22 ), 25 );
        assertQueryResult( person, "age", greaterThan( 25 ).andLessThan( 35 ), 27, 30 );
        assertQueryResult( person, "age", greaterThanOrEqualTo( 30 ).andLessThan( 35 ), 30);

        assertQueryResult( person, "age", lessThan( 25 ).andGreaterThanOrEqualTo( 21 ), 22, 22 );
        assertQueryResult( person, "age", lessThanOrEqualTo( 25 ).andGreaterThanOrEqualTo( 22 ), 22, 22, 25 );
        assertQueryResult( person, "age", greaterThan( 25 ).andLessThanOrEqualTo( 35 ), 27, 30 );
        assertQueryResult( person, "age", greaterThanOrEqualTo( 30 ).andLessThanOrEqualTo( 35 ), 30);
    }

    @Test
    public void shouldNegateNumericalRangeQueries() throws Exception
    {
        // given
        Label person = label( "Person" );
        createIndex( person, "age" );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( person ).setProperty( "age", 22 );
            db.createNode( person ).setProperty( "age", 28 );
            db.createNode( person ).setProperty( "age", 23 );
            db.createNode( person ).setProperty( "age", 27 );
            db.createNode( person ).setProperty( "age", 31 );
            db.createNode( person ).setProperty( "age", 21 );
            db.createNode( person ).setProperty( "age", 28 );
            db.createNode( person ).setProperty( "age", 35 );
            db.createNode( person ).setProperty( "age", 23 );

            tx.success();
        }

        // then
        assertQueryResult( person, "age", not( greaterThanOrEqualTo( 23 ).andLessThanOrEqualTo( 28 ) ),
                           21, 22, 31, 35 );
    }

    @Test
    public void TODO_SupportNegatedQueries() throws Exception
    {
        // given
        Label person = label( "Person" );
        createIndex( person, "name" );

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.findNodes( person, "name", not( equalTo( "Foo" ) ) );

            fail( "expected exception" );
            tx.success();
        }
        // then
        catch ( UnsupportedOperationException e )
        {
            assertTrue( e.getMessage(), e.getMessage().startsWith( "Cannot negate: " ) );
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.findNodes( person, "name", not( startsWith( "Foo" ) ) );

            fail( "expected exception" );
            tx.success();
        }
        // then
        catch ( UnsupportedOperationException e )
        {
            assertTrue( e.getMessage(), e.getMessage().startsWith( "Cannot negate: " ) );
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.findNodes( person, "name", not( endsWith( "Foo" ) ) );

            fail( "expected exception" );
            tx.success();
        }
        // then
        catch ( UnsupportedOperationException e )
        {
            assertTrue( e.getMessage(), e.getMessage().startsWith( "Cannot negate: " ) );
        }
    }

    private void assertQueryResult( Label label, String key, Lookup query, Object... expectedValues )
    {
        List<?> expected = Arrays.asList( expectedValues ), actual;
        try ( Transaction tx = db.beginTx() )
        {
            actual = collect( key, db.findNodes( label, key, query ) );
            tx.success();
        }
        if ( !expected.equals( actual ) )
        {
            String message = "expected:<" + expected + "> but was:<" + actual + ">";
            if ( !counted( expected ).equals( counted( actual ) ) )
            {
                fail( message );
            }
            else
            {
                System.err.println( "Values not in expected order, " + message );
            }
        }
    }

    private static List<?> collect( String key, ResourceIterator<? extends PropertyContainer> iterator )
    {
        try
        {
            List<Object> values = new ArrayList<>();
            while ( iterator.hasNext() )
            {
                values.add( iterator.next().getProperty( key ) );
            }
            return values;
        }
        finally
        {
            iterator.close();
        }
    }

    private static Map<Object, Integer> counted( Iterable<?> items )
    {
        Map<Object, Integer> result = new HashMap<>();
        for ( Object item : items )
        {
            Integer count = result.get( item );
            result.put( item, count == null ? 1 : (count + 1) );
        }
        return result;
    }

    private static <T> Set<T> assertedUnique( List<T> list )
    {
        Set<T> set = new HashSet<>( list );
        assertEquals( list.size(), set.size() );
        return set;
    }

    private void createIndex( Label label, String propertyKey )
    {
        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.schema().indexFor( label ).on( propertyKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 10, TimeUnit.SECONDS );
            tx.success();
        }
    }
}
