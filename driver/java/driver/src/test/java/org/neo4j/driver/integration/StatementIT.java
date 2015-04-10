/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.ReusableResult;
import org.neo4j.driver.Values;
import org.neo4j.driver.util.TestSession;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.Neo4j.parameters;

public class StatementIT
{
    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldRun() throws Throwable
    {
        // When
        session.run( "CREATE (n:FirstNode)" );

        // Then nothing should've failed
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // When I run a simple write statement
        session.run( "CREATE (a {name:'Adam'})" );

        // And I run a read statement
        Result result = session.run( "MATCH (a) RETURN a.name" );

        // Then I expect to get the name back
        String name = null;
        while ( result.next() )
        {
            name = result.get( "a.name" ).javaString();
        }

        assertThat( name, equalTo( "Adam" ) );
    }

    @Test
    public void shouldRunWithResult() throws Throwable
    {
        // When I execute a statement that yields a result
        ReusableResult result = session.run( "UNWIND [1,2,3] AS k RETURN k" ).retain();

        // Then the result object should contain the returned values
        assertThat( result.size(), equalTo( 3l ) );

        // And it should allow random access
        assertThat( result.get( 0 ).get( "k" ).javaLong(), equalTo( 1l ) );
        assertThat( result.get( 1 ).get( "k" ).javaLong(), equalTo( 2l ) );
        assertThat( result.get( 2 ).get( "k" ).javaLong(), equalTo( 3l ) );

        // And it should allow iteration
        long expected = 0;
        for ( Record value : result )
        {
            expected += 1;
            assertThat( value.get( "k" ), equalTo( Values.value( expected ) ) );
        }
        assertThat( expected, equalTo( 3l ) );
    }

    @Test
    public void shouldRunWithParameters() throws Throwable
    {
        // When
        session.run( "CREATE (n:FirstNode {name:{name}})", parameters( "name", "Steven" ) );

        // Then nothing should've failed
    }

    @Test
    public void shouldRunParameterizedWithResult() throws Throwable
    {
        // When
        ReusableResult result =
                session.run( "UNWIND {list} AS k RETURN k", parameters( "list", asList( 1, 2, 3 ) ) ).retain();

        // Then
        assertThat( result.size(), equalTo( 3l ) );
    }

    @Test
    public void shouldReadYourWrites() throws Throwable
    {
        // Given I've performed a write operation
        session.run( "CREATE (n:MyNode)" );

        // When
        ReusableResult result = session.run( "MATCH (n:MyNode) RETURN 1" ).retain();

        // Then
        assertThat( result.size(), equalTo( 1l ) );
    }
}
