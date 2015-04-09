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
import org.junit.rules.ExpectedException;

import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.util.TestSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class TransactionIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldRunAndCommit() throws Throwable
    {
        // When
        try ( Transaction tx = session.newTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNOde)" );
            tx.success();
        }

        // Then the outcome of both statements should be visible
        long nodes = session.run( "MATCH (n) RETURN count(n)" ).single().get( "count(n)" ).javaLong();
        assertThat( nodes, equalTo( 2l ) );
    }

    @Test
    public void shouldRunAndRollbackByDefault() throws Throwable
    {
        // When
        try ( Transaction tx = session.newTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNOde)" );
        }

        // Then there should be no visible effect of the transaction
        long nodes = session.run( "MATCH (n) RETURN count(n)" ).single().get( "count(n)" ).javaLong();
        assertThat( nodes, equalTo( 0l ) );
    }

    @Test
    public void shouldRetrieveResults() throws Throwable
    {
        // Given
        session.run( "CREATE (n {name:'Steve Brook'})" );

        // When
        try ( Transaction tx = session.newTransaction() )
        {
            Result res = tx.run( "MATCH (n) RETURN n.name" );

            // Then
            assertThat( res.single().get( "n.name" ).javaString(), equalTo( "Steve Brook" ) );
        }
    }

    @Test
    public void shouldNotAllowSessionLevelStatementsWhenThereIsATransaction() throws Throwable
    {
        // Given
        session.newTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.run( "anything" );
        fail( "Should have failed." );
    }
}
