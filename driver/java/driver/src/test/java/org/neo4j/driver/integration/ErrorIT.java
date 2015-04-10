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

import org.neo4j.Neo4j;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.util.TestSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ErrorIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldThrowHelpfulSyntaxError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Invalid input 'i': expected <init> (line 1, column 1 (offset: 0))\n" +
                                 "\"invalid statement\"\n" +
                                 " ^" );

        // When
        session.run( "invalid statement" );
    }

    @Test
    public void shouldNotAllowUsingTransactionAfterError() throws Throwable
    {
        // Given
        Transaction tx = session.newTransaction();

        // And Given an error has occurred
        try { tx.run( "invalid" ); } catch ( ClientException e ) {}

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Cannot run more statements in this transaction, because previous statements in the " +
                                 "transaction has failed and the transaction has been rolled back. Please start a new" +
                                 " transaction to run another statement." );

        // When
        tx.run( "invalid statement" );
    }

    @Test
    public void shouldAllowNewStatementAfterAFailure() throws Throwable
    {
        // Given an error has occurred
        try { session.run( "invalid" ); } catch ( ClientException e ) {}

        // When
        int val = session.run( "RETURN 1" ).single().get( "1" ).javaInteger();

        // Then
        assertThat( val, equalTo( 1 ) );
    }

    @Test
    public void shouldAllowNewTransactionAfterFailure() throws Throwable
    {
        // Given an error has occurred in a prior transaction
        try ( Transaction tx = session.newTransaction() )
        {
            tx.run( "invalid" );
        }
        catch ( ClientException e ) {}

        // When
        try ( Transaction tx = session.newTransaction() )
        {
            int val = tx.run( "RETURN 1" ).single().get( "1" ).javaInteger();

            // Then
            assertThat( val, equalTo( 1 ) );
        }
    }

    @Test
    public void shouldExplainConnectionError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Unable to connect to 'localhost' on port 7777, ensure the database is running " +
                                 "and that there is a working network connection to it." );

        // When
        Neo4j.session( "neo4j://localhost:7777" );
    }

}
