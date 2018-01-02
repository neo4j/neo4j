/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.transactional.integration;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class TransientErrorIT extends AbstractRestFunctionalTestBase
{
    @Rule
    public final OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    @Test( timeout = 60000 )
    public void deadlockShouldRollbackTransaction() throws Exception
    {
        // Given
        HTTP.Response initial = POST( txCommitUri(), quotedJson(
                "{'statements': [{'statement': 'CREATE (n1 {prop : 1}), (n2 {prop : 2})'}]}" ) );
        assertThat( initial.status(), is( 200 ) );
        assertThat( initial, containsNoErrors() );

        // When

        // tx1 takes a write lock on node1
        HTTP.Response firstInTx1 = POST( txUri(), quotedJson(
                "{'statements': [{'statement': 'MATCH (n {prop : 1}) SET n.prop = 3'}]}" ) );
        final long tx1 = extractTxId( firstInTx1 );

        // tx2 takes a write lock on node2
        HTTP.Response firstInTx2 = POST( txUri(), quotedJson(
                "{'statements': [{'statement': 'MATCH (n {prop : 2}) SET n.prop = 4'}]}" ) );
        long tx2 = extractTxId( firstInTx2 );

        // tx1 attempts to take a write lock on node2
        Future<HTTP.Response> future = otherThread.execute( new OtherThreadExecutor.WorkerCommand<Void,HTTP.Response>()
        {
            @Override
            public HTTP.Response doWork( Void state ) throws Exception
            {
                return POST( txUri( tx1 ), quotedJson(
                        "{'statements': [{'statement': 'MATCH (n {prop : 2}) SET n.prop = 5'}]}" ) );
            }
        } );

        // tx2 attempts to take a write lock on node1
        HTTP.Response secondInTx2 = POST( txUri( tx2 ), quotedJson(
                "{'statements': [{'statement': 'MATCH (n {prop : 1}) SET n.prop = 6'}]}" ) );

        HTTP.Response secondInTx1 = future.get();

        // Then
        assertThat( secondInTx1.status(), is( 200 ) );
        assertThat( secondInTx2.status(), is( 200 ) );

        // either tx1 or tx2 should fail because of the deadlock
        HTTP.Response failed;
        if ( containsError( secondInTx1 ) )
        {
            failed = secondInTx1;
        }
        else if ( containsError( secondInTx2 ) )
        {
            failed = secondInTx2;
        }
        else
        {
            failed = null;
            fail( "Either tx1 or tx2 is expected to fail" );
        }

        assertThat( failed, hasErrors( Status.Transaction.DeadlockDetected ) );

        // transaction was rolled back on the previous step and we can't commit it
        HTTP.Response commit = POST( failed.stringFromContent( "commit" ) );
        assertThat( commit.status(), is( 404 ) );
    }

    @Test
    public void unavailableCsvResourceShouldRollbackTransaction() throws JsonParseException
    {
        // Given
        HTTP.Response first = POST( txUri(), quotedJson( "{'statements': [{'statement': 'CREATE ()'}]}" ) );
        assertThat( first.status(), is( 201 ) );
        assertThat( first, containsNoErrors() );

        long txId = extractTxId( first );

        // When
        HTTP.Response second = POST( txUri( txId ), quotedJson(
                "{'statements': [{'statement': 'LOAD CSV FROM \\\"http://127.0.0.1/null/\\\" AS line " +
                "CREATE (a {name:line[0]})'}]}" ) );

        // Then

        // request fails because specified CSV resource is invalid
        assertThat( second.status(), is( 200 ) );
        assertThat( second, hasErrors( Status.Statement.ExternalResourceFailure ) );

        // transaction was rolled back on the previous step and we can't commit it
        HTTP.Response commit = POST( second.stringFromContent( "commit" ) );
        assertThat( commit.status(), is( 404 ) );
    }

    private static boolean containsError( HTTP.Response response ) throws JsonParseException
    {
        return response.get( "errors" ).iterator().hasNext();
    }
}
