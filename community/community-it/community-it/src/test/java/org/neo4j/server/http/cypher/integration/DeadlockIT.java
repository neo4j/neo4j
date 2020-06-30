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
package org.neo4j.server.http.cypher.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.DeadlockDetected;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@ExtendWith( OtherThreadExtension.class )
public class DeadlockIT extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( container().getBaseUri() );

    @Inject
    public OtherThreadRule otherThread;

    private final CountDownLatch secondNodeLocked = new CountDownLatch( 1 );

    @Test
    public void shouldReturnCorrectStatusCodeOnDeadlock() throws Exception
    {
        // Given
        try ( Transaction tx = graphdb().beginTx() )
        {
            tx.createNode( Label.label( "First" ) );
            tx.createNode( Label.label( "Second" ) );
            tx.commit();
        }

        // When I lock node:First
        HTTP.Response begin = http.POST( txUri(), quotedJson( "{ 'statements': [ { 'statement': 'MATCH (n:First) SET n.prop=1' } ] }" ) );

        // and I lock node:Second, and wait for a lock on node:First in another transaction
        otherThread.execute( writeToFirstAndSecond() );

        // and I wait for those locks to be pending
        assertTrue( secondNodeLocked.await( 10, TimeUnit.SECONDS ) );
        Thread.sleep( 1000 );

        // and I then try and lock node:Second in the first transaction
        HTTP.Response deadlock = http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'MATCH (n:Second) SET n.prop=1' } ] }" ) );

        // Then
        assertThat( deadlock.get( "errors" ).get( 0 ).get( "code" ).asText() ).isEqualTo( DeadlockDetected.code().serialize() );
    }

    private OtherThreadExecutor.WorkerCommand<Object, Object> writeToFirstAndSecond()
    {
        return state ->
        {
            HTTP.Response post = http.POST( txUri(), quotedJson( "{ 'statements': [ { 'statement': 'MATCH (n:Second) SET n.prop=1' } ] }" ) );
            secondNodeLocked.countDown();
            http.POST( post.location(), quotedJson( "{ 'statements': [ { 'statement': 'MATCH (n:First) SET n.prop=1' } ] }" ) );
            return null;
        };
    }
}
