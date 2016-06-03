/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.HTTP.RawPayload;
import org.neo4j.test.server.HTTP.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.test.Assert.assertEventually;
import static org.neo4j.test.server.HTTP.Builder;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.withBaseUri;

public class TransactionTerminationIT
{
    private final Neo4jRule neo4j = new Neo4jRule()
            .withConfig( ServerSettings.auth_enabled, Settings.FALSE )
            .withConfig( KernelTransactions.tx_termination_aware_locks, Settings.TRUE );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( neo4j );

    private GraphDatabaseService db;
    private Builder http;

    @Before
    public void setUp() throws Exception
    {
        db = neo4j.getGraphDatabaseService();
        http = withBaseUri( neo4j.httpURI().toString() );
    }

    @Test
    public void terminateTransactionThatWaitsForLock() throws Exception
    {
        final String labelName = "foo";
        final String property = "bar";
        final long value1 = 1L;
        final long value2 = 2L;

        createNode( labelName );

        final Response tx1 = startTx();
        final Response tx2 = startTx();

        assertNumberOfActiveTransactions( 2 );

        Response update1 = executeUpdateStatement( tx1, labelName, property, value1 );
        assertThat( update1.status(), equalTo( 200 ) );
        assertThat( update1, containsNoErrors() );

        final CountDownLatch latch = new CountDownLatch( 1 );
        Future<?> tx2Result = Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                latch.countDown();
                Response update2 = executeUpdateStatement( tx2, labelName, property, value2 );
                assertEquals( 200, update2.status() );
                assertThat( update2, hasErrors( Status.Statement.ExecutionFailure ) );
                assertThat( update2.rawContent(), containsString( LockClientStoppedException.class.getSimpleName() ) );
            }
        } );

        assertTrue( latch.await( 1, TimeUnit.MINUTES ) );
        Thread.sleep( 2000 );

        terminate( tx2 );
        commit( tx1 );

        Response update3 = executeUpdateStatement( tx2, labelName, property, value2 );
        assertThat( update3.status(), equalTo( 404 ) );

        tx2Result.get( 1, TimeUnit.MINUTES );

        assertSingleNodeExists( labelName, property, value1 );
    }

    private void createNode( String labelName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( DynamicLabel.label( labelName ) );
            tx.success();
        }
    }

    private Response startTx()
    {
        Response tx = http.POST( "db/data/transaction" );
        assertThat( tx.status(), equalTo( 201 ) );
        assertThat( tx, containsNoErrors() );
        return tx;
    }

    private void commit( Response tx ) throws JsonParseException
    {
        http.POST( tx.stringFromContent( "commit" ) );
    }

    private void terminate( Response tx )
    {
        http.DELETE( tx.location() );
    }

    private Response executeUpdateStatement( Response tx, String labelName, String property, long value )
    {
        String updateQuery = "MATCH (n:" + labelName + ") SET n." + property + "=" + value;
        RawPayload json = quotedJson( "{'statements': [{'statement':'" + updateQuery + "'}]}" );
        return http.POST( tx.location(), json );
    }

    private void assertNumberOfActiveTransactions( int expectedCount )
    {
        ThrowingSupplier<Integer,RuntimeException> txCountSupplier = new ThrowingSupplier<Integer,RuntimeException>()
        {
            @Override
            public Integer get() throws RuntimeException
            {
                return activeTxCount();
            }
        };

        assertEventually( "Wrong active tx count", txCountSupplier, equalTo( expectedCount ), 1, TimeUnit.MINUTES );
    }

    private int activeTxCount()
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        KernelTransactions kernelTransactions = resolver.resolveDependency( KernelTransactions.class );
        return kernelTransactions.activeTransactions().size();
    }

    private void assertSingleNodeExists( String labelName, String property, long value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = single( db.findNodes( DynamicLabel.label( labelName ) ) );
            assertEquals( value, node.getProperty( property ) );
            tx.success();
        }
    }
}
