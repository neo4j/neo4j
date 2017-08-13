/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

public class KernelTransactionTimeoutMonitorIT
{
    @Rule
    public DatabaseRule database = new EmbeddedDatabaseRule()
            .withSetting( GraphDatabaseSettings.transaction_monitor_check_interval, "100ms" );
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int NODE_ID = 0;
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception
    {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() throws Exception
    {
        executor.shutdown();
    }

    @Test( timeout = 30_000 )
    public void terminateExpiredTransaction() throws Exception
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }

        expectedException.expectMessage( "The transaction has been terminated." );

        try ( Transaction transaction = database.beginTx() )
        {
            Node nodeById = database.getNodeById( NODE_ID );
            nodeById.setProperty( "a", "b" );
            executor.submit( startAnotherTransaction() ).get();
        }
    }

    private Runnable startAnotherTransaction()
    {
        return () ->
        {
            try ( InternalTransaction transaction = database
                    .beginTransaction( KernelTransaction.Type.implicit, SecurityContext.AUTH_DISABLED, 1,
                            TimeUnit.SECONDS ) )
            {
                Node node = database.getNodeById( NODE_ID );
                node.setProperty( "c", "d" );
            }
        };
    }
}
