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
package org.neo4j.server.security.enterprise.auth;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;

public class ThreadedTransactionCreate<S>
{
    private volatile Future<Throwable> done = null;
    private final NeoInteractionLevel<S> neo;
    private final DoubleLatch latch;

    ThreadedTransactionCreate( NeoInteractionLevel<S> neo, DoubleLatch latch  )
    {
        this.neo = neo;
        this.latch = latch;
    }

    String execute( ThreadingRule threading, S subject )
    {
        final String query = "CREATE (:Test { name: '" + neo.nameOf( subject ) + "-node'})";
        return execute( threading, subject, query );
    }

    String execute( ThreadingRule threading, S subject, String query )
    {
        NamedFunction<S, Throwable> startTransaction =
                new NamedFunction<S, Throwable>( "start-transaction-" + query.hashCode() )
                {
                    @Override
                    public Throwable apply( S subject )
                    {
                        try
                        {
                            try ( InternalTransaction tx = neo.startTransactionAsUser( subject ) )
                            {
                                Result result = neo.getGraph().execute( query );
                                latch.startAndWaitForAllToStart();
                                latch.waitForAllToFinish();
                                result.close();
                                tx.success();
                                return null;
                            }
                        }
                        catch (Throwable t)
                        {
                            return t;
                        }
                    }
                };

        done = threading.execute( startTransaction, subject );
        return query;
    }

    @SuppressWarnings( "ThrowableResultOfMethodCallIgnored" )
    void closeAndAssertSuccess() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread != null )
        {
            fail( "Expected no exception in ThreadedCreate, got '"+exceptionInOtherThread.getMessage()+"'" );
        }
    }

    void closeAndAssertTransactionTermination() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread == null )
        {
            fail( "Expected BridgeTransactionTerminatedException in ThreadedCreate, but no exception was raised" );
        }
        assertThat( exceptionInOtherThread.getMessage(), containsString( "Explicitly terminated by the user.") );
    }

    void finish() {
        latch.finish();
    }

    private Throwable join() throws ExecutionException, InterruptedException
    {
        return done.get();
    }
}
