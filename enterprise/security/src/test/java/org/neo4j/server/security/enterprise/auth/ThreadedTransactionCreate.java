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

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.Barrier;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

public class ThreadedTransactionCreate<S>
{
    final Barrier.Control barrier = new Barrier.Control();
    private Future<Throwable> done;

    NeoInteractionLevel<S> neo;

    ThreadedTransactionCreate( NeoInteractionLevel<S> neo )
    {
        this.neo = neo;
    }

    void execute( ThreadingRule threading, S subject )
    {
        NamedFunction<S, Throwable> startTransaction =
                new NamedFunction<S, Throwable>( "start-transaction" )
                {
                    @Override
                    public Throwable apply( S subject )
                    {
                        try ( InternalTransaction tx = neo.startTransactionAsUser( subject ) )
                        {
                            barrier.reached();
                            neo.getGraph().execute( "CREATE (:Test { name: '" + neo.nameOf( subject ) + "-node'})" );
                            // We would like to put barrier.reached() here, but this seems to cause threading issues
                            tx.success();
                            return null;
                        }
                        catch (Throwable t)
                        {
                            return t;
                        }
                    }
                };

        done = threading.execute( startTransaction, subject );
    }

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
        assertTrue( "Expected TransactionTerminatedException in ThreadedCreate, got '"+exceptionInOtherThread.getMessage()
                +"'", exceptionInOtherThread instanceof TransactionTerminatedException );
    }

    void closeAndAssertException( Class exceptionType, String msg ) throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread == null )
        {
            fail( "Expected " + exceptionType + " in ThreadedCreate, but no exception was raised" );
        }
        assertTrue( "Expected " + exceptionType + " in ThreadedCreate, got " + exceptionInOtherThread.getClass() + "",
                 exceptionType.isInstance( exceptionInOtherThread ) );
        assertThat( exceptionInOtherThread.getMessage(), equalTo( msg ) );
    }

    private Throwable join() throws ExecutionException, InterruptedException
    {
        barrier.release();
        return done.get();
    }
}
