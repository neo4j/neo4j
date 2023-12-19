/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;

class ThreadedTransaction<S>
{
    private volatile Future<Throwable> done;
    private final NeoInteractionLevel<S> neo;
    private final DoubleLatch latch;

    ThreadedTransaction( NeoInteractionLevel<S> neo, DoubleLatch latch  )
    {
        this.neo = neo;
        this.latch = latch;
    }

    String executeCreateNode( ThreadingRule threading, S subject )
    {
        final String query = "CREATE (:Test { name: '" + neo.nameOf( subject ) + "-node'})";
        return execute( threading, subject, query );
    }

    String execute( ThreadingRule threading, S subject, String query )
    {
        return doExecute( threading, subject, KernelTransaction.Type.explicit, false, query )[0];
    }

    String[] execute( ThreadingRule threading, S subject, String... queries )
    {
        return doExecute( threading, subject, KernelTransaction.Type.explicit, false, queries );
    }

    String executeEarly( ThreadingRule threading, S subject, KernelTransaction.Type txType, String query )
    {
        return doExecute( threading, subject, txType, true, query )[0];
    }

    String[] executeEarly( ThreadingRule threading, S subject, KernelTransaction.Type txType, String... queries )
    {
        return doExecute( threading, subject, txType, true, queries );
    }

    private String[] doExecute(
        ThreadingRule threading, S subject, KernelTransaction.Type txType, boolean startEarly, String... queries )
    {
        NamedFunction<S, Throwable> startTransaction =
                new NamedFunction<S, Throwable>( "threaded-transaction-" + Arrays.hashCode( queries ) )
                {
                    @Override
                    public Throwable apply( S subject )
                    {
                        try ( InternalTransaction tx = neo.beginLocalTransactionAsUser( subject, txType ) )
                        {
                            Result result = null;
                            try
                            {
                                if ( startEarly )
                                {
                                    latch.start();
                                }
                                for ( String query : queries )
                                {
                                    if ( result != null )
                                    {
                                        result.close();
                                    }
                                    result = neo.getLocalGraph().execute( query );
                                }
                                if ( !startEarly )
                                {
                                    latch.startAndWaitForAllToStart();
                                }
                            }
                            finally
                            {
                                if ( !startEarly )
                                {
                                    latch.start();
                                }
                                latch.finishAndWaitForAllToFinish();
                            }
                            result.close();
                            tx.success();
                            return null;
                        }
                        catch ( Throwable t )
                        {
                            return t;
                        }
                    }
                };

        done = threading.execute( startTransaction, subject );
        return queries;
    }

    void closeAndAssertSuccess() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread != null )
        {
            throw new AssertionError( "Expected no exception in ThreadCreate, but got one.", exceptionInOtherThread );
        }
    }

    void closeAndAssertExplicitTermination() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread == null )
        {
            fail( "Expected explicit TransactionTerminatedException in the threaded transaction, " +
                    "but no exception was raised" );
        }
        assertThat( Exceptions.stringify( exceptionInOtherThread ),
                exceptionInOtherThread.getMessage(), containsString( "Explicitly terminated by the user.") );
    }

    void closeAndAssertSomeTermination() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread == null )
        {
            fail( "Expected a TransactionTerminatedException in the threaded transaction, but no exception was raised" );
        }
        assertThat( Exceptions.stringify( exceptionInOtherThread ),
                exceptionInOtherThread, instanceOf( TransactionTerminatedException.class ) );
    }

    private Throwable join() throws ExecutionException, InterruptedException
    {
        return done.get();
    }
}
