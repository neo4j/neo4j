package org.neo4j.server.security.enterprise.auth;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.Barrier;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.concurrent.ThreadingRule;

public class ThreadedTransactionCreate<S>
{
    final Barrier.Control barrier = new Barrier.Control();
    Future<Long> done;

    NeoInteractionLevel<S> neo;

    ThreadedTransactionCreate( NeoInteractionLevel<S> neo )
    {
        this.neo = neo;
    }

    NamedFunction<S, Long> startTransaction =
            new NamedFunction<S, Long>( "start-transaction" )
            {
                @Override
                public Long apply( S subject )
                {
                    try
                    {
                        InternalTransaction tx = neo.startTransactionAsUser( subject );
                        barrier.reached();
                        neo.getGraph().execute( "CREATE (:Test { name: '" + neo.nameOf( subject ) + "-node'})" );
                        tx.success();
                        tx.close();
                        return 0L;
                    }
                    catch (Throwable t)
                    {
                        return 1L;
                    }
                }
            };

    void execute( ThreadingRule threading, S subject )
    {
        done = threading.execute( startTransaction, subject );
    }

    Long close() throws ExecutionException, InterruptedException
    {
        barrier.release();
        return done.get();
    }
}
