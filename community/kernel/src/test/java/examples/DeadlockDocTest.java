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
package examples;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.TransactionTemplate;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.test.EmbeddedDatabaseRule;

public class DeadlockDocTest
{
    @Rule
    public EmbeddedDatabaseRule rule = new EmbeddedDatabaseRule(  );

    @Test
    public void transactionWithRetries() throws InterruptedException
    {
        Object result = transactionWithRetry();
    }

    @Test
    public void transactionWithTemplate() throws InterruptedException
    {
        GraphDatabaseService graphDatabaseService = rule.getGraphDatabaseService();

        // START SNIPPET: template
        TransactionTemplate template = new TransactionTemplate(  ).retries( 5 ).backoff( 3, TimeUnit.SECONDS );
        // END SNIPPET: template

        // START SNIPPET: usage-template
        Object result = template.with(graphDatabaseService).execute( new Function<Transaction, Object>()
        {
            @Override
            public Object apply( Transaction transaction ) throws RuntimeException
            {
                Object result = null;
                return result;
            }
        } );
        // END SNIPPET: usage-template
    }

    private Object transactionWithRetry()
    {
        GraphDatabaseService graphDatabaseService = rule.getGraphDatabaseService();

        // START SNIPPET: retry
        Throwable txEx = null;
        int RETRIES = 5;
        int BACKOFF = 3000;
        for ( int i = 0; i < RETRIES; i++ )
        {
            try ( Transaction tx = graphDatabaseService.beginTx() )
            {
                Object result = doStuff(tx);
                tx.success();
                return result;
            }
            catch ( Throwable ex )
            {
                txEx = ex;

                // Add whatever exceptions to retry on here
                if ( !(ex instanceof DeadlockDetectedException) )
                {
                    break;
                }
            }

            // Wait so that we don't immediately get into the same deadlock
            if ( i < RETRIES - 1 )
            {
                try
                {
                    Thread.sleep( BACKOFF );
                }
                catch ( InterruptedException e )
                {
                    throw new TransactionFailureException( "Interrupted", e );
                }
            }
        }

        if ( txEx instanceof TransactionFailureException )
        {
            throw ((TransactionFailureException) txEx);
        }
        else if ( txEx instanceof Error )
        {
            throw ((Error) txEx);
        }
        else if ( txEx instanceof RuntimeException )
        {
            throw ((RuntimeException) txEx);
        }
        else
        {
            throw new TransactionFailureException( "Failed", txEx );
        }
        // END SNIPPET: retry
    }

    private Object doStuff( Transaction tx )
    {
        return null;
    }
}
