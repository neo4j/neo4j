/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.test.Barrier;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class ThreadedTransactionPeriodicCommit<S>
{
    final Barrier.Control barrier = new Barrier.Control();
    private Future<String> loadCsvResult;
    private Future<Throwable> servCsvResult;

    int csvHttpPort = 8089;

    NeoInteractionLevel<S> neo;

    ThreadedTransactionPeriodicCommit( NeoInteractionLevel<S> neo )
    {
        this.neo = neo;
    }

    void execute( ThreadingRule threading, S subject, int nLines )
    {
        NamedFunction<Integer, Throwable> servCsv =
                new NamedFunction<Integer,Throwable>("serv-csv")
                {
                    @Override
                    public Throwable apply( Integer n ) throws RuntimeException
                    {
                        try
                        {
                            ServerSocket serverSocket = new ServerSocket( csvHttpPort );
                            Socket clientSocket = serverSocket.accept();
                            PrintWriter out = new PrintWriter( clientSocket.getOutputStream(), true );

                            // Start sending our reply, using the HTTP 1.1 protocol
                            out.print( "HTTP/1.1 200 \r\n" ); // Version & status code
                            out.print( "Content-Type: text/plain\r\n" ); // The type of data
                            out.print( "Connection: close\r\n" ); // Will close stream
                            out.print( "\r\n" ); // End of headers

                            for ( int i = 0; i < n - 1; i++ )
                            {
                                out.print( "line " + i + "\r\n" );
                            }

                            out.flush();

                            barrier.reached();

                            out.print( "line " + (n - 1) + "\r\n" );

                            out.close();

                            clientSocket.close();
                            serverSocket.close();

                            return null;
                        }
                        catch ( Throwable t )
                        {
                            return t;
                        }
                    }
                };

        NamedFunction<S, String> loadCsv =
                new NamedFunction<S,String>( "load-csv" )
                {
                    @Override
                    public String apply( S subject )
                    {
                        try
                        {
                            return neo.executeQuery(
                                    subject,
                                    "USING PERIODIC COMMIT 1 " +
                                    "LOAD CSV FROM 'http://localhost:" + csvHttpPort + "/file.csv' AS line " +
                                    "CREATE (l:Line {name: line[0]}) RETURN line[0] as name",
                                    null, r -> {}
                                );
                        }
                        catch (Throwable t)
                        {
                            return t.getMessage();
                        }
                    }
                };

        servCsvResult = threading.execute( servCsv, nLines );
        loadCsvResult = threading.execute( loadCsv, subject );
    }

    void closeAndAssertSuccess() throws Throwable
    {
        String exceptionMsgInOtherThread = join();
        if ( exceptionMsgInOtherThread != "" )
        {
            fail( "Expected no exception in ThreadedCreate, got '" + exceptionMsgInOtherThread + "'" );
        }
    }

    void closeAndAssertError( String errMsg ) throws Throwable
    {
        String exceptionMsgInOtherThread = join();
        assertThat( exceptionMsgInOtherThread, equalTo( errMsg ) );
    }

    private String join() throws ExecutionException, InterruptedException
    {
        barrier.release();
        servCsvResult.get();
        return loadCsvResult.get();
    }
}
