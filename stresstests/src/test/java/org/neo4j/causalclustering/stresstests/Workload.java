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
package org.neo4j.causalclustering.stresstests;

import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helper.RepeatUntilCallable;

class Workload extends RepeatUntilCallable
{
    private Cluster cluster;
    private static final Label label = Label.label( "Label" );

    Workload( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster )
    {
        super( keepGoing, onFailure );
        this.cluster = cluster;
    }

    @Override
    protected void doWork()
    {
        try
        {
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label );
                for ( int i = 1; i <= 8; i++ )
                {
                    node.setProperty( prop( i ),
                            "let's add some data here so the transaction logs rotate more often..." );
                }
                tx.success();
            } );
        }
        catch ( Throwable e )
        {
            if ( isInterrupted( e ) || isTransient( e ) )
            {
                // whatever let's go on with the workload
                return;
            }

            throw new RuntimeException( e );
        }
    }

    private boolean isTransient( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof  TimeoutException || e instanceof DatabaseShutdownException ||
                e instanceof TransactionFailureException )
        {
            return true;
        }

        return isInterrupted( e.getCause() );
    }

    private boolean isInterrupted( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof InterruptedException )
        {
            Thread.interrupted();
            return true;
        }

        return isInterrupted( e.getCause() );
    }

    static void setupIndexes( Cluster cluster )
    {
        try
        {
            cluster.coreTx( ( db, tx ) ->
            {
                for ( int i = 1; i <= 8; i++ )
                {
                    db.schema().indexFor( label ).on( prop( i ) ).create();
                }
                tx.success();
            } );
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }

    private static String prop( int i )
    {
        return "prop" + i;
    }
}
