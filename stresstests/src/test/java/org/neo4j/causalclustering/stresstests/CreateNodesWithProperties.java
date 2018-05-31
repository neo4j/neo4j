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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helper.Workload;
import org.neo4j.kernel.impl.util.CappedLogger;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.RandomValues;

class CreateNodesWithProperties extends Workload
{
    private static final Label label = Label.label( "Label" );

    private final Cluster cluster;
    private final CappedLogger txLogger;
    private final boolean enableIndexes;

    private long txSuccessCount;
    private long txFailCount;

    CreateNodesWithProperties( Control control, Resources resources, Config config )
    {
        super( control );
        this.enableIndexes = config.enableIndexes();
        this.cluster = resources.cluster();
        Log log = resources.logProvider().getLog( getClass() );
        this.txLogger = new CappedLogger( log ).setTimeLimit( 5, TimeUnit.SECONDS, resources.clock() );
    }

    @Override
    public void prepare()
    {
        if ( enableIndexes )
        {
            setupIndexes( cluster );
        }
    }

    @Override
    protected void doWork()
    {
        txLogger.info( "SuccessCount: " + txSuccessCount + " FailCount: " + txFailCount );
        RandomValues randomValues = RandomValues.create();

        try
        {
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label );
                for ( int i = 1; i <= 8; i++ )
                {
                    node.setProperty( prop( i ), randomValues.nextValue().asObject() );
                }
                tx.success();
            } );
        }
        catch ( Throwable e )
        {
            txFailCount++;

            if ( isInterrupted( e ) || isTransient( e ) )
            {
                // whatever let's go on with the workload
                return;
            }

            throw new RuntimeException( e );
        }

        txSuccessCount++;
    }

    private static void setupIndexes( Cluster cluster )
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

    private boolean isTransient( Throwable e )
    {
        return e != null &&
                (e instanceof TimeoutException || e instanceof DatabaseShutdownException || e instanceof TransactionFailureException || isInterrupted(
                        e.getCause() ));
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
}
