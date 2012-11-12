/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Exchanger;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;

public class RaceBetweenCommitAndGetMoreRelationshipsIT extends TimerTask
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );
    private static RaceBetweenCommitAndGetMoreRelationshipsIT instance;
    private final GraphDatabaseService graphdb;
    private final NodeManager nodeManager;
    private final Timer timer;
    private final Exchanger<Throwable> error = new Exchanger<Throwable>();

    /**
     * A hack to transport the test to the main thread through the debugger
     */
    public static boolean exception( Throwable err )
    {
        RaceBetweenCommitAndGetMoreRelationshipsIT race = instance;
        if ( race != null ) try
        {
            race.error.exchange( err );
        }
        catch ( InterruptedException e )
        {
            // ignore
        }
        return false;
    }

    private RaceBetweenCommitAndGetMoreRelationshipsIT(GraphDatabaseService graphdb, NodeManager nodeManager)
    {
        this.graphdb = graphdb;
        this.nodeManager = nodeManager;
        this.timer = new Timer( /*daemon:*/true );
    }

    public static void main( String... args )
    {
        String path = "target/commit-getMore-race";
        if ( args != null && args.length >= 1 )
        {
            path = args[0];
        }
        delete( new File( path ) );
        InternalAbstractGraphDatabase graphdb = new EmbeddedGraphDatabase( path );
        RaceBetweenCommitAndGetMoreRelationshipsIT race = instance = new RaceBetweenCommitAndGetMoreRelationshipsIT(
                graphdb, graphdb.getNodeManager() );
        try
        {
            race.execute();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    private static void delete( File file )
    {
        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
                delete( child );
        }
        else if ( !file.exists() )
        {
            return;
        }
        file.delete();
    }

    private void execute() throws Throwable
    {
        setup( 1000 );
        nodeManager.clearCache();
        timer.schedule( this, 10, 10 );
        Worker[] threads = { new Worker( "writer", error )
        {
            @Override
            void perform()
            {
                setup( 100 );
                if ( assertions() ) System.out.println( "created 100" );
            }
        }, new Worker( "reader", error )
        {
            @Override
            void perform()
            {
                try
                {
                    int count = 0;
                    for ( @SuppressWarnings( "unused" ) Relationship rel : graphdb.getReferenceNode()
                            .getRelationships() )
                    {
                        count++;
                    }
                    if ( count % 100 != 0 ) throw new IllegalStateException( "Not atomic!" );
                    if ( assertions() ) System.out.println( "counted relationships" );
                }
                catch ( InvalidRecordException ire )
                {
                    if ( assertions() ) ire.printStackTrace();
                    else System.err.println( ire );
                }
            }
        } };
        try
        {
            throw error.exchange( new Error( "this should never see the light of day" ) );
        }
        finally
        {
            cancel();
            for ( Worker worker : threads )
                worker.done();
        }
    }

    private static boolean assertions()
    {
        boolean assertions = false;
        assert ( assertions = true ) == true;
        return assertions;
    }

    protected void setup( int relCount )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Node root = graphdb.getReferenceNode();
            for ( int i = 0; i < relCount; i++ )
            {
                root.createRelationshipTo( graphdb.createNode(), TYPE );
            }

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Override
    public void run()
    {
        nodeManager.clearCache();
        if ( assertions() ) System.out.println( "cleared cache" );
    }

    private static abstract class Worker extends Thread
    {
        private final Exchanger<Throwable> error;
        private volatile boolean done;

        Worker( String name, Exchanger<Throwable> error )
        {
            super( name );
            this.error = error;
            start();
        }

        void done()
        {
            if ( done ) interrupt();
            this.done = true;
        }

        @Override
        public void run()
        {
            try
            {
                while ( !done )
                {
                    perform();
                }
            }
            catch ( Throwable err )
            {
                done = true;
                try
                {
                    error.exchange( err );
                }
                catch ( InterruptedException e )
                {
                    // ignore - we know where it came from
                }
            }
        }

        abstract void perform();
    }
}
