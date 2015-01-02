/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ha.upgrade;

import java.io.File;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.shell.impl.RmiLocation;
import org.neo4j.test.ProcessStreamHandler;

import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperty;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.fail;

import static org.neo4j.ha.upgrade.Utils.execJava;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.tooling.GlobalGraphOperations.at;

public class LegacyDatabaseImpl extends UnicastRemoteObject implements LegacyDatabase
{
    // This has to be adapted to the way HA GDB is started in the specific old version it's used for.
    public static void main( String[] args ) throws Exception
    {
        Args arguments = new Args( args );
        String storeDir = arguments.orphans().get( 0 );
        
        GraphDatabaseAPI db = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( storeDir )
                .setConfig( arguments.asMap() )
                .newGraphDatabase();

        LegacyDatabaseImpl legacyDb = new LegacyDatabaseImpl( storeDir, db );
        rmiLocation( parseInt( arguments.orphans().get( 1 ) ) ).bind( legacyDb );
    }
    
    private final GraphDatabaseAPI db;
    private final String storeDir;

    public LegacyDatabaseImpl( String storeDir, GraphDatabaseAPI db ) throws RemoteException
    {
        super();
        this.storeDir = storeDir;
        this.db = db;
    }
    
    public static Future<LegacyDatabase> start( String classpath, File storeDir, Map<String, String> config )
            throws Exception
    {
        List<String> args = new ArrayList<String>();
        args.add( storeDir.getAbsolutePath() );
        int rmiPort = 7000 + parseInt( config.get( "ha.server_id" ) );
        args.add( "" + rmiPort );
        args.addAll( asList( new Args( config ).asArgs() ) );
        final Process process = execJava( appendNecessaryTestClasses( classpath ),
                LegacyDatabaseImpl.class.getName(), args.toArray( new String[0] ) );
        new ProcessStreamHandler( process, false ).launch();
        
        final RmiLocation rmiLocation = rmiLocation( rmiPort );
        ExecutorService executor = newSingleThreadExecutor();
        Future<LegacyDatabase> future = executor.submit( new Callable<LegacyDatabase>()
        {
            @Override
            public LegacyDatabase call() throws Exception
            {
                long endTime = currentTimeMillis() + SECONDS.toMillis( 10000 );
                while ( currentTimeMillis() < endTime )
                {
                    try
                    {
                        return (LegacyDatabase) rmiLocation.getBoundObject();
                    }
                    catch ( RemoteException e )
                    {
                        // OK
                        sleep( 100 );
                    }
                }
                process.destroy();
                throw new IllegalStateException( "Couldn't get remote to legacy database" );
            }
        } );
        executor.shutdown();
        return future;
    }

    private static String appendNecessaryTestClasses( String classpath )
    {
        for ( String path : getProperty( "java.class.path" ).split( File.pathSeparator ) )
        {
            if ( path.contains( "test-classes" ) && !path.contains( File.separator + "kernel" + File.separator ) )
            {
                classpath = classpath + File.pathSeparator + path;
            }
        }
        return classpath;
    }

    private static RmiLocation rmiLocation( int rmiPort )
    {
        return RmiLocation.location( "127.0.0.1", rmiPort, "remote" );
    }
    
    @Override
    public int stop()
    {
        db.shutdown();
        System.exit( 0 );
        return 0;
    }

    @Override
    public String getStoreDir()
    {
        return storeDir;
    }

    @Override
    public void awaitStarted( long time, TimeUnit unit )
    {
        db.beginTx().finish();
    }

    @Override
    public long createNode( String name )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.setProperty( "name", name );
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }

    @Override
    public void verifyNodeExists( long id, String name )
    {
        try
        {
            db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        }
        catch ( Exception e )
        {
            throw launderedException( e );
        }
        
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : at( db ).getAllNodes() )
            {
                if ( name.equals( node.getProperty( "name", null ) ) )
                {
                    return;
                }
            }
            tx.success();
        }
        fail( "Node " + id + " with name '" + name + "' not found" );
    }
    
    @Override
    public boolean isMaster() throws RemoteException
    {
        return ((HighlyAvailableGraphDatabase)db).isMaster();
    }
}
