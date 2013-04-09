/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperty;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.ha.upgrade.Utils.execJava;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.tooling.GlobalGraphOperations.at;

import java.io.File;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Args;
import org.neo4j.shell.impl.RmiLocation;
import org.neo4j.test.ProcessStreamHandler;

public class LegacyDatabaseImpl extends UnicastRemoteObject implements LegacyDatabase
{
    // This has to be adapted to the way HA GDB is started in 1.8
    public static void main( String[] args ) throws Exception
    {
        Args arguments = new Args( args );
        File storeDir = new File( arguments.orphans().get( 0 ) );
        
        GraphDatabaseService db = (GraphDatabaseService) LegacyDatabaseImpl.class.forName(
                "org.neo4j.kernel.HighlyAvailableGraphDatabase" ).getConstructor(
                        String.class, Map.class ).newInstance( storeDir.getAbsolutePath(), arguments.asMap() );

        LegacyDatabaseImpl legacyDb = new LegacyDatabaseImpl( storeDir.getAbsolutePath(), db );
        rmiLocation( parseInt( arguments.orphans().get( 1 ) ) ).bind( legacyDb );
    }
    
    private GraphDatabaseService db;
    private String storeDir;

    public LegacyDatabaseImpl( String storeDir, GraphDatabaseService db ) throws RemoteException
    {
        super();
        this.storeDir = storeDir;
        this.db = db;
    }
    
    public static LegacyDatabase start( String classpath, File storeDir, Map<String, String> config ) throws Exception
    {
        List<String> args = new ArrayList<String>();
        args.add( storeDir.getAbsolutePath() );
        int rmiPort = 7000 + parseInt( config.get( "ha.server_id" ) );
        args.add( "" + rmiPort );
        args.addAll( asList( new Args( config ).asArgs() ) );
        Process process = execJava( appendNecessaryTestClasses( classpath ), LegacyDatabaseImpl.class.getName(), args.toArray( new String[0] ) );
        new ProcessStreamHandler( process, false ).launch();
        
        RmiLocation rmiLocation = rmiLocation( rmiPort );
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

    private static String appendNecessaryTestClasses( String classpath )
    {
        for ( String path : getProperty( "java.class.path" ).split( File.pathSeparator ) )
            if ( path.contains( "test-classes" ) )
                classpath = classpath + File.pathSeparator + path;
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
            db.getClass().getDeclaredMethod( "pullUpdates" ).invoke( db );
        }
        catch ( Exception e )
        {
            throw launderedException( e );
        }
        
        for ( Node node : at( db ).getAllNodes() )
            if ( name.equals( node.getProperty( "name", null ) ) )
                return;
        fail( "Node " + id + " with name '" + name + "' not found" );
    }
}
