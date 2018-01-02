/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
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
import static org.neo4j.ha.upgrade.RollingUpgradeIT.type1;
import static org.neo4j.ha.upgrade.RollingUpgradeIT.type2;
import static org.neo4j.ha.upgrade.Utils.execJava;
import static org.neo4j.helpers.Exceptions.launderedException;

public class LegacyDatabaseImpl extends UnicastRemoteObject implements LegacyDatabase
{
    // This has to be adapted to the way HA GDB is started in the specific old version it's used for.
    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.parse( args );
        String storeDir = arguments.orphans().get( 0 );

        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestHighlyAvailableGraphDatabaseFactory()
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
        db.beginTx().close();
    }

    @Override
    public long createNode() throws RemoteException
    {
        long result = -1;
        try( Transaction tx = db.beginTx() )
        {
            result = db.createNode().getId();
            tx.success();
        }
        return result;
    }

    @Override
    public long initialize()
    {
        long result = -1;
        try( Transaction tx = db.beginTx() )
        {
            Node center = db.createNode();
            result = center.getId();

            long largest = 0;
            for ( int i = 0; i < 100; i++ )
            {
                long current  = center.createRelationshipTo( db.createNode(), type1 ).getId();
                if ( current > largest )
                {
                    largest = current;
                }
            }

            for ( int i = 0; i < 100; i++ )
            {
                long current  = center.createRelationshipTo( db.createNode(), type2 ).getId();
                if ( current > largest )
                {
                    largest = current;
                }
            }

            for ( Relationship rel : center.getRelationships() )
            {
                rel.setProperty( "relProp", "relProp"+rel.getId()+"-"+largest );
                Node other = rel.getEndNode();
                other.setProperty( "nodeProp", "nodeProp"+other.getId()+"-"+largest );
            }
            tx.success();
        }
        return result;
    }

    @Override
    public void doComplexLoad( long center )
    {
        try( Transaction tx = db.beginTx() )
        {
            Node central = db.getNodeById( center );

            long[] type1RelId = new long[ 100 ];
            long[] type2RelId = new long[ 100 ];

            int index = 0;
            for ( Relationship relationship : central.getRelationships( type1 ) )
            {
                type1RelId[index++] = relationship.getId();
            }
            index = 0;
            for ( Relationship relationship : central.getRelationships( type2 ) )
            {
                type2RelId[index++] = relationship.getId();
            }

            // Delete the first half of each type
            Arrays.sort( type1RelId );
            Arrays.sort( type2RelId );

            for ( int i = 0; i < type1RelId.length / 2; i++ )
            {
                db.getRelationshipById( type1RelId[i] ).delete();
            }
            for ( int i = 0; i < type2RelId.length / 2; i++ )
            {
                db.getRelationshipById( type2RelId[i] ).delete();
            }

            // Go ahead and create relationships to make up for these deletes
            for ( int i = 0; i < type1RelId.length / 2; i++ )
            {
                central.createRelationshipTo( db.createNode(), type1 );
            }

            long largestCreated = 0;
            // The result is the id of the latest created relationship. We'll use that to set the properties
            for ( int i = 0; i < type2RelId.length / 2; i++ )
            {
                long current = central.createRelationshipTo( db.createNode(), type2 ).getId();
                if ( current > largestCreated )
                {
                    largestCreated = current;
                }
            }

            for ( Relationship relationship : central.getRelationships() )
            {
                relationship.setProperty( "relProp", "relProp" + relationship.getId() + "-" + largestCreated );
                Node end = relationship.getEndNode();
                end.setProperty( "nodeProp", "nodeProp" + end.getId() + "-" + largestCreated  );
            }

            tx.success();
        }
    }

    @Override
    public void verifyComplexLoad( long centralNode )
    {
        try( Transaction tx = db.beginTx() )
        {
            Node center = db.getNodeById( centralNode );
            long maxRelId = -1;
            for ( Relationship relationship : center.getRelationships() )
            {
                if (relationship.getId() > maxRelId )
                {
                    maxRelId = relationship.getId();
                }
            }

            int typeCount = 0;
            for ( Relationship relationship : center.getRelationships( type1 ) )
            {
                typeCount++;
                if ( !relationship.getProperty( "relProp" ).equals( "relProp" +relationship.getId()+"-"+maxRelId) )
                {
                    fail( "damn");
                }
                Node other = relationship.getEndNode();
                if ( !other.getProperty( "nodeProp" ).equals( "nodeProp"+other.getId()+"-"+maxRelId ) )
                {
                    fail("double damn");
                }
            }
            if ( typeCount != 100 )
            {
                fail("tripled damn");
            }

            typeCount = 0;
            for ( Relationship relationship : center.getRelationships( type2 ) )
            {
                typeCount++;
                if ( !relationship.getProperty( "relProp" ).equals( "relProp" +relationship.getId()+"-"+maxRelId) )
                {
                    fail( "damn2");
                }
                Node other = relationship.getEndNode();
                if ( !other.getProperty( "nodeProp" ).equals( "nodeProp"+other.getId()+"-"+maxRelId ) )
                {
                    fail("double damn2");
                }
            }
            if ( typeCount != 100 )
            {
                fail("tripled damn2");
            }
            tx.success();
        }
    }

    @Override
    public void verifyNodeExists( long id )
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
            db.getNodeById( id );
            tx.success();
        }
    }
    
    @Override
    public boolean isMaster() throws RemoteException
    {
        return ((HighlyAvailableGraphDatabase)db).isMaster();
    }
}
