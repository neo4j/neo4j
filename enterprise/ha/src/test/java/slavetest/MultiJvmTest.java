/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package slavetest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.ha.StandaloneDatabase;

public class MultiJvmTest extends AbstractHaTest
{
    private final List<StandaloneDatabase> jvms = new ArrayList<StandaloneDatabase>();

    @Override
    protected int addDb( Map<String, String> config, boolean awaitStarted ) throws Exception
    {
        int machineId = jvms.size();
        jvms.add( null );
        startDb( machineId, config, awaitStarted );
        return machineId;
    }
    
    @Override
    protected void startDb( int machineId, Map<String, String> config, boolean awaitStarted ) throws Exception
    {
        File slavePath = dbPath( machineId );
        StandaloneDatabase slaveJvm = spawnJvm( slavePath, machineId, buildExtraArgs( config ) );
        if ( awaitStarted )
        {
            slaveJvm.awaitStarted();
        }
        jvms.set( machineId, slaveJvm );
    }

    @Override
    protected void awaitAllStarted() throws Exception
    {
        for ( StandaloneDatabase jvm : jvms )
            jvm.awaitStarted();
    }

    protected static String[] buildExtraArgs( Map<String, String> config )
    {
        List<String> list = new ArrayList<String>();
        for ( Map.Entry<String, String> entry : config.entrySet() )
        {
            list.add( "-" + entry.getKey() );
            list.add( entry.getValue() );
        }
        return list.toArray( new String[list.size()] );
    }
    
    @Override
    protected void shutdownDb( int machineId )
    {
        jvms.get( machineId ).kill();
    }

    @After
    public void shutdownDbsAndVerify() throws Exception
    {
        shutdownDbs();

        if ( !shouldDoVerificationAfterTests() )
        {
            return;
        }
        
        GraphDatabaseService masterDb = new EmbeddedGraphDatabase( dbPath( 0 ).getAbsolutePath() );
        try
        {
            for ( int i = 1; i < jvms.size(); i++ )
            {
                GraphDatabaseService slaveDb = new EmbeddedGraphDatabase( dbPath( i ).getAbsolutePath() );
                try
                {
                    verify( masterDb, slaveDb );
                }
                finally
                {
                    slaveDb.shutdown();
                }
            }
        }
        finally
        {
            masterDb.shutdown();
        }
    }

    @Override
    protected void shutdownDbs() throws Exception
    {
        for ( StandaloneDatabase slave : jvms )
        {
            slave.shutdown();
        }
    }

    protected StandaloneDatabase spawnJvm( File path, int machineId, String... extraArgs ) throws Exception
    {
        return StandaloneDatabase.withFakeBroker( testName.getMethodName(), path.getAbsoluteFile(),
                machineId, 0, extraArgs );
    }

    @Override
    protected void pullUpdates( int... slaves ) throws Exception
    {
        if ( slaves.length == 0 )
        {
            for ( int i = 1; i < jvms.size(); i++ )
            {
                jvms.get( i ).pullUpdates();
            }
        }
        else
        {
            for ( int slave : slaves )
            {
                jvms.get( slave+1 ).pullUpdates();
            }
        }
    }

    @Override
    protected <T> T executeJob( Job<T> job, int onSlave ) throws Exception
    {
        return jvms.get( onSlave+1 ).executeJob( job );
    }

    @Override
    protected <T> T executeJobOnMaster( Job<T> job ) throws Exception
    {
        return jvms.get( 0 ).executeJob( job );
    }

    @Override
    protected void startUpMaster( Map<String, String> config ) throws Exception
    {
        Map<String, String> newConfig = new HashMap<String, String>( config );
        newConfig.put( "master", "true" );
        StandaloneDatabase com = spawnJvm( dbPath( 0 ), 0, buildExtraArgs( newConfig ) );
        if ( jvms.isEmpty() )
        {
            jvms.add( com );
        }
        else
        {
            jvms.set( 0, com );
        }
        com.awaitStarted();
    }

    @Override
    protected CommonJobs.ShutdownDispatcher getMasterShutdownDispatcher()
    {
        try
        {
            return new CommonJobs.ShutdownJvm( jvms.get( 0 ) );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected Fetcher<DoubleLatch> getDoubleLatch() throws Exception
    {
        return new MultiJvmDLFetcher();
    }

    @Test
    public void testCancelledCopyWithSuccessfulRetry() throws Exception
    {
        createBigMasterStore( 200 );
        startUpMaster( MapUtil.stringMap() );
        AtomicBoolean called = new AtomicBoolean();
        Thread shutdownThread = startThreadWhichWillShutdownDb( called, 1, 100 );
        int slaveMachineId = addDb( MapUtil.stringMap(), false );
        
        // This will be interrupted when the db is shutdown in the middle of the copying
        try
        {
            awaitAllStarted();
        }
        catch ( Exception e )
        {
            // Will be thrown because the db is shut down in the middle of the copying
        }
        shutdownThread.join();
        assertTrue( called.get() );
        
        int otherNeostoreFilesCount = 0;
        for ( File dbFile : dbPath( slaveMachineId ).listFiles() )
        {
            if ( dbFile.getName().equals( "neostore" ) )
            {
                fail( "The neostore file shouldn't have been copied at a cancelled copying" );
            }
            else if ( dbFile.getName().startsWith( "neostore." ) )
            {
                otherNeostoreFilesCount++;
            }
        }
        assertTrue( otherNeostoreFilesCount > 0 );
        
        startDb( slaveMachineId, MapUtil.stringMap(), true );
        awaitAllStarted();
    }

    private Thread startThreadWhichWillShutdownDb( final AtomicBoolean called, final int machineId,
            final int atLeastDbSizeMb )
    {
        Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                File path = dbPath( machineId );
                while ( true )
                {
                    int size = directorySizeMb( path );
                    if ( size >= atLeastDbSizeMb )
                    {
                        called.set( true );
                        shutdownDb( machineId );
                        break;
                    }
                    sleeep( 10 );
                }
            }

            private int directorySizeMb( File path )
            {
                if ( !path.exists() )
                {
                    return 0;
                }
                int totalBytes = 0;
                for ( File file : path.listFiles() )
                {
                    totalBytes += file.length();
                }
                return totalBytes / (1024*1024);
            }
        };
        thread.start();
        return thread;
    }
}
