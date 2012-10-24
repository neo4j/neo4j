/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

@Ignore
public class TestHardKillIT
{
    private static final File path = TargetDirectory.forTest( TestHardKillIT.class ).graphDbDir( true );

    private ProcessStreamHandler processHandler;

    @Test
    public void testMasterSwitchHappensOnKillMinus9() throws Exception
    {
        Process proc = null;
        try
        {
            proc = run( "1" );
            Thread.sleep( 12000 );
            HighlyAvailableGraphDatabase slave1 = startDb( 2 );
            HighlyAvailableGraphDatabase slave2 = startDb( 3 );

            assertTrue( !slave1.isMaster() );
            assertTrue( !slave2.isMaster() );

            proc.destroy();
            proc = null;

            Thread.sleep( 60000 );

            assertTrue( slave1.isMaster() );
            assertTrue( !slave2.isMaster() );
        }
        finally
        {
            if (proc != null)
            {
                proc.destroy();
            }
        }
    }

    private Process run( String machineId ) throws IOException
    {
        List<String> allArgs = new ArrayList<String>( Arrays.asList( "java", "-cp", System.getProperty( "java" +
                ".class.path" ), TestHardKillIT.class.getName() ) );
        allArgs.add( machineId );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ));
        processHandler = new ProcessStreamHandler( process, false );
        processHandler.launch();
        return process;
    }

    /*
     * Used to launch the master instance
     */
    public static void main( String[] args )
    {
        int machineId = Integer.parseInt( args[0] );

        HighlyAvailableGraphDatabase db = startDb( machineId );
    }

    private static HighlyAvailableGraphDatabase startDb( int serverId )
    {
        GraphDatabaseBuilder builder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( serverId ) )
                .setConfig( HaSettings.server_id, "" + serverId )
                .setConfig( HaSettings.ha_server, ":" + (8001 + serverId) )
                .setConfig( HaSettings.initial_hosts, "127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004" )
                .setConfig( HaSettings.cluster_server, "127.0.0.1:" + (5001 + serverId) )
                .setConfig( HaSettings.tx_push_factor, "0" )
                ;
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) builder.newGraphDatabase();
        Transaction tx = db.beginTx();
        tx.finish();
        try
        {
            Thread.sleep( 2000 );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        return db;
    }

    private static String path( int i )
    {
        return new File( path, "" + i ).getAbsolutePath();
    }
}
