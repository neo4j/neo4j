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
package org.neo4j.backup;

import org.apache.zookeeper.KeeperException;
import org.neo4j.com.ComException;
import org.neo4j.com.backup.OnlineBackup;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class Backup
{
    private static final String TO = "to";
    private static final String FROM_HA = "from-ha";
    private static final String FROM = "from";
    private static final String INCREMENTAL = "incremental";
    private static final String FULL = "full";

    public static void main( String[] args )
    {
        Args arguments = new Args( args );
        boolean full = arguments.has( FULL );
        boolean incremental = arguments.has( INCREMENTAL );
        if ( full&incremental || !(full|incremental) )
        {
            System.out.println( "Specify either " + dash( FULL ) + " or " + dash( INCREMENTAL ) );
            exitAbnormally();
        }
        
        String from = arguments.get( FROM, null );
        String fromHa = arguments.get( FROM_HA, null );
        if ( (from != null && fromHa != null) || (from == null && fromHa == null) )
        {
            System.out.println( "Specify either " + dash( FROM ) + " or " + dash( FROM_HA ) );
            exitAbnormally();
        }
        
        String to = arguments.get( TO, null );
        if ( to == null )
        {
            System.out.println( "Specify target location with " + dash( TO ) + " <target-directory>" );
            exitAbnormally();
        }
        
        if ( fromHa != null )
        {
            // This means we're trying to reach an HA cluster, a ZooKeeper service
            // managing that cluster, that is.
            try
            {
                System.out.println( "Asking ZooKeeper service at '" + fromHa + "' for master" );
                from = getMasterServerInCluster( fromHa );
                System.out.println( "Found master '" + from + "' in cluster" );
            }
            catch ( ComException e )
            {
                System.out.println( e.getMessage() );
                exitAbnormally();
            }
            catch ( RuntimeException e )
            {
                if ( e.getCause() instanceof KeeperException )
                {
                    KeeperException zkException = (KeeperException) e.getCause();
                    System.out.println( "Couldn't connect to '" + fromHa + "', " + zkException.getMessage() );
                    exitAbnormally();
                }
                throw e;
            }
        }
        
        doBackup( full, from, to );
    }

    private static void doBackup( boolean trueForFullFalseForIncremental, String from, String to )
    {
        OnlineBackup backup = newOnlineBackup( from );
        try
        {
            if ( trueForFullFalseForIncremental )
            {
                System.out.println( "Performing full backup from '" + from + "'" );
                backup.full( to );
            }
            else
            {
                System.out.println( "Performing incremental backup from '" + from + "'" );
                backup.incremental( to );
            }
            System.out.println( "Done" );
        }
        catch ( ComException e )
        {
            System.out.println( "Couldn't connect to '" + from + "', " + e.getMessage() );
            exitAbnormally();
        }
    }

    private static void exitAbnormally()
    {
        System.exit( 1 );
    }

    private static String dash( String name )
    {
        return "-" + name;
    }

    private static OnlineBackup newOnlineBackup( String from )
    {
        if ( from.contains( ":" ) )
        {
            int colonIndex = from.indexOf( ':' );
            String host = from.substring( 0, colonIndex );
            int port = Integer.parseInt( from.substring( colonIndex+1 ) );
            return OnlineBackup.from( host, port );
        }
        return OnlineBackup.from( from );
    }

    private static String getMasterServerInCluster( String from )
    {
        ClusterManager clusterManager = new ClusterManager( from );
        Pair<String, Integer> masterServer = null;
        try
        {
            clusterManager.waitForSyncConnected();
            Machine master = clusterManager.getMaster();
            masterServer = master.getServer();
            if ( masterServer != null )
            {
                int backupPort = clusterManager.getBackupPort( master.getMachineId() );
                return masterServer.first() + ":" + backupPort;
            }
            throw new ComException( "Master couldn't be found from cluster managed by " + from );
        }
        finally
        {
            clusterManager.shutdown();
        }
    }
}
