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

package org.neo4j.kernel.ha.backup;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.BackupExtensionService;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.HighAvailabilityEvents;
import org.neo4j.kernel.ha.cluster.HighAvailabilityListener;
import org.neo4j.kernel.ha.cluster.paxos.PaxosHighAvailabilityEvents;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;

@Service.Implementation(BackupExtensionService.class)
public final class HaBackupProvider extends BackupExtensionService
{
    public HaBackupProvider()
    {
        super( "ha" );
    }

    @Override
    public URI resolve( URI address, Args args, Logging logging )
    {
        String master = null;
        StringLogger logger = logging.getLogger( HaBackupProvider.class );
        logger.logMessage( "Asking cluster member at '" + address
                + "' for master" );

        String clusterName = args.get( ClusterSettings.cluster_name.name(), null );
        if ( clusterName == null )
        {
            clusterName = args.get( ClusterSettings.cluster_name.name(), ClusterSettings.cluster_name.getDefaultValue() );
        }

        master = getMasterServerInCluster( address.getSchemeSpecificPart().substring(
                2 ), clusterName, logging ); // skip the "//" part

        logger.logMessage( "Found master '" + master + "' in cluster" );
        URI toReturn = null;
        try
        {
            toReturn = new URI( master );
        }
        catch ( URISyntaxException e )
        {
            // no way
        }
        return toReturn;
    }

    private static String getMasterServerInCluster( String from, String clusterName, final Logging logging )
    {
        LifeSupport life = new LifeSupport();
        Map<String, String> params = new HashMap<String, String>();
        params.put( HaSettings.server_id.name(), "-1" );
        params.put( ClusterSettings.cluster_name.name(), clusterName );
        params.put( ClusterSettings.initial_hosts.name(), from );
        params.put( ClusterSettings.cluster_discovery_enabled.name(), "false" );
        final Config config = new Config( params,
                ClusterSettings.class, OnlineBackupSettings.class );

        ClusterClient clusterClient = life.add( new ClusterClient( ClusterClient.adapt( config ), logging,
                new BackupElectionCredentialsProvider() ) );
        HighAvailabilityEvents events = life.add( new PaxosHighAvailabilityEvents( PaxosHighAvailabilityEvents.adapt(
                config ), clusterClient, StringLogger.SYSTEM ) );
        final Semaphore infoReceivedLatch = new Semaphore( 0 );
        final ClusterInfoHolder addresses = new ClusterInfoHolder();

        events.addHighAvailabilityEventListener( new HighAvailabilityListener()
        {
            @Override
            public void masterIsElected( URI masterUri )
            {
            }

            @Override
            public void memberIsAvailable( String role, URI masterClusterUri, Iterable<URI> masterURIs )
            {
                if ( HighAvailabilityEvents.MASTER.equals( role ) )
                {
                    addresses.held = masterURIs;
                    infoReceivedLatch.release();
                }
            }

        } );

        life.start();

        try
        {
            if ( !infoReceivedLatch.tryAcquire( 10, TimeUnit.SECONDS ) )
            {
                throw new RuntimeException( "Could not find master in cluster " + clusterName + " at " + from + ", " +
                        "operation timed out" );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            life.shutdown();
        }

        String backupAddress = null;
        for ( URI uri : addresses.held )
        {
            if ( "backup".equals( uri.getScheme() ) )
            {
                backupAddress = uri.toString();
                break;
            }
        }
        return backupAddress;
    }

    private static final class ClusterInfoHolder
    {
        public Iterable<URI> held;
    }
}
