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
package org.neo4j.kernel.ha.backup;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.backup.BackupExtensionService;
import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents;
import org.neo4j.cluster.protocol.election.CoordinatorIncapableCredentialsProvider;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;

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
        logger.debug( "Asking cluster member at '" + address
                + "' for master" );

        String clusterName = args.get( ClusterSettings.cluster_name.name(), null );
        if ( clusterName == null )
        {
            clusterName = args.get( ClusterSettings.cluster_name.name(), ClusterSettings.cluster_name.getDefaultValue() );
        }

        try
        {
            master = getMasterServerInCluster( address.getSchemeSpecificPart().substring(
                    2 ), clusterName, logging ); // skip the "//" part

            logger.debug( "Found master '" + master + "' in cluster" );
            return URI.create( master );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getMessage() );
        }
    }

    private String getMasterServerInCluster( String from, String clusterName, final Logging logging )
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
                new CoordinatorIncapableCredentialsProvider() ) );
        ClusterMemberEvents events = life.add( new PaxosClusterMemberEvents( clusterClient, clusterClient,
                clusterClient, new SystemOutLogging() ) );

        final Semaphore infoReceivedLatch = new Semaphore( 0 );
        final AtomicReference<URI> backupUri = new AtomicReference<URI>(  );
        events.addClusterMemberListener( new ClusterMemberListener.Adapter()
        {
            Map<URI, URI> backupUris = new HashMap<URI, URI>();
            URI master = null;

            @Override
            public void memberIsAvailable( String role, URI clusterUri, URI roleUri )
            {
                if ( OnlineBackupKernelExtension.BACKUP.equals( role ) )
                {
                    backupUris.put( clusterUri, roleUri );
                }
                else if ( HighAvailabilityModeSwitcher.MASTER.equals( role ) )
                {
                    master = clusterUri;
                }

                if ( master != null && backupUris.containsKey( master ) )
                {
                    backupUri.set( backupUris.get( master ) );
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

        return backupUri.get().toString();
    }
}
