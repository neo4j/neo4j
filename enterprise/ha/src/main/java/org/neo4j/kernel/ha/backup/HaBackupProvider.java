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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.backup.BackupExtensionService;
import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
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
        StringLogger logger = logging.getMessagesLog( HaBackupProvider.class );
        logger.debug( "Asking cluster member(s) at '" + address
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
        params.put( ClusterSettings.server_id.name(), "-1" );
        params.put( ClusterSettings.cluster_name.name(), clusterName );
        params.put( ClusterSettings.initial_hosts.name(), from );
        params.put(ClusterClient.clusterJoinTimeout.name(), "20s");
        final Config config = new Config( params,
                ClusterSettings.class, OnlineBackupSettings.class );

        final ClusterClient clusterClient = life.add( new ClusterClient( ClusterClient.adapt( config ), logging,
                new NotElectableElectionCredentialsProvider() ) );
        ClusterMemberEvents events = life.add( new PaxosClusterMemberEvents( clusterClient, clusterClient,
                clusterClient, clusterClient, new SystemOutLogging(),
                Predicates.<PaxosClusterMemberEvents.ClusterMembersSnapshot>TRUE() ) );

        // Refresh the snapshot once we join
        clusterClient.addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                clusterClient.refreshSnapshot();
                clusterClient.removeClusterListener( this );
            }
        });
        final Semaphore infoReceivedLatch = new Semaphore( 0 );
        final AtomicReference<URI> backupUri = new AtomicReference<URI>(  );
        events.addClusterMemberListener( new ClusterMemberListener.Adapter()
        {
            Map<InstanceId, URI> backupUris = new HashMap<InstanceId, URI>();
            InstanceId master = null;

            @Override
            public void memberIsAvailable( String role, InstanceId clusterUri, URI roleUri )
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

            /**
             * Called when new master has been elected. The new master may not be available a.t.m.
             * A call to {@link #memberIsAvailable} will confirm that the master given in
             * the most recent {@link #coordinatorIsElected(org.neo4j.cluster.InstanceId)} call is up and running as
             * master.
             *
             * @param coordinatorId the connection information to the master.
             */
            @Override
            public void coordinatorIsElected( InstanceId coordinatorId )
            {
            }
        } );

        try
        {
            life.start();

            if ( !infoReceivedLatch.tryAcquire( 20, TimeUnit.SECONDS ) )
            {
                throw new RuntimeException( "Could not find backup server in cluster " + clusterName + " at " + from + ", " +
                        "operation timed out" );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( LifecycleException e )
        {
            Throwable ex = Exceptions.peel( e, Exceptions.exceptionsOfType( TimeoutException.class ) );
            if ( ex != null )
            {
                throw new RuntimeException( "Could not find backup server in cluster " + clusterName + " at " + from + ", " +
                        "operation timed out" );
            }
            else
            {
                throw new RuntimeException(Exceptions.peel(e, new Predicate<Throwable>()
                {
                    @Override
                    public boolean accept( Throwable item )
                    {
                        return !(item instanceof LifecycleException);
                    }
                }));
            }
        }
        finally
        {
            life.shutdown();
        }

        return backupUri.get().toString();
    }
}
