/*
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
package org.neo4j.kernel.ha.backup;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterEntryDeniedException;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HANewSnapshotFunction;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.Log;
import org.neo4j.kernel.monitoring.Monitors;

//@Service.Implementation(BackupExtensionService.class)
public final class HaBackupProvider //extends BackupExtensionService
{
    public HaBackupProvider()
    {
//        super( "ha" );
    }

//    @Override
    public URI resolve( String address, Args args, LogService logService )
    {
        String master;
        Log log = logService.getInternalLog( HaBackupProvider.class );
        log.debug( "Asking cluster member(s) at '" + address + "' for master" );

        String clusterName = args.get( ClusterSettings.cluster_name.name(), null );
        if ( clusterName == null )
        {
            clusterName = args.get( ClusterSettings.cluster_name.name(), ClusterSettings.cluster_name.getDefaultValue
                    () );
        }

        try
        {
            master = getMasterServerInCluster( normalizeAddress( address ), clusterName, logService );
            log.debug( "Found master '" + master + "' in cluster" );
            return URI.create( master );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getMessage() );
        }
    }

    private static String normalizeAddress( String address )
    {
        int index = address.indexOf( "://" );
        if ( index != -1 )
        {
            return address.substring( index + 3 );
        }
        return address;
    }

    private String getMasterServerInCluster( String from, String clusterName, final LogService logService )
    {
        LifeSupport life = new LifeSupport();
        Map<String, String> params = new HashMap<>();
        params.put( ClusterSettings.server_id.name(), "-1" );
        params.put( ClusterSettings.cluster_name.name(), clusterName );
        params.put( ClusterSettings.initial_hosts.name(), from );
        params.put( ClusterSettings.instance_name.name(), "Backup" );
        params.put( ClusterClient.clusterJoinTimeout.name(), "20s" );
//        final Config config = new Config( params,
//                ClusterSettings.class, OnlineBackupSettings.class );
        final Config config = null;
        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();
        Monitors monitors = new Monitors();
        final ClusterClient clusterClient = life.add( new ClusterClient( monitors,
                ClusterClient.adapt( config ), logService,
                new NotElectableElectionCredentialsProvider(), objectStreamFactory, objectStreamFactory ) );
        ClusterMemberEvents events = life.add( new PaxosClusterMemberEvents( clusterClient, clusterClient,
                clusterClient, clusterClient, FormattedLogProvider.toOutputStream( System.out ),
                Predicates.<PaxosClusterMemberEvents.ClusterMembersSnapshot>alwaysTrue(), new HANewSnapshotFunction(),
                objectStreamFactory, objectStreamFactory, monitors.newMonitor( NamedThreadFactory.Monitor.class ) ) );

        // Refresh the snapshot once we join
        clusterClient.addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                clusterClient.performRoleElections();
                clusterClient.removeClusterListener( this );
            }
        } );
        final Semaphore infoReceivedLatch = new Semaphore( 0 );
        final AtomicReference<URI> backupUri = new AtomicReference<>();
        events.addClusterMemberListener( new ClusterMemberListener.Adapter()
        {
            Map<InstanceId, URI> backupUris = new HashMap<>();
            InstanceId master = null;

            @Override
            public void memberIsAvailable( String role, InstanceId clusterUri, URI roleUri, StoreId storeId )
            {

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
                throw new RuntimeException( "Could not find backup server in cluster " + clusterName + " at " + from
                        + ", " +
                        "operation timed out" );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( LifecycleException e )
        {
            Throwable ex = Exceptions.peel( e, Predicates.<Throwable>instanceOf( LifecycleException.class ) );

            if ( ex != null && ex instanceof ClusterEntryDeniedException )
            {
                // Someone else is doing a backup
                throw new RuntimeException( "Another backup client is currently performing backup; concurrent backups" +
                        " are not allowed" );
            }

            ex = Exceptions.peel( e, Predicates.<Throwable>instanceOf( TimeoutException.class ) );
            if ( ex != null )
            {
                throw new RuntimeException( "Could not find backup server in cluster " + clusterName + " at " + from
                        + ", " +
                        "operation timed out" );
            }
            else
            {
                throw new RuntimeException( Exceptions.peel( e, new Predicate<Throwable>()
                {
                    @Override
                    public boolean test( Throwable item )
                    {
                        return !(item instanceof LifecycleException);
                    }
                } ) );
            }
        }
        finally
        {
            life.shutdown();
        }

        return backupUri.get().toString();
    }
}
