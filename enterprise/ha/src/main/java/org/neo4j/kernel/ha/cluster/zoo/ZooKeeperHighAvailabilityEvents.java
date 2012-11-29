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
package org.neo4j.kernel.ha.cluster.zoo;

import java.net.URI;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.com.BindingNotifier;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.switchover.CompatibilityModeListener;
import org.neo4j.kernel.ha.switchover.Switchover;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

public class ZooKeeperHighAvailabilityEvents
        implements ClusterMemberEvents, ClusterMemberAvailability, BindingNotifier, Lifecycle
{
    private ZooClient client;
    private final Logging logger;
    private final Config config;
    private final Switchover switchover;
    private final int serverId;

    private Iterable<ClusterMemberListener> haListeners = Listeners.newListeners();
    private Iterable<BindingListener> bindingListeners = Listeners.newListeners();

    private final LifeSupport life;

    public ZooKeeperHighAvailabilityEvents( Logging logger, Config config, Switchover switchover )
    {
        this.logger = logger;
        this.config = config;
        this.switchover = switchover;
        this.life = new LifeSupport();
        this.serverId = config.get( HaSettings.server_id );
    }

    @Override
    public void init() throws Throwable
    {
        client = new ZooClient( logger.getLogger( ZooClient.class ), config );
        life.add( client );
        client.addZooListener( new ZooHaEventListener() );
        client.addCompatibilityModeListener( new ZooCompatibilityModeListener() );
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start(); // Binds ZooClient so we have a local address to use next
        Listeners.notifyListeners( bindingListeners, new Listeners.Notification<BindingListener>()
        {
            @Override
            public void notify( BindingListener listener )
            {
                listener.listeningAt( URI.create( client.getClusterServer() ) );
            }
        } );
        client.refreshMasterFromZooKeeper();
        Listeners.notifyListeners( haListeners, new Listeners.Notification<ClusterMemberListener>()
        {
            @Override
            public void notify( ClusterMemberListener listener )
            {
                listener.masterIsElected( URI.create( "cluster://" + client.getCachedMaster().getServerAsString() ) );
            }
        } );
        Listeners.notifyListeners( haListeners, new Listeners.Notification<ClusterMemberListener>()
        {
            @Override
            public void notify( ClusterMemberListener listener )
            {
                listener.memberIsAvailable( HighAvailabilityModeSwitcher.MASTER,
                        URI.create( "cluster://"+client.getCachedMaster().getServerAsString() ),
                                URI.create( "ha://" + client.getCachedMaster().getServerAsString() ) );
            }
        } );
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    @Override
    public void memberIsAvailable( final String role, final URI roleUri )
    {
        Listeners.notifyListeners( haListeners, new Listeners.Notification<ClusterMemberListener>()
        {
            @Override
            public void notify( ClusterMemberListener listener )
            {
                logger.getLogger( getClass() ).logMessage( "got member is available for me: "+ client.getClusterServer() );
                listener.memberIsAvailable( role,
                        URI.create( client.getClusterServer() ),
                                roleUri );
            }
        } );
    }

    @Override
    public void addClusterMemberListener( ClusterMemberListener listener )
    {
        haListeners = Listeners.addListener( listener, haListeners );
    }

    @Override
    public void removeClusterMemberListener( ClusterMemberListener listener )
    {
        haListeners = Listeners.removeListener( listener, haListeners );
    }

    @Override
    public void addBindingListener( BindingListener listener )
    {
        bindingListeners = Listeners.addListener( listener, bindingListeners );
    }

    @Override
    public void removeBindingListener( BindingListener listener )
    {
        bindingListeners = Listeners.removeListener( listener, bindingListeners );
    }

    @Override
    public void memberIsUnavailable( String role )
    {
    }

    private class ZooHaEventListener implements ZooListener
    {
        @Override
        public void newMasterRequired()
        {
            logger.getLogger( getClass() ).logMessage( "Refreshing master from zk, got "+
                    client.refreshMasterFromZooKeeper() );
        }

        @Override
        public void reconnect()
        {
            try
            {
                stop();
                start();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        }

        @Override
        public void masterNotify()
        {
            logger.getLogger( getClass() ).logMessage( "Got master notify" );
            Listeners.notifyListeners( haListeners, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override
                public void notify( ClusterMemberListener listener )
                {
                    listener.masterIsElected( URI.create( client.getCachedMaster().getServerAsString() ) );
                }
            } );
        }

        @Override
        public void masterRebound()
        {
            logger.getLogger( getClass() ).logMessage( "Got master rebound" );
            Listeners.notifyListeners( haListeners, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override
                public void notify( ClusterMemberListener listener )
                {
                    listener.memberIsAvailable( HighAvailabilityModeSwitcher.MASTER,
                            URI.create( "cluster://"+client.getCachedMaster().getServerAsString() ),
                                    URI.create( "ha://" + client.getCachedMaster().getServerAsString() ) );
                }
            } );
        }
    }

    private class ZooCompatibilityModeListener implements CompatibilityModeListener
    {
        @Override
        public void leftCompatibilityMode()
        {
            switchover.doSwitchover();
        }
    }
}
