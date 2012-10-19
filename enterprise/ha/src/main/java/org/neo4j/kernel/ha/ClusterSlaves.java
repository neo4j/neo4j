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

import static org.neo4j.kernel.ha.cluster.ClusterMemberModeSwitcher.getServerId;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.com.ComSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.ClusterMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.ClusterMemberListener;
import org.neo4j.kernel.ha.cluster.ClusterMemberState;
import org.neo4j.kernel.ha.cluster.ClusterMemberStateMachine;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class ClusterSlaves implements Slaves, Lifecycle
{
    private volatile Map<URI /*Member cluster URI, not HA URI*/, Slave> slaves;
    protected final StringLogger msgLog;
    private StoreId storeId;
    private final Config config;
    private final LifeSupport life;
    private final ClusterListener clusterListener;

    public ClusterSlaves( ClusterMemberStateMachine clusterEvents, StringLogger msgLog, Config config,
                          XaDataSourceManager xaDsm )
    {
        this.msgLog = msgLog;
        this.config = config;
        this.life = new LifeSupport();
        this.slaves = new HashMap<URI, Slave>();
        clusterEvents.addClusterMemberListener( new SlavesClusterEventsListener() );
        xaDsm.addDataSourceRegistrationListener( new StoreIdSettingListener() );
        clusterListener = new SlavesClusterListener();
    }

    // TODO Creepy getter due to initialization problems between this and ClusterJoin
    ClusterListener getClusterListener()
    {
        return clusterListener;
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
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
    public Iterable<Slave> getSlaves()
    {
        return slaves.values();
    }

    private class SlavesClusterEventsListener implements ClusterMemberListener
    {
        private volatile boolean instantiateFullSlaveClients;
        private volatile URI previouslyElectedMaster;

        @Override
        public void masterIsElected( ClusterMemberChangeEvent event )
        {
            instantiateFullSlaveClients = event.getNewState() == ClusterMemberState.TO_MASTER || event.getNewState
                    () == ClusterMemberState.MASTER;
            URI masterUri = event.getServerClusterUri();
            if ( previouslyElectedMaster == null || !previouslyElectedMaster.equals( masterUri ) )
            {
                life.clear();
                slaves = new HashMap<URI, Slave>();
                previouslyElectedMaster = masterUri;
            }
        }

        @Override
        public void slaveIsAvailable( ClusterMemberChangeEvent event )
        {
            Map<URI, Slave> newSlaves = new HashMap<URI, Slave>( slaves );

            URI slaveHaUri = event.getServerHaUri();
            Slave slave = instantiateFullSlaveClients ?
                    life.add( new SlaveClient( getServerId( slaveHaUri ),
                            slaveHaUri.getHost(), slaveHaUri.getPort(), msgLog, storeId,
                            config.get( HaSettings.max_concurrent_channels_per_slave ),
                            config.get( ComSettings.com_chunk_size ) ) ) :
                    new SlaveInformation( slaveHaUri, getServerId( slaveHaUri ) );

            newSlaves.put( event.getServerClusterUri(), slave );
            slaves = newSlaves;
        }

        @Override
        public void instanceStops( ClusterMemberChangeEvent event )
        {
            slaveLeft( event.getServerHaUri() );
        }

        @Override
        public void masterIsAvailable( ClusterMemberChangeEvent event )
        {
        }
    }

    private class StoreIdSettingListener implements DataSourceRegistrationListener
    {
        @Override
        public void registeredDataSource( XaDataSource ds )
        {
            if ( ds.getName().equals( Config.DEFAULT_DATA_SOURCE_NAME ) )
            {
                NeoStoreXaDataSource neoXaDs = (NeoStoreXaDataSource) ds;
                storeId = neoXaDs.getStoreId();
            }
        }

        @Override
        public void unregisteredDataSource( XaDataSource ds )
        {
        }
    }

    private class SlavesClusterListener extends ClusterListener.Adapter
    {
        @Override
        public void leftCluster( URI member )
        {
            slaveLeft( member );
        }
    }

    protected Slave instantiateSlaveClient( URI haUri )
    {
        return life.add( new SlaveClient( getServerId( haUri ),
                haUri.getHost(), haUri.getPort(), msgLog, storeId,
                config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( ComSettings.com_chunk_size ) ) );
    }

    private void slaveLeft( URI slaveUri )
    {
        Map<URI, Slave> newSlaves = new HashMap<URI, Slave>( slaves );
        Slave slave = newSlaves.remove( slaveUri );
        life.remove( slave );
        slaves = newSlaves;
    }
}
