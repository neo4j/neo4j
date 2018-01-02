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
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.function.Functions;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.ha.cluster.member.ClusterMembers.inRole;

/**
 * Keeps active connections to {@link Slave slaves} for a master to communicate to
 * when so needed.
 */
public class HighAvailabilitySlaves implements Lifecycle, Slaves
{
    private final LifeSupport life = new LifeSupport();
    private final Map<ClusterMember, Slave> slaves = new CopyOnWriteHashMap<ClusterMember, Slave>();
    private final ClusterMembers clusterMembers;
    private final Cluster cluster;
    private final SlaveFactory slaveFactory;
    private final HostnamePort me;
    private HighAvailabilitySlaves.HASClusterListener clusterListener;

    public HighAvailabilitySlaves( ClusterMembers clusterMembers, Cluster cluster, SlaveFactory slaveFactory,
            HostnamePort me )
    {
        this.clusterMembers = clusterMembers;
        this.cluster = cluster;
        this.slaveFactory = slaveFactory;
        this.me = me;
    }

    private Function<ClusterMember, Slave> slaveForMember()
    {
        return new Function<ClusterMember, Slave>()
        {
            @Override
            public Slave apply( ClusterMember from )
            {
                synchronized ( HighAvailabilitySlaves.this )
                {
                    Slave presentSlave = slaves.get( from );
                    if ( presentSlave == null )
                    {
                        presentSlave = slaveFactory.newSlave( life, from, me.getHost(), me.getPort() );
                        slaves.put( from, presentSlave );
                    }
                    return presentSlave;
                }
            }
        };
    }

    @Override
    public Iterable<Slave> getSlaves()
    {
        // Return all cluster members which are currently SLAVEs,
        // are alive, and convert to Slave with a cache if possible
        return map( withDefaults( slaveForMember(), Functions.map( slaves ) ),
                        filter( inRole( HighAvailabilityModeSwitcher.SLAVE ),
                                clusterMembers.getAliveMembers() ) );
    }

    @Override
    public void init()
    {
        life.init();

        clusterListener = new HASClusterListener();
        cluster.addClusterListener( clusterListener );
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
        cluster.removeClusterListener( clusterListener );

        life.shutdown();
        slaves.clear();
    }

    private class HASClusterListener extends ClusterListener.Adapter
    {
        @Override
        public void elected( String role, InstanceId instanceId, URI electedMember )
        {
            if ( role.equals( ClusterConfiguration.COORDINATOR ) )
            {
                life.clear();
                slaves.clear();
            }
        }
    }
}
