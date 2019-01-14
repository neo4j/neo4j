/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

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
    private final Map<ClusterMember, Slave> slaves = new CopyOnWriteHashMap<>();
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
        return from ->
        {
            synchronized ( HighAvailabilitySlaves.this )
            {
                return slaves.computeIfAbsent( from, f -> slaveFactory.newSlave( life, f, me.getHost(), me.getPort() ) );
            }
        };
    }

    @Override
    public Iterable<Slave> getSlaves()
    {
        // Return all cluster members which are currently SLAVEs,
        // are alive, and convert to Slave with a cache if possible
        return map( clusterMember ->
        {
            Slave slave = slaveForMember().apply( clusterMember );

            if ( slave == null )
            {
                return slaves.get( clusterMember );
            }
            else
            {
                return slave;
            }
        }, filter( inRole( HighAvailabilityModeSwitcher.SLAVE ), clusterMembers.getAliveMembers() ) );
    }

    @Override
    public void init()
    {
        life.init();

        clusterListener = new HASClusterListener();
        cluster.addClusterListener( clusterListener );
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
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
