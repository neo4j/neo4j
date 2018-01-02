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
package org.neo4j.kernel.ha.com.master;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.logging.LogProvider;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

public class DefaultSlaveFactory implements SlaveFactory
{
    private final LogProvider logProvider;
    private final Monitors monitors;
    private final int chunkSize;
    private StoreId storeId;

    public DefaultSlaveFactory( LogProvider logProvider, Monitors monitors, int chunkSize )
    {
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.chunkSize = chunkSize;
    }

    @Override
    public Slave newSlave( LifeSupport life, ClusterMember clusterMember, String originHostNameOrIp, int originPort )
    {
        return life.add( new SlaveClient( clusterMember.getInstanceId(), clusterMember.getHAUri().getHost(),
                clusterMember.getHAUri().getPort(), originHostNameOrIp, logProvider, storeId,
                2, // and that's 1 too many, because we push from the master from one thread only anyway
                chunkSize, monitors.newMonitor( ByteCounterMonitor.class, SlaveClient.class ),
                monitors.newMonitor( RequestMonitor.class, SlaveClient.class ) ) );
    }

    @Override
    public void setStoreId( StoreId storeId )
    {
        this.storeId = storeId;
    }
}
