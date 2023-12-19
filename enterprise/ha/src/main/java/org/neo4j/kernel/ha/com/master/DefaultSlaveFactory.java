/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.com.master;

import java.util.function.Supplier;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public class DefaultSlaveFactory implements SlaveFactory
{
    private final LogProvider logProvider;
    private final Monitors monitors;
    private final int chunkSize;
    private StoreId storeId;
    private final Supplier<LogEntryReader<ReadableClosablePositionAwareChannel>> entryReader;

    public DefaultSlaveFactory( LogProvider logProvider, Monitors monitors, int chunkSize,
            Supplier<LogEntryReader<ReadableClosablePositionAwareChannel>> logEntryReader )
    {
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.chunkSize = chunkSize;
        this.entryReader = logEntryReader;
    }

    @Override
    public Slave newSlave( LifeSupport life, ClusterMember clusterMember, String originHostNameOrIp, int originPort )
    {
        return life.add( new SlaveClient( clusterMember.getInstanceId(), clusterMember.getHAUri().getHost(),
                clusterMember.getHAUri().getPort(), originHostNameOrIp, logProvider, storeId,
                2, // and that's 1 too many, because we push from the master from one thread only anyway
                chunkSize, monitors.newMonitor( ByteCounterMonitor.class, SlaveClient.class ),
                monitors.newMonitor( RequestMonitor.class, SlaveClient.class ), entryReader.get() ) );
    }

    @Override
    public void setStoreId( StoreId storeId )
    {
        this.storeId = storeId;
    }
}
