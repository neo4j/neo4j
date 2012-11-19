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
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;
import java.util.Map;

import org.neo4j.cluster.ClusterMonitor;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.ha.Slave;
import org.neo4j.kernel.ha.SlaveFactory;
import org.neo4j.kernel.ha.Slaves;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

/**
 * Keeps active connections to {@link Slave slaves} for a master to communicate to
 * when so needed.
 * 
 * @author Mattias Persson
 */
public class HighAvailabilitySlaves extends AbstractHighAvailabilityMembers implements Lifecycle, Slaves
{
    private static final Predicate<SlaveContext> AVAILABLE = new Predicate<HighAvailabilitySlaves.SlaveContext>()
    {
        @Override
        public boolean accept( SlaveContext item )
        {
            return item.available;
        }
    };
    
    private final LifeSupport life = new LifeSupport();
    private final Map<URI, SlaveContext> slaves = new CopyOnWriteHashMap<URI, SlaveContext>();
    private SlaveFactory slaveFactory;
    
    public HighAvailabilitySlaves( ClusterMonitor clusterMonitor, HighAvailability highAvailability,
            SlaveFactory slaveFactory, Logging logging )
    {
        super( clusterMonitor, highAvailability );
        this.slaveFactory = slaveFactory;
        clusterMonitor.addHeartbeatListener( new LocalHeartbeatListener() );
    }
    
    private static class SlaveContext
    {
        private final Slave slave;
        private volatile boolean available = true;
        
        protected SlaveContext( Slave slave )
        {
            this.slave = slave;
        }
    }
    
    private class LocalHeartbeatListener extends HeartbeatListener.Adapter
    {
        @Override
        public void failed( URI server )
        {
            getSlave( server ).available = false;
        }

        @Override
        public void alive( URI server )
        {
            getSlave( server ).available = true;
        }
    }

    @Override
    public Iterable<Slave> getSlaves()
    {
        return new IterableWrapper<Slave, SlaveContext>( new FilteringIterable<SlaveContext>( slaves.values(), AVAILABLE ) )
        {
            @Override
            protected Slave underlyingObjectToObject( SlaveContext context )
            {
                return context.slave;
            }
        };
    }
    
    public SlaveContext getSlave( URI server )
    {
        SlaveContext slave = slaves.get( server );
        if ( slave == null )
            throw new IllegalStateException( "Slave for '" + server + "' not found" );
        return slave;
    }

    @Override
    protected void slaveIsAvailable( URI serverClusterUri, URI serverHaUri, boolean iAmMaster )
    {
        if ( iAmMaster )
        {
            Slave slave = life.add( slaveFactory.newSlave( serverHaUri ) );
            slaves.put( serverClusterUri, new SlaveContext( slave ) );
        }
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
    protected void newMasterElected()
    {
        life.clear();
        slaves.clear();
    }
}
