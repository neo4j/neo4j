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
package org.neo4j.kernel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.NotCompliantMBeanException;
import javax.management.remote.JMXServiceURL;

import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestContext.Tx;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.ha.AbstractHAGraphDatabase;
import org.neo4j.kernel.ha.ConnectionInformation;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.InstanceInfo;
import org.neo4j.management.SlaveInfo;
import org.neo4j.management.SlaveInfo.SlaveTransaction;

@Service.Implementation( ManagementBeanProvider.class )
public final class HighAvailabilityBean extends ManagementBeanProvider
{
    public HighAvailabilityBean()
    {
        super( HighAvailability.class );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management ) throws NotCompliantMBeanException
    {
        if ( !isHA( management ) ) return null;
        return new HighAvailibilityImpl( management, true );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        if ( !isHA( management ) ) return null;
        return new HighAvailibilityImpl( management );
    }

    private static boolean isHA( ManagementData management )
    {
        return management.getKernelData().graphDatabase() instanceof AbstractHAGraphDatabase;
    }

    private static class HighAvailibilityImpl extends Neo4jMBean implements HighAvailability
    {
        private final HighlyAvailableGraphDatabase db;

        HighAvailibilityImpl( ManagementData management )
                throws NotCompliantMBeanException
        {
            super( management );
            this.db = ((AbstractHAGraphDatabase) management.getKernelData().graphDatabase()).getHighlyAvailableGraphDatabase();
        }

        HighAvailibilityImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            this.db = ((AbstractHAGraphDatabase) management.getKernelData().graphDatabase()).getHighlyAvailableGraphDatabase();
        }

        public String getMachineId()
        {
            return Integer.toString( db.getMachineId() );
        }

        public InstanceInfo[] getInstancesInCluster()
        {
            ConnectionInformation[] connections = db.getBroker().getConnectionInformation();
            InstanceInfo[] result = new InstanceInfo[connections.length];
            for ( int i = 0; i < result.length; i++ )
            {
                ConnectionInformation connection = connections[i];
                JMXServiceURL jmxServiceURL = connection.getJMXServiceURL();
                result[i] = new InstanceInfo( jmxServiceURL == null ? null : jmxServiceURL.toString(),
                        connection.getInstanceId(), connection.getMachineId(),
                        connection.isMaster(), connection.getLastCommitedTransactionId() );
            }
            return result;
        }

        public boolean isMaster()
        {
            return db.isMaster();
        }

        public SlaveInfo[] getConnectedSlaves()
        {
            MasterServer master = db.getMasterServerIfMaster();
            if ( master == null ) return null;
            List<SlaveInfo> result = new ArrayList<SlaveInfo>();
            for ( Map.Entry<Integer, Collection<RequestContext>> entry : master.getSlaveInformation().entrySet() )
            {
                result.add( slaveInfo( entry.getKey().intValue(), entry.getValue() ) );
            }
            return result.toArray( new SlaveInfo[result.size()] );
        }

        public String getLastUpdateTime()
        {
            return Format.date( db.lastUpdateTime() );
        }

        public String update()
        {
            long time = System.currentTimeMillis();
            try
            {
                db.pullUpdates();
            }
            catch ( Exception e )
            {
                return "Update failed: " + e;
            }
            time = System.currentTimeMillis() - time;
            return "Update completed in " + time + "ms";
        }

        private SlaveInfo slaveInfo( int machineId, Collection<RequestContext> contexts )
        {
            List<SlaveTransaction> txInfo = new ArrayList<SlaveTransaction>();
            for ( RequestContext context : contexts )
            {
                Map<String, Long> lastTransactions = new HashMap<String, Long>();
                for ( Tx tx : context.lastAppliedTransactions() )
                {
                    lastTransactions.put( tx.getDataSourceName(), tx.getTxId() );
                }
                txInfo.add( new SlaveTransaction( context.getEventIdentifier(), lastTransactions ) );
            }
            ConnectionInformation connection = db.getBroker().getConnectionInformation( machineId );
            JMXServiceURL jmxServiceURL = connection.getJMXServiceURL();
            return new SlaveInfo( jmxServiceURL == null ? null : jmxServiceURL.toString(),
                    connection.getInstanceId(), machineId, false,
                    connection.getLastCommitedTransactionId(),
                    txInfo.toArray( new SlaveTransaction[txInfo.size()] ) );
        }
    }
}
