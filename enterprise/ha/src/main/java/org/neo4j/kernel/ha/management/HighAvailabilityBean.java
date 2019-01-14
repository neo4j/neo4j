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
package org.neo4j.kernel.ha.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Format;
import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;

@Service.Implementation( ManagementBeanProvider.class )
public final class HighAvailabilityBean extends ManagementBeanProvider
{
    public HighAvailabilityBean()
    {
        super( HighAvailability.class );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        if ( !isHA( management ) )
        {
            return null;
        }
        return new HighAvailabilityImpl( management, true );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        if ( !isHA( management ) )
        {
            return null;
        }
        return new HighAvailabilityImpl( management );
    }

    private static boolean isHA( ManagementData management )
    {
        return management.getKernelData().graphDatabase() instanceof HighlyAvailableGraphDatabase;
    }

    private static class HighAvailabilityImpl extends Neo4jMBean implements HighAvailability
    {
        private final HighlyAvailableKernelData kernelData;

        HighAvailabilityImpl( ManagementData management )
                throws NotCompliantMBeanException
        {
            super( management );
            this.kernelData = (HighlyAvailableKernelData) management.getKernelData();
        }

        HighAvailabilityImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            this.kernelData = (HighlyAvailableKernelData) management.getKernelData();
        }

        @Override
        public String getInstanceId()
        {
            return kernelData.getMemberInfo().getInstanceId();
        }

        @Override
        public ClusterMemberInfo[] getInstancesInCluster()
        {
            return kernelData.getClusterInfo();
        }

        @Override
        public String getRole()
        {
            return kernelData.getMemberInfo().getHaRole();
        }

        @Override
        public boolean isAvailable()
        {
            return kernelData.getMemberInfo().isAvailable();
        }

        @Override
        public boolean isAlive()
        {
            return kernelData.getMemberInfo().isAlive();
        }

        @Override
        public String getLastUpdateTime()
        {
            long lastUpdateTime = kernelData.getMemberInfo().getLastUpdateTime();
            return lastUpdateTime == 0 ? "N/A" : Format.date( lastUpdateTime );
        }

        @Override
        public long getLastCommittedTxId()
        {
            return kernelData.getMemberInfo().getLastCommittedTxId();
        }

        @Override
        public String update()
        {
            long time = System.currentTimeMillis();
            try
            {
                kernelData.graphDatabase()
                        .getDependencyResolver()
                        .resolveDependency( UpdatePuller.class )
                        .pullUpdates();
            }
            catch ( Exception e )
            {
                return "Update failed: " + e;
            }
            time = System.currentTimeMillis() - time;
            return "Update completed in " + time + "ms";
        }
    }
}
