/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.discovery;

import java.util.Collections;
import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class HazelcastClient extends LifecycleAdapter implements EdgeDiscoveryService
{
    private final Log log;
    private HazelcastConnector connector;
    private HazelcastInstance hazelcastInstance;

    public HazelcastClient( HazelcastConnector connector, LogProvider logProvider )
    {
        this.connector = connector;
        log = logProvider.getLog( getClass() );
    }

    @Override
    public ClusterTopology currentTopology()
    {
        Set<Member> hazelcastMembers = Collections.emptySet();
        boolean attemptedConnection = false;

        while ( hazelcastMembers.isEmpty() && !attemptedConnection )
        {
            if ( hazelcastInstance == null )
            {
                try
                {
                    attemptedConnection = true;
                    hazelcastInstance = connector.connectToHazelcast();
                }
                catch ( IllegalStateException e )
                {
                    log.info( "Unable to connect to core cluster" );
                    break;
                }
            }

            try
            {
                hazelcastMembers = hazelcastInstance.getCluster().getMembers();
            }
            catch ( HazelcastInstanceNotActiveException e )
            {
                hazelcastInstance = null;
            }
        }
        return new HazelcastClusterTopology( hazelcastMembers );
    }

    @Override
    public void stop() throws Throwable
    {
        if ( hazelcastInstance != null )
        {
            hazelcastInstance.shutdown();
        }
    }
}
