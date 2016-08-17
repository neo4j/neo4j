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
package org.neo4j.coreedge.core.state;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.logging.Log;

class BindingProcess
{
    private final ClusterId localClusterId;
    private final Log log;

    BindingProcess( ClusterId localClusterId, Log log )
    {
        this.localClusterId = localClusterId;
        this.log = log;
    }

    ClusterId attempt( ClusterTopology topology ) throws InterruptedException, TimeoutException, BindingException
    {
        ClusterId commonClusterId = topology.clusterId();

        if ( commonClusterId != null )
        {
            if ( localClusterId == null )
            {
                log.info( "Binding to discovered cluster: " + commonClusterId );
            }
            else if ( commonClusterId.equals( localClusterId ) )
            {
                log.info( "Found expected cluster: " + commonClusterId );
            }
            else
            {
                throw new BindingException( String.format( "Cluster mismatch. Is the configuration correct? " +
                                                           "Expected: %s Discovered: %s", localClusterId, commonClusterId ) );
            }
        }
        else if ( topology.canBeBootstrapped() )
        {
            if ( localClusterId == null )
            {
                commonClusterId = new ClusterId( UUID.randomUUID() );
                log.info( "Bootstrapping cluster: " + commonClusterId );
            }
            else
            {
                commonClusterId = localClusterId;
            }
        }

        return commonClusterId;
    }
}
