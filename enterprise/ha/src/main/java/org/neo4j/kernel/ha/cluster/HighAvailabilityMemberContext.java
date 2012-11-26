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
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.Binding;
import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.client.ClusterClient;

/**
 * Context used by the {@link HighAvailabilityMemberStateMachine}. Keeps track of what elections and previously
 * available master this cluster member has seen.
 */
public class HighAvailabilityMemberContext
{
    private URI electedMasterId;
    private URI availableHaMasterId;
    private URI myId;

    public HighAvailabilityMemberContext( Binding binding )
    {
        binding.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                myId = me;
            }
        } );
    }

    public URI getMyId()
    {
        return myId;
    }

    public URI getElectedMasterId()
    {
        return electedMasterId;
    }

    public void setElectedMasterId( URI electedMasterId )
    {
        this.electedMasterId = electedMasterId;
    }

    public URI getAvailableHaMaster()
    {
        return availableHaMasterId;
    }

    public void setAvailableHaMasterId( URI availableHaMasterId )
    {
        this.availableHaMasterId = availableHaMasterId;
    }
}
