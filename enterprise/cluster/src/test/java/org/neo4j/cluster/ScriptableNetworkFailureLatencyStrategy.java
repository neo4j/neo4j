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
package org.neo4j.cluster;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Network failure strategy where you can declare, as the system runs,
 * what failures exist in the system.
 */
public class ScriptableNetworkFailureLatencyStrategy
    implements NetworkLatencyStrategy
{
    List<String> nodesDown = new ArrayList<>();
    List<String[]> linksDown = new ArrayList<>();

    public ScriptableNetworkFailureLatencyStrategy nodeIsDown( String id )
    {
        nodesDown.add( id );
        return this;
    }

    public ScriptableNetworkFailureLatencyStrategy nodeIsUp( String id )
    {
        nodesDown.remove( id );
        return this;
    }

    public ScriptableNetworkFailureLatencyStrategy linkIsDown( String node1, String node2 )
    {
        linksDown.add( new String[]{node1, node2} );
        linksDown.add( new String[]{node2, node1} );
        return this;
    }

    public ScriptableNetworkFailureLatencyStrategy linkIsUp( String node1, String node2 )
    {
        linksDown.remove( new String[]{node1, node2} );
        linksDown.remove( new String[]{node2, node1} );
        return this;
    }

    @Override
    public long messageDelay( Message<? extends MessageType> message, String serverIdTo )
    {
        if ( nodesDown.contains( serverIdTo ) || nodesDown.contains( message.getHeader( Message.HEADER_FROM ) ) )
        {
            return LOST;
        }

        if ( linksDown.contains( new String[]{message.getHeader( Message.HEADER_FROM ), serverIdTo} ) )
        {
            return LOST;
        }

        return 0;
    }
}
