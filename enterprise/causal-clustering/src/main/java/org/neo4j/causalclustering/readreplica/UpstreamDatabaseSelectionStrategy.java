/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.readreplica;

import java.util.Optional;

import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;

public abstract class UpstreamDatabaseSelectionStrategy extends Service
{
    protected TopologyService topologyService;
    protected Config config;
    protected MemberId myself;

    public UpstreamDatabaseSelectionStrategy( String key, String... altKeys )
    {
        super( key, altKeys );
    }

    // Service loaded can't inject this via the constructor
    void setTopologyService( TopologyService topologyService )
    {
        this.topologyService = topologyService;
    }

    void setConfig( Config config )
    {
        this.config = config;
    }

    void setMyself( MemberId myself )
    {
        this.myself = myself;
    }

    public abstract Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException;

    @Override
    public String toString()
    {
        return nicelyCommaSeparatedList( getKeys() );
    }

    private static String nicelyCommaSeparatedList( Iterable<String> keys )
    {
        StringBuilder sb = new StringBuilder();
        for ( String key : keys )
        {
            sb.append( key );
            sb.append( "," );
            sb.append( " " );
        }

        int trimThese = sb.lastIndexOf( ", " );
        if ( trimThese > 1 )
        {
            sb.replace( trimThese, sb.length(), "" );
        }

        return sb.toString();
    }
}
