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
package org.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;

public class TopologyLookupException extends Exception
{
    public TopologyLookupException( Throwable cause )
    {
        super( cause );
    }

    public TopologyLookupException( MemberId memberId )
    {
        super( format( "Cannot find the target member %s socket address", memberId ) );
    }

    public TopologyLookupException( String message )
    {
        super( message );
    }
}
