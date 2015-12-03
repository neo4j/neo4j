/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Pair;

import static org.mockito.Mockito.mock;

public class ControlledRenewableTimeoutService implements RenewableTimeoutService
{
    private Map<TimeoutName, Pair<TimeoutHandler, RenewableTimeout>> handlers = new HashMap<>();

    @Override
    public RenewableTimeout create( TimeoutName name, long delayInMillis, long randomRangeInMillis, TimeoutHandler handler )
    {
        RenewableTimeout timeout = mock( RenewableTimeout.class );
        handlers.put( name, Pair.of( handler, timeout ) );
        return timeout;
    }

    public void invokeTimeout( TimeoutName name )
    {
        Pair<TimeoutHandler, RenewableTimeout> pair = handlers.get( name );
        pair.first().onTimeout( pair.other() );
    }
}
