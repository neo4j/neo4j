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
package org.neo4j.kernel.impl.enterprise;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;

import static java.util.stream.Collectors.toSet;

public class StandardBoltConnectionTracker implements BoltConnectionTracker
{
    private Map<ManagedBoltStateMachine,String> sessions = new ConcurrentHashMap<>();

    @Override
    public void onRegister( ManagedBoltStateMachine machine, String owner )
    {
        sessions.put( machine, owner );
    }

    @Override
    public void onTerminate( ManagedBoltStateMachine machine )
    {
        sessions.remove( machine );
    }

    @Override
    public Set<ManagedBoltStateMachine> getActiveConnections()
    {
        return sessions.keySet().stream().collect( toSet() );
    }

    @Override
    public Set<ManagedBoltStateMachine> getActiveConnections( String owner )
    {
        return sessions
                .entrySet()
                .stream()
                .filter( entry -> entry.getValue().equals( owner ) )
                .map( Map.Entry::getKey ).collect( toSet() );
    }
}
