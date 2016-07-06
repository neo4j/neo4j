/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime.internal;

import io.netty.util.internal.ConcurrentSet;

import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.bolt.KillableUserSession;
import org.neo4j.kernel.api.bolt.SessionManager;

@Service.Implementation( SessionManager.class )
public class StandardSessionManager extends Service implements SessionManager
{
    public StandardSessionManager()
    {
        super( "standard-session-tracker" );
    }

    private Set<KillableUserSession> sessions = new ConcurrentSet<>();

    @Override
    public void sessionActivated( KillableUserSession session )
    {
        sessions.add( session );
    }

    @Override
    public void sessionHalted( KillableUserSession session )
    {
        sessions.remove( session );
    }

    @Override
    public Set<KillableUserSession> getActiveSessions()
    {
        return sessions.stream().collect( Collectors.toSet() );
    }
}
