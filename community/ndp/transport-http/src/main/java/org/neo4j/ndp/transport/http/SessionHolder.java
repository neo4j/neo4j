/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.http;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.helpers.Clock;
import org.neo4j.ndp.runtime.Session;

public class SessionHolder implements SessionAcquisition
{
    private final Clock clock;
    private final Session session;
    private final AtomicBoolean acquired = new AtomicBoolean();
    private long lastReleaseTimestamp = 0;

    public SessionHolder( Clock clock, Session session )
    {
        this.clock = clock;
        this.session = session;
        this.lastReleaseTimestamp = clock.currentTimeMillis();
    }

    public boolean acquire()
    {
        return acquired.compareAndSet( false, true );
    }

    public void release()
    {
        this.lastReleaseTimestamp = clock.currentTimeMillis();
        acquired.set( false );
    }

    @Override
    public Session session()
    {
        return session;
    }

    @Override
    public boolean success()
    {
        return true;
    }

    @Override
    public boolean sessionExists()
    {
        return true;
    }

    public long lastTimeActive()
    {
        return lastReleaseTimestamp;
    }
}
