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
package org.neo4j.coreedge.server;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;

import static java.util.concurrent.TimeUnit.MINUTES;

public class Expiration
{
    private final Clock clock;
    private final long timeout;

    /**
     * Objects associated with this Expiration will expire after 2 minutes by default.
     * This is a reasonable time limit for resources like outbound socket connections etc.
     */
    public Expiration( Clock clock )
    {
        this( clock, 2, MINUTES );
    }

    public Expiration( Clock clock, long timeout, TimeUnit timeUnit )
    {
        this.clock = clock;
        this.timeout = timeUnit.toMillis( timeout );
    }

    class ExpirationTime
    {
        private volatile long expires;

        ExpirationTime()
        {
            renew();
        }

        boolean expired()
        {
            return clock.currentTimeMillis() > expires;
        }

        void renew()
        {
            expires = clock.currentTimeMillis() + timeout;
        }

        @Override
        public String toString()
        {
            return String.format( "ExpirationTime{expires=%d}", expires );
        }
    }
}
