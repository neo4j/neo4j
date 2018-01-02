/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.logging.async;

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class AsyncLogProvider implements LogProvider
{
    private final LogProvider provider;
    private final AsyncEventSender<AsyncLogEvent> events;

    public AsyncLogProvider( AsyncEventSender<AsyncLogEvent> events, LogProvider provider )
    {
        this.provider = provider;
        this.events = events;
    }

    @Override
    public Log getLog( Class loggingClass )
    {
        return new AsyncLog( events, provider.getLog( loggingClass ) );
    }

    @Override
    public Log getLog( String name )
    {
        return new AsyncLog( events, provider.getLog( name ) );
    }
}
