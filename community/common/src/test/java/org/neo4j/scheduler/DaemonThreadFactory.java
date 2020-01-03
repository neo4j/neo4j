/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class DaemonThreadFactory implements ThreadFactory
{
    private final String prefix;
    private final AtomicInteger counter = new AtomicInteger();

    public DaemonThreadFactory()
    {
        this( "DaemonThread" );
    }

    public DaemonThreadFactory( String prefix )
    {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread( Runnable runnable )
    {
        ThreadFactory factory = Executors.defaultThreadFactory();
        Thread thread = factory.newThread( runnable );
        thread.setDaemon( true );
        thread.setName( prefix + counter.getAndIncrement() );
        return thread;
    }
}
