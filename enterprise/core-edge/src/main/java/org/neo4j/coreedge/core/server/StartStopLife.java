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
package org.neo4j.coreedge.core.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class StartStopLife extends LifecycleAdapter
{
    private final List<Lifecycle> services = new ArrayList<>();

    @Override
    public synchronized void start() throws Throwable
    {
        for ( Lifecycle service : services )
        {
            service.start();
        }
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        for ( Lifecycle service : services )
        {
            service.stop();
        }
    }

    synchronized void register( Lifecycle lifecycle )
    {
        Objects.requireNonNull( lifecycle );
        services.add( lifecycle );
    }

    synchronized void unregister( Lifecycle lifecycle )
    {
        Objects.requireNonNull( lifecycle );
        services.remove( lifecycle );
    }
}
