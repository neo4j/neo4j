/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.database;

import java.io.File;

import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class WrappedDatabase extends LifecycleAdapter implements Database
{
    private final GraphDatabaseFacade graph;

    public WrappedDatabase( GraphDatabaseFacade graph )
    {
        this.graph = graph;
        try
        {
            start();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }
    }

    @Override
    public File getLocation()
    {
        return graph.getStoreDir();
    }

    @Override
    public GraphDatabaseFacade getGraph()
    {
        return graph;
    }

    @Override
    public boolean isRunning()
    {
        return true;
    }
}
