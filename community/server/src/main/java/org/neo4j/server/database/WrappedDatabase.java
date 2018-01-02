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
package org.neo4j.server.database;

import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class WrappedDatabase extends LifecycleAdapter implements Database
{
    private final GraphDatabaseAPI graph;

    public static Database.Factory wrappedDatabase( final GraphDatabaseAPI db )
    {
        return new Factory()
        {
            @Override
            public Database newDatabase( Config config, CommunityFacadeFactory.Dependencies dependencies)
            {
                return new WrappedDatabase( db );
            }
        };
    }

    public WrappedDatabase( GraphDatabaseAPI graph )
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
    public String getLocation()
    {
        return graph.getStoreDir();
    }

    @Override
    public GraphDatabaseAPI getGraph()
    {
        return graph;
    }

    @Override
    public boolean isRunning()
    {
        return true;
    }
}
