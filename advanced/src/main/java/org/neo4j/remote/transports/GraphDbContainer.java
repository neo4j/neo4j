/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.remote.transports;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.index.IndexService;

class GraphDbContainer implements Runnable, KernelEventHandler
{
    final GraphDatabaseService service;
    private final Collection<IndexService> indexServices = new LinkedList<IndexService>();
    private final String key;
    private final ConcurrentMap<String, GraphDbContainer> instances;

    GraphDbContainer( GraphDatabaseService graphDb )
    {
        this.service = graphDb;
        this.key = null;
        this.instances = null;
    }

    GraphDbContainer( GraphDatabaseService graphDb, String path,
            ConcurrentMap<String, GraphDbContainer> instances )
    {
        this.service = graphDb;
        this.key = path;
        this.instances = instances;
    }

    void addIndexService( IndexService index )
    {
        this.indexServices.add( index );
    }

    /*
     * This container is usable as a shutdown hook
     */
    public void run()
    {
        beforeShutdown();
    }

    public void beforeShutdown()
    {
        if ( instances != null ) instances.remove( key, this );
        for ( IndexService index : indexServices )
        {
            index.shutdown();
        }
        this.service.shutdown();
    }

    public Object getResource()
    {
        return service;
    }

    public void kernelPanic( ErrorState error )
    {
    }

    public ExecutionOrder orderComparedTo( KernelEventHandler other )
    {
        if ( other instanceof GraphDbContainer )
        {
            return ExecutionOrder.DOESNT_MATTER;
        }
        else
        {
            return ExecutionOrder.AFTER;
        }
    }
}
