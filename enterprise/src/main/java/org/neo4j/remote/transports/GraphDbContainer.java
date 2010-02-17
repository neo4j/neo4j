/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;

class GraphDbContainer implements Runnable
{
    final GraphDatabaseService service;
    private final Collection<IndexService> indexServices = new LinkedList<IndexService>();

    GraphDbContainer( GraphDatabaseService graphDb )
    {
        this.service = graphDb;
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
        for ( IndexService index : indexServices )
        {
            index.shutdown();
        }
        this.service.shutdown();
    }
}
