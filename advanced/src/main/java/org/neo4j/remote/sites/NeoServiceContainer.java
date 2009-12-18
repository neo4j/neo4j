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
package org.neo4j.remote.sites;

import java.util.Collection;
import java.util.LinkedList;

import org.neo4j.api.core.NeoService;
import org.neo4j.util.index.IndexService;

class NeoServiceContainer implements Runnable
{
    final NeoService service;
    private final Collection<IndexService> indexServices = new LinkedList<IndexService>();

    NeoServiceContainer( NeoService neo )
    {
        this.service = neo;
    }

    void addIndexService( IndexService index )
    {
        this.indexServices.add( index );
    }

    /*
     * This container is usable as a shutdown hook for the NeoService it contains
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
