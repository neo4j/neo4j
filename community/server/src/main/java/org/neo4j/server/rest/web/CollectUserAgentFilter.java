/*
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
package org.neo4j.server.rest.web;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class CollectUserAgentFilter implements ContainerRequestFilter
{
    private static CollectUserAgentFilter INSTANCE;

    public static CollectUserAgentFilter instance()
    {
        if ( INSTANCE == null )
        {
            new CollectUserAgentFilter();
        }
        return INSTANCE;
    }

    private final Collection<String> userAgents = Collections.synchronizedCollection( new HashSet<String>() );

    public CollectUserAgentFilter()
    {
        // Bear with me here. There are some fairly unpleasant constraints that have led me to this solution.
        //
        // 1. The UDC can't depend on server components, because it has to work in embedded. So the read side of this
        //    in DefaultUdcInformationCollector is invoked by reflection. For that reason we need the actual list of
        //    user agents in a running system to be statically accessible.
        //
        // 2. On the write side, Jersey's contract is that we provide a class which it instantiates itself. So we need
        //    to write the list of user agents from any instance. However Jersey will only create one instance, so we
        //     can rely on the constructor being called only once in the running system.
        //
        // 3. For testing purposes, we would like to be able to create independent instances; otherwise we get problems
        //    with global state being carried over between tests.
        INSTANCE = this;
    }

    @Override
    public ContainerRequest filter( ContainerRequest request )
    {
        try
        {
            List<String> headers = request.getRequestHeader( "User-Agent" );
            if ( headers != null && !headers.isEmpty() )
            {
                userAgents.add( headers.get( 0 ).split( " " )[0] );
            }
        }
        catch ( RuntimeException e )
        {
            // We're fine with that
        }
        return request;
    }

    public void reset()
    {
        userAgents.clear();
    }

    public Collection<String> getUserAgents()
    {
        return Collections.unmodifiableCollection( userAgents );
    }
}
