/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.routing.load_balancing.plugins;

import java.util.Collections;
import java.util.Map;

import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;

/**
 * Shuffles the servers of the delegate around so that every client
 * invocation gets a a little bit of that extra entropy spice.
 *
 * N.B: Lists are shuffled in place.
 */
public class ServerShufflingProcessor implements LoadBalancingProcessor
{
    private final LoadBalancingProcessor delegate;

    public ServerShufflingProcessor( LoadBalancingProcessor delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public Result run( Map<String,String> context ) throws ProcedureException
    {
        Result result = delegate.run( context );

        Collections.shuffle( result.routeEndpoints() );
        Collections.shuffle( result.writeEndpoints() );
        Collections.shuffle( result.readEndpoints() );

        return result;
    }

    public LoadBalancingProcessor delegate()
    {
        return delegate;
    }
}
