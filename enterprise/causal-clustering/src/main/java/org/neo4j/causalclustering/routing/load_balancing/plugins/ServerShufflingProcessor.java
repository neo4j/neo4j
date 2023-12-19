/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
