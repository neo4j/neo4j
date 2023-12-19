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
package org.neo4j.causalclustering.routing.load_balancing;

import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.RoutingResult;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;

public interface LoadBalancingProcessor
{
    /**
     * Runs the procedure using the supplied client context
     * and returns the result.
     *
     * @param context The client supplied context.
     * @return The result of invoking the procedure.
     */
    Result run( Map<String,String> context ) throws ProcedureException;

    interface Result extends RoutingResult
    {
        /**
         * @return List of ROUTE-capable endpoints.
         */
        List<Endpoint> routeEndpoints();

        /**
         * @return List of WRITE-capable endpoints.
         */
        List<Endpoint> writeEndpoints();

        /**
         * @return List of READ-capable endpoints.
         */
        List<Endpoint> readEndpoints();
    }
}
