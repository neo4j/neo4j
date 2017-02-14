/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.load_balancing.strategy.server_policy;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.load_balancing.filters.Filter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class Policies
{
    static final String POLICY_KEY = "load_balancing.policy"; // TODO: move somewhere (driver support package?)
    private static final Filter<ServerInfo> DEFAULT_POLICY = input -> input;

    private final Map<String,Filter<ServerInfo>> policies = new HashMap<>();
    private final Log log;

    Policies( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
        policies.put( null, DEFAULT_POLICY );
    }

    void addPolicy( String policyName, Filter<ServerInfo> filter )
    {
        Filter<ServerInfo> oldPolicy = policies.putIfAbsent( policyName, filter );
        if ( oldPolicy != null )
        {
            log.error( "Policy name conflict for: " + policyName );
        }
    }

    Filter<ServerInfo> selectFor( Map<String,String> context )
    {
        return policies.get( context.get( POLICY_KEY ) );
    }
}
