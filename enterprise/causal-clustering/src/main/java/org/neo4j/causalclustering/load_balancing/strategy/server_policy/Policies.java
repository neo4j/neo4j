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

import org.neo4j.causalclustering.load_balancing.filters.IdentityFilter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

class Policies
{
    static final String POLICY_KEY = "load_balancing.policy"; // TODO: move somewhere (driver support package?)

    private final Map<String,Policy> policies = new HashMap<>();
    private final Policy DEFAULT_POLICY = new FilteringPolicy( IdentityFilter.as() );
    private final Log log;

    Policies( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    void addPolicy( String policyName, Policy policy )
    {
        Policy oldPolicy = policies.putIfAbsent( policyName, policy );
        if ( oldPolicy != null )
        {
            log.error( format( "Policy name conflict for '%s'.", policyName ) );
        }
    }

    Policy selectFor( Map<String,String> context )
    {
        String policyName = context.get( POLICY_KEY );
        Policy selectedPolicy = policies.get( policyName );

        if ( policyName == null )
        {
            return DEFAULT_POLICY;
        }
        else if ( selectedPolicy == null )
        {
            log.warn( format( "Policy '%s' could not be found. Will use default instead.", policyName ) );
            return DEFAULT_POLICY;
        }
        else
        {
            return selectedPolicy;
        }
    }
}
