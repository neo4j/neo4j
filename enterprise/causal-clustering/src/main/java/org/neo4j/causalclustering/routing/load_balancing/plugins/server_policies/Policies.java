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
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.routing.load_balancing.filters.IdentityFilter;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;

import static java.lang.String.format;

public class Policies
{
    public static final String POLICY_KEY = "policy";
    static final String DEFAULT_POLICY_NAME = "default";
    static final Policy DEFAULT_POLICY = new FilteringPolicy( IdentityFilter.as() ); // the default default

    private final Map<String,Policy> policies = new HashMap<>();

    private final Log log;

    Policies( Log log )
    {
        this.log = log;
    }

    void addPolicy( String policyName, Policy policy )
    {
        Policy oldPolicy = policies.putIfAbsent( policyName, policy );
        if ( oldPolicy != null )
        {
            log.error( format( "Policy name conflict for '%s'.", policyName ) );
        }
    }

    Policy selectFor( Map<String,String> context ) throws ProcedureException
    {
        String policyName = context.get( POLICY_KEY );

        if ( policyName == null )
        {
            return defaultPolicy();
        }
        else
        {
            Policy selectedPolicy = policies.get( policyName );
            if ( selectedPolicy == null )
            {
                throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                        format( "Policy definition for '%s' could not be found.", policyName ) );
            }
            return selectedPolicy;
        }
    }

    private Policy defaultPolicy()
    {
        Policy registeredDefault = policies.get( DEFAULT_POLICY_NAME );
        return registeredDefault != null ? registeredDefault : DEFAULT_POLICY;
    }
}
