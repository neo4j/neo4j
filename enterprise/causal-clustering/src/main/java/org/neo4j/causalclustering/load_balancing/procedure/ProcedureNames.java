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
package org.neo4j.causalclustering.load_balancing.procedure;

import java.util.Arrays;

import static java.lang.String.join;

/**
 * This is part of the cluster / driver interface specification and
 * defines the procedure names involved in the load balancing solution.
 *
 * These procedures are used by cluster driver software to discover endpoints,
 * their capabilities and to eventually schedule work appropriately.
 *
 * The intention is for this class to eventually move over to a support package
 * which can be included by driver software.
 */
public enum ProcedureNames
{
    GET_SERVERS_V1( "getServers" ),
    GET_SERVERS_V2( "getRoutingTable" );

    private static final String[] nameSpace = new String[]{"dbms", "cluster", "routing"};
    private final String name;

    ProcedureNames( String name )
    {
        this.name = name;
    }

    public String procedureName()
    {
        return name;
    }

    public String[] procedureNameSpace()
    {
        return nameSpace;
    }

    public String[] fullyQualifiedProcedureName()
    {
        String[] fullyQualifiedProcedureName = Arrays.copyOf( nameSpace, nameSpace.length + 1 );
        fullyQualifiedProcedureName[nameSpace.length] = name;
        return fullyQualifiedProcedureName;
    }

    public String callName()
    {
        return join( ".", nameSpace ) + "." + name;
    }
}
