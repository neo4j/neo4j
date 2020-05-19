/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.fabric.executor;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.planning.QueryType;
import org.neo4j.graphdb.QueryExecutionType;

public class EffectiveQueryType
{
    public static QueryExecutionType.QueryType effectiveQueryType( AccessMode requested, QueryType queryType )
    {
        if ( queryType == QueryType.READ() )
        {
            return QueryExecutionType.QueryType.READ_ONLY;
        }
        if ( queryType == QueryType.READ_PLUS_UNRESOLVED() )
        {
            switch ( requested )
            {
            case READ:
                return QueryExecutionType.QueryType.READ_ONLY;
            case WRITE:
                return QueryExecutionType.QueryType.READ_WRITE;
            default:
                throw new IllegalArgumentException( "Unexpected access mode: " + requested );
            }
        }
        if ( queryType == QueryType.WRITE() )
        {
            return QueryExecutionType.QueryType.READ_WRITE;
        }

        throw new IllegalArgumentException( "Unexpected query type: " + queryType );
    }

    public static AccessMode effectiveAccessMode( AccessMode requested, QueryType queryType )
    {
        if ( queryType == QueryType.READ() )
        {
            return AccessMode.READ;
        }
        if ( queryType == QueryType.READ_PLUS_UNRESOLVED() )
        {
            return requested;
        }
        if ( queryType == QueryType.WRITE() )
        {
            return AccessMode.WRITE;
        }

        throw new IllegalArgumentException( "Unexpected query type: " + queryType );
    }

    public static QueryExecutionType queryExecutionType( FabricPlan plan, AccessMode accessMode )
    {
        QueryExecutionType.QueryType effectiveQueryType = effectiveQueryType( accessMode, plan.queryType() );
        if ( plan.executionType() == FabricPlan.EXECUTE() )
        {
            return QueryExecutionType.query( effectiveQueryType );
        }
        else if ( plan.executionType() == FabricPlan.EXPLAIN() )
        {
            return QueryExecutionType.explained( effectiveQueryType );
        }
        else if ( plan.executionType() == FabricPlan.PROFILE() )
        {
            return QueryExecutionType.profiled( effectiveQueryType );
        }
        else
        {
            throw new IllegalArgumentException( "Unexpected execution type: " + plan.executionType() );
        }
    }
}
