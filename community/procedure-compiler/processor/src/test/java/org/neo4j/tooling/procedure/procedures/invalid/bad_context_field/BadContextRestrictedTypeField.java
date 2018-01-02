/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tooling.procedure.procedures.invalid.bad_context_field;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;

public class BadContextRestrictedTypeField
{

    @Context
    public GraphDatabaseAPI notOfficiallySupported;

    @Procedure
    public void sproc()
    {
    }

    @UserFunction
    public Long function()
    {
        return 42L;
    }

    @UserAggregationFunction
    public MyAggregation aggregation()
    {
        return new MyAggregation();
    }

    public static class MyAggregation
    {
        @UserAggregationResult
        public Long result()
        {
            return 42L;
        }

        @UserAggregationUpdate
        public void woot( @Name( "undostres" ) String onetwothree )
        {

        }
    }
}
