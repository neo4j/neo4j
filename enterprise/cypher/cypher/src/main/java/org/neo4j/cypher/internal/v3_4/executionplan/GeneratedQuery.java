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
package org.neo4j.cypher.internal.v3_4.executionplan;

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.Provider;
import org.neo4j.cypher.internal.runtime.ExecutionMode;
import org.neo4j.cypher.internal.runtime.QueryContext;
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription;
import org.neo4j.cypher.internal.util.v3_4.TaskCloser;
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer;
import org.neo4j.values.virtual.MapValue;

public interface GeneratedQuery
{
    org.neo4j.cypher.internal.v3_4.executionplan.GeneratedQueryExecution execute(
            TaskCloser closer,
            QueryContext queryContext,
            ExecutionMode executionMode,
            Provider<InternalPlanDescription> description,
            QueryExecutionTracer tracer,
            MapValue params );
}
