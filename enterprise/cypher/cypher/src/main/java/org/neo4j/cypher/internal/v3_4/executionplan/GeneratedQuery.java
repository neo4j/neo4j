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
package org.neo4j.cypher.internal.v3_4.executionplan;

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.Provider;
import org.neo4j.cypher.internal.runtime.ExecutionMode;
import org.neo4j.cypher.internal.runtime.QueryContext;
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription;
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer;
import org.neo4j.values.virtual.MapValue;

public interface GeneratedQuery
{
    org.neo4j.cypher.internal.v3_4.executionplan.GeneratedQueryExecution execute(
            QueryContext queryContext,
            ExecutionMode executionMode,
            Provider<InternalPlanDescription> description,
            QueryExecutionTracer tracer,
            MapValue params );
}
