/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionMode;
import org.neo4j.cypher.internal.compiler.v2_3.TaskCloser;
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.CompiledExecutionResult;
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.GeneratedQueryExecution;
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult;
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.Statement;

//TODO this should be replaced, here for testing stuff out
public class GeneratedCodeLoader
{
    // TODO: Depending on gdb and supplier is not exactly like going through the SPI
    // TODO: Perhaps using a builder or at lesat a data carrier for all the arguments might be nice to have here
    public static InternalExecutionResult newInstance( Class<GeneratedQueryExecution> clazz, TaskCloser closer, Statement statement,
                                                       GraphDatabaseService db, ExecutionMode executionMode, Supplier<InternalPlanDescription> description,
                                                       QueryExecutionTracer tracer, Map<String, Object> params)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        Constructor<GeneratedQueryExecution> constructor =
                clazz.getDeclaredConstructor( TaskCloser.class, Statement.class, GraphDatabaseService.class, ExecutionMode.class, Supplier.class, QueryExecutionTracer.class, Map.class );
        GeneratedQueryExecution compiledCode = constructor.newInstance( closer, statement, db, executionMode, description, tracer, params );
        return new CompiledExecutionResult( closer, statement, compiledCode, description );
    }
}
