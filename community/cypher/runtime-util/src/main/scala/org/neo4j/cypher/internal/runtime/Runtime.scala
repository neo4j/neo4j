/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.kernel.impl.query.{QueryExecution, ResultBuffer}
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.util.symbols.CypherType

// =============================================== /
// RUNTIME INTERFACES, implemented by each runtime /
// _______________________________________________ /

/**
  * A runtime knows how to compile logical plans into executable queries. Executable queries are intended to be reused
  * for executing the same multiple times, also concurrently. To facilitate this, all execution state is held in a
  * QueryExecutionState object. The runtime has the power to allocate and release these execution states,
  */
trait Runtime {

  def allocateExecutionState: QueryExecutionState
  def compileToExecutable(query: String, logicalPlan: LogicalPlan, context: PhysicalCompilationContext): ExecutableQuery
  def releaseExecutionState(executionState: QueryExecutionState): Unit
}

/**
  * An executable representation of a query.
  *
  * The ExecutableQuery holds no mutable state, and is safe to cache, reuse and use concurrently.
  */
trait ExecutableQuery {

  /**
    * Execute this query.
    *
    * @param params Parameters of the execution.
    * @param resultBuffer The result buffer to write results to.
    * @param transaction The transaction to execute the query in. If None, a new transaction will be begun
    *                    for the duration of this execution.
    * @return A QueryExecution representing the started execution.
    */
  def execute( params: MapValue,
               resultBuffer: ResultBuffer,
               transaction: Option[Transaction]
             ): QueryExecution
}

/**
  * A QueryExecutionState holds the mutable state needed during execution of a query.
  *
  * QueryExecutionStates of the correct type are allocated and released by the relevant runtime.
  */
trait QueryExecutionState

// ====================================================== /
// SUPPORTING INTERFACES, implemented by execution engine /
// ______________________________________________________ /

/**
  * Context for physical compilation.
  */
trait PhysicalCompilationContext {
  def typeFor(expression: Expression): CypherType
  def semanticTable: SemanticTable
  def readOnly: Boolean
  def tokenContext: TokenContext
  def periodicCommit: Option[Long]
}

