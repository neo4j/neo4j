/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutablePlanBuilder, ExecutionPlan, PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{FieldSignature, PlanContext, ProcedureName}
import org.neo4j.cypher.internal.compiler.v3_0.{CompilationPhaseTracer, PreparedQuery}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{CallProcedure, Expression, ProcName, Query, SingleQuery}
import org.neo4j.cypher.internal.frontend.v3_0.{CypherTypeException, InvalidArgumentException, SemanticTable}

/**
  * This planner takes on queries that requires no planning
  * @param delegate The plan builder to delegate to
  */
case class DelegatingProcedureExecutablePlanBuilder(delegate: ExecutablePlanBuilder) extends ExecutablePlanBuilder {

  override def producePlan(inputQuery: PreparedQuery, planContext: PlanContext, tracer: CompilationPhaseTracer,

                           createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference): ExecutionPlan = {

    inputQuery.statement match {
      case Query(None, SingleQuery(Seq(CallProcedure(namespace, ProcName(name), args)))) =>
        val signature = planContext.procedureSignature(ProcedureName(namespace, name))

        if (args.nonEmpty && args.size != signature.inputSignature.size)
          throw new InvalidArgumentException(
            s"""Procedure ${signature.name.name} takes ${signature.inputSignature.size}
                  |arguments but ${args.size} was provided.""".stripMargin)
        args.zip(signature.inputSignature).foreach {
          case (arg, field) => typeCheck(inputQuery.semanticTable)(arg, field)
        }

        CallProcedureExecutionPlan(signature, args)

      case _ => delegate.producePlan(inputQuery, planContext, tracer, createFingerprintReference)
    }
  }

  private def typeCheck(semanticTable: SemanticTable)(exp: Expression, field: FieldSignature) = {
    if (!(semanticTable.types(exp).actual containsAny field.typ.covariant))
      throw new CypherTypeException(
        s"""${field.name} expects ${field.typ}, but got ${semanticTable.types(exp).actual.mkString(",", "or")}""")
  }
}

