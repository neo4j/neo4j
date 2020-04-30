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
package org.neo4j.fabric.eval

import java.util.function.Supplier

import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.fabric.util.Errors
import org.neo4j.fabric.util.Rewritten.RewritingOps
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters.mapAsScalaMapConverter

class UseEvaluation(
  catalogManager: CatalogManager,
  proceduresSupplier: Supplier[GlobalProcedures],
  signatureResolver: ProcedureSignatureResolver,
) {

  private val evaluator = new StaticEvaluation.StaticEvaluator(proceduresSupplier)

  def instance(query: String): UseEvaluation.Instance =
    new UseEvaluation.Instance(query, catalogManager.currentCatalog(), evaluator, signatureResolver)

}

object UseEvaluation {

  class Instance(
    query: String,
    catalog: Catalog,
    evaluator: StaticEvaluation.StaticEvaluator,
    signatureResolver: ProcedureSignatureResolver,
  ) {

    def evaluate(
      graphSelection: GraphSelection,
      parameters: MapValue,
      context: java.util.Map[String, AnyValue]
    ): Catalog.Graph = Errors.errorContext(query, graphSelection) {

      graphSelection.graphReference
      graphSelection.expression match {
        case v: Variable =>
          catalog.resolve(nameFromVar(v))

        case p: Property =>
          catalog.resolve(nameFromProp(p))

        case f: FunctionInvocation =>
          val ctx = CypherRow(context.asScala)
          val argValues = f.args
            .map(resolveFunctions)
            .map(expr => evaluator.evaluate(expr, parameters, ctx))
          catalog.resolve(nameFromFunc(f), argValues)

        case x =>
          Errors.openCypherUnexpected("graph or view reference", x)
      }
    }

    private def resolveFunctions(expr: Expression): Expression = expr.rewritten.bottomUp {
      case f: FunctionInvocation if f.needsToBeResolved => {
        val resolved = ResolvedFunctionInvocation(signatureResolver.functionSignature)(f).coerceArguments

        if (resolved.fcnSignature.isEmpty) {
          Errors.openCypherFailure(Errors.openCypherSemantic(s"Unknown function '${resolved.qualifiedName}'", resolved))
        }

        return resolved
      }
    }
  }

  def isStatic(graphSelection: GraphSelection): Boolean =
    graphSelection.expression match {
      case _: Variable | _: Property => true
      case _                         => false
    }

  def evaluateStatic(graphSelection: GraphSelection): Option[CatalogName] =
    graphSelection.expression match {
      case v: Variable => Some(nameFromVar(v))
      case p: Property => Some(nameFromProp(p))
      case _           => None
    }

  private def nameFromVar(variable: Variable): CatalogName =
    CatalogName(variable.name)

  private def nameFromProp(property: Property): CatalogName = {
    def parts(expr: Expression): List[String] = expr match {
      case p: Property    => parts(p.map) :+ p.propertyKey.name
      case Variable(name) => List(name)
      case x              => Errors.openCypherUnexpected("Graph name segment", x)
    }

    CatalogName(parts(property))
  }

  private def nameFromFunc(func: FunctionInvocation): CatalogName = {
    CatalogName(func.namespace.parts :+ func.functionName.name)
  }

}
