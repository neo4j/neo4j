/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.eval

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.evaluator.StaticEvaluation
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.fabric.util.Errors
import org.neo4j.fabric.util.Rewritten.RewritingOps
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.MapHasAsScala

class UseEvaluation(
) {

  def instance(
    evaluator: StaticEvaluation.StaticEvaluator,
    signatureResolver: ProcedureSignatureResolver,
    query: String,
    catalog: Catalog
  ) =
    new UseEvaluation.Instance(query, catalog, evaluator, signatureResolver)

}

object UseEvaluation {

  class Instance(
    query: String,
    catalog: Catalog,
    val evaluator: StaticEvaluation.StaticEvaluator,
    signatureResolver: ProcedureSignatureResolver
  ) {

    def evaluate(
      graphSelection: GraphSelection,
      parameters: MapValue,
      context: java.util.Map[String, AnyValue],
      sessionDb: DatabaseReference
    ): Catalog.Graph = Errors.errorContext(query, graphSelection) {

      graphSelection.expression match {
        case v: Variable =>
          catalog.resolveGraph(nameFromVar(v))

        case p: Property =>
          catalog.resolveGraph(nameFromProp(p))

        case f: FunctionInvocation =>
          val ctx = CypherRow(context.asScala)
          val argValues = f.args
            .map(resolveFunctions)
            .map(expr => evaluator.evaluate(expr, parameters, ctx))
          catalog.resolveView(nameFromFunc(f), argValues, sessionDb: DatabaseReference)

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

    def resolveGraph(compositeName: NormalizedDatabaseName): Catalog.Graph =
      catalog.resolveGraph(CatalogName(compositeName.name()))

    def isConstituentOrSelf(graph: Catalog.Graph, composite: Catalog.Graph): Boolean =
      (graph, composite) match {
        case (c1: Catalog.Composite, c2: Catalog.Composite) =>
          c1 == c2

        case (a: Catalog.Alias, c: Catalog.Composite) =>
          a.namespace.contains(c.name)

        case _ =>
          false
      }

    def isSystem(graph: Catalog.Graph): Boolean =
      qualifiedNameString(graph) == GraphDatabaseSettings.SYSTEM_DATABASE_NAME

    def isDatabaseOrAliasInRoot(graph: Catalog.Graph): Boolean = graph match {
      case _: Catalog.Composite => false
      case alias: Catalog.Alias => alias.namespace.isEmpty
    }

    def qualifiedNameString(graph: Catalog.Graph): String =
      Catalog.catalogName(graph).qualifiedNameString
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
