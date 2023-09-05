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

import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.fabric.util.Errors

import scala.annotation.tailrec

class StaticUseEvaluation {

  def isStatic(graphSelection: GraphSelection): Boolean =
    graphSelection.expression match {
      case _: Variable | _: Property => true
      case _                         => false
    }

  def evaluateStaticOption(graphSelection: GraphSelection): Option[CatalogName] =
    graphSelection.expression match {
      case v: Variable => Some(nameFromVar(v))
      case p: Property => Some(nameFromProp(p))
      case _           => None
    }

  def evaluateStaticLeadingGraphSelection(statement: Statement): Option[CatalogName] =
    leadingGraphSelection(statement).map(evaluateStatic)

  private def evaluateStatic(graphSelection: GraphSelection): CatalogName =
    evaluateStaticOption(graphSelection).getOrElse(Errors.dynamicGraphNotAllowed(graphSelection))

  @tailrec
  private def leftmostSingleQuery(statement: Statement): Option[SingleQuery] =
    statement match {
      case sq: SingleQuery => Some(sq)
      case union: Union    => leftmostSingleQuery(union.lhs)
      case _               => None
    }

  private def leadingGraphSelection(statement: Statement): Option[GraphSelection] = {
    statement match {
      case sc: SchemaCommand => sc.useGraph
      case _                 => queryLeadingGraphSelection(statement)
    }
  }

  private def queryLeadingGraphSelection(statement: Statement): Option[GraphSelection] = {
    val singleQuery = leftmostSingleQuery(statement)
    val clause = singleQuery.flatMap(_.clauses.headOption)
    clause.collect {
      case gs: GraphSelection => gs
    }
  }

  protected def nameFromVar(variable: Variable): CatalogName =
    CatalogName(variable.name)

  protected def nameFromProp(property: Property): CatalogName = {
    def parts(expr: Expression): List[String] = expr match {
      case p: Property    => parts(p.map) :+ p.propertyKey.name
      case Variable(name) => List(name)
      case x              => Errors.openCypherUnexpected("Graph name segment", x)
    }

    CatalogName(parts(property))
  }
}
