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
package org.neo4j.cypher.internal.compiler.v2_3.planDescription

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.compiler.v2_3.helpers.UnNamedNameGenerator._


object PlanDescriptionArgumentSerializer {
  private val SEPARATOR = ", "
  private val UNNAMED_PATTERN = """  (UNNAMED|FRESHID|AGGREGATION)(\d+)""".r
  private val DEDUP_PATTERN =   """  (.+)@\d+""".r
  def serialize(arg: Argument): AnyRef = {

    arg match {
      case ColumnsLeft(columns) => s"keep columns ${columns.mkString(SEPARATOR)}"
      case LegacyExpression(expr) => removeGeneratedNames(expr.toString)
      case Expression(expr) => removeGeneratedNames(expr.toString)
      case UpdateActionName(action) => action
      case MergePattern(startPoint) => s"MergePattern($startPoint)"
      case LegacyIndex(index) => index
      case Index(label, property) => s":$label($property)"
      case PrefixIndex(label, property, prefix) => s":$label($property STARTS WITH $prefix)"
      case InequalityIndex(label, property, bounds) => s":$label($property) ${bounds.mkString(", ")}"
      case LabelName(label) => s":$label"
      case KeyNames(keys) => keys.map(removeGeneratedNames).mkString(SEPARATOR)
      case KeyExpressions(expressions) => expressions.mkString(SEPARATOR)
      case DbHits(value) => Long.box(value)
      case _: EntityByIdRhs => arg.toString
      case Rows(value) => Long.box(value)
      case Time(value) => Long.box(value)
      case EstimatedRows(value) => Double.box(value)
      case Version(version) => version
      case Planner(planner) => planner
      case PlannerImpl(plannerName) => plannerName
      case Runtime(runtime) => runtime
      case SourceCode(className, sourceCode) => sourceCode
      case RuntimeImpl(runtimeName) => runtimeName
      case ExpandExpression(from, rel, typeNames, to, dir: SemanticDirection, varLength) =>
        val left = if (dir == SemanticDirection.INCOMING) "<-" else "-"
        val right = if (dir == SemanticDirection.OUTGOING) "->" else "-"
        val asterisk = if (varLength) "*" else ""
        val types = typeNames.mkString(":", "|:", "")
        val relInfo = if (!varLength && typeNames.isEmpty && rel.unnamed) "" else s"[$rel$types$asterisk]"
        s"($from)$left$relInfo$right($to)"

      // Do not add a fallthrough here - we rely on exhaustive checking to ensure
      // that we don't forget to add new types of arguments here
    }
  }

   def removeGeneratedNames(s: String) = {
    val named = UNNAMED_PATTERN.replaceAllIn(s, m => s"anon[${m group 2}]")
    DEDUP_PATTERN.replaceAllIn(named, _.group(1))
  }
}
