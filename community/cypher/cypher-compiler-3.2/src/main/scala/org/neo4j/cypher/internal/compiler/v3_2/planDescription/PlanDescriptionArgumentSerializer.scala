/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planDescription

import org.neo4j.cypher.internal.frontend.v3_2.helpers.UnNamedNameGenerator._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection


object PlanDescriptionArgumentSerializer {
  private val SEPARATOR = ", "
  private val UNNAMED_PATTERN = """  (UNNAMED|FRESHID|AGGREGATION)(\d+)""".r
  private val DEDUP_PATTERN =   """  (.+)@\d+""".r
  def serialize(arg: Argument): AnyRef = {

    arg match {
      case ColumnsLeft(columns) => s"keep columns ${columns.mkString(SEPARATOR)}"
      case LegacyExpression(expr) => removeGeneratedNames(expr.toString)
      case LegacyExpressions(expressions) => expressions.map({ case (k, v) => s"$k : $v" }).mkString("{", ", ", "}")
      case Expression(expr) => removeGeneratedNames(expr.toString)
      case Expressions(expressions) => expressions.map({ case (k, v) => s"$k : $v" }).mkString("{", ", ", "}")
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
      case ByteCode(className, byteCode) => byteCode
      case RuntimeImpl(runtimeName) => runtimeName
      case ExpandExpression(from, rel, typeNames, to, dir: SemanticDirection, min, max) =>
        val left = if (dir == SemanticDirection.INCOMING) "<-" else "-"
        val right = if (dir == SemanticDirection.OUTGOING) "->" else "-"
        val types = typeNames.mkString(":", "|:", "")
        val lengthDescr = (min, max) match {
          case (1, Some(1)) => ""
          case (1, None) => "*"
          case (1, Some(m)) => s"*..$m"
          case _ => s"*$min..${max.getOrElse("")}"
        }
        val relInfo = if (lengthDescr == "" && typeNames.isEmpty && rel.unnamed) "" else s"[$rel$types$lengthDescr]"
        s"($from)$left$relInfo$right($to)"
      case CountNodesExpression(ident, label) =>
        val node = label.map(":" + _).mkString
        s"count( ($node) )" + (if (ident.startsWith(" ")) "" else s" AS $ident")
      case CountRelationshipsExpression(ident, startLabel, typeNames, endLabel) =>
        val start = startLabel.map(l => ":" + l).mkString
        val end = endLabel.map(l => ":" + l).mkString
        val types = typeNames.mkString(":", "|:", "")
        s"count( ($start)-[$types]->($end) )" + (if (ident.unnamed) "" else s" AS $ident")
      case Signature(procedureName, args, results) =>
        val argString = args.mkString(", ")
        val resultString = results.map { case (name, typ) => s"$name :: $typ" }.mkString(", ")
        s"$procedureName($argString) :: ($resultString)"

      // Do not add a fallthrough here - we rely on exhaustive checking to ensure
      // that we don't forget to add new types of arguments here
    }
  }

   def removeGeneratedNames(s: String) = {
    val named = UNNAMED_PATTERN.replaceAllIn(s, m => s"anon[${m group 2}]")
    DEDUP_PATTERN.replaceAllIn(named, _.group(1))
  }
}
