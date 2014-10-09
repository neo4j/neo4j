/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planDescription

import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription.Arguments._

object PlanDescriptionArgumentSerializer {
  def serialize(arg: Argument): String = {
    val SEPARATOR = ", "
    arg match {
      case ColumnsLeft(columns) => s"keep columns ${columns.mkString(SEPARATOR)}"
      case LegacyExpression(expr) => expr.toString
      case UpdateActionName(action) => action
      case LegacyIndex(index) => index
      case Index(label, property) => s":$label($property)"
      case LabelName(label) => s":$label"
      case KeyNames(keys) => keys.mkString(SEPARATOR)
      case KeyExpressions(expressions) => expressions.mkString(SEPARATOR)
      case DbHits(value) => value.toString
      case _: EntityByIdRhs => arg.toString
      case IntroducedIdentifier(n) => n
      case Rows(value) => value.toString
      case EstimatedRows(value) => value.toString

      // Do not add a fallthrough here - we rely on exhaustive checking to ensure
      // that we don't forget to add new types of arguments here
    }
  }
}
