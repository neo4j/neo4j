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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.neo4j.cypher.internal.v3_5.expressions.{Expression, Property, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.v3_5.util.InputPosition

trait ProjectingPlan extends LogicalPlan {
  val source: LogicalPlan
  // The projected expressions
  val projectExpressions: Map[String, Expression]

  override val lhs: Option[LogicalPlan] = Some(source)
  override val rhs: Option[LogicalPlan] = None
  /**
    * Given
    * - projection var("n") -> "m"
    * - properties Map(prop("n", "prop") -> "n.prop")
    * -> Map(prop("m", "prop") -> "n.prop")
    */
  override final def availableCachedNodeProperties: Map[Property, CachedNodeProperty] = {
    source.availableCachedNodeProperties.flatMap {
      case (Property(variable@Variable(varName), PropertyKeyName(propName)), columnName) if projectsValue(variable) =>
        projectExpressions.collect {
          case (newName, Variable(`varName`)) =>
            (Property(Variable(newName)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(
              InputPosition.NONE), columnName)
        }

      case (property, columnName) if projectsValue(property) || projectsValue(columnName) => Map.empty[Property, CachedNodeProperty]

      //we should pass along cached node properties that we are not projecting
      case (key, value) => Map(key -> value)
    }
  }

  private def projectsValue(expression: Expression) = projectExpressions.values.collectFirst {
    case e if e == expression => e
  }.nonEmpty
}
