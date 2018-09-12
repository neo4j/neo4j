/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.expressions.Property
import org.opencypher.v9_0.expressions.PropertyKeyName
import org.opencypher.v9_0.expressions.Variable
import org.opencypher.v9_0.util.InputPosition

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
      case (Property(Variable(varName), PropertyKeyName(propName)), columnName) =>
        projectExpressions.collect {
          case (newName, Variable(`varName`)) =>
            (Property(Variable(newName)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE), columnName)
        }

      case _ => Map.empty[Property, CachedNodeProperty]
    }
  }
}
