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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.Variable

// This is when dynamic properties are used
object AsDynamicPropertyNonSeekable {

  def unapply(v: Any): Option[Variable] = v match {
    case WithSeekableArgs(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsDynamicPropertyNonScannable {

  def unapply(v: Any): Option[Variable] = v match {

    case IsNotNull(ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case Equals(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case expr: InequalityExpression =>
      expr.lhs match {
        case ContainerIndex(variable: Variable, _) => Some(variable)
        case _                                     => None
      }

    case StartsWith(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case EndsWith(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case Contains(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case RegexMatch(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsStringRangeNonSeekable {

  def unapply(v: Any): Option[Variable] = v match {
    case StartsWith(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case Contains(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case EndsWith(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsValueRangeNonSeekable {

  def unapply(v: Any): Option[Variable] = v match {
    case GreaterThan(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case GreaterThan(_, prop @ ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case GreaterThanOrEqual(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case GreaterThanOrEqual(_, prop @ ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case LessThan(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case LessThan(_, prop @ ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case LessThanOrEqual(prop @ ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case LessThanOrEqual(_, prop @ ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case _ =>
      None
  }
}
