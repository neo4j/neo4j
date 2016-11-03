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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v3_2.ast._

// This is when dynamic properties are used
object AsDynamicPropertyNonSeekable {
  def unapply(v: Any) = v match {
    case WithSeekableArgs(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsDynamicPropertyNonScannable {
  def unapply(v: Any) = v match {

    case func@FunctionInvocation(_, _, _, IndexedSeq(ContainerIndex(variable: Variable, _)))
      if  func.function == functions.Exists =>
      Some(variable)

    case Equals(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case expr: InequalityExpression =>
      expr.lhs match {
        case ContainerIndex(variable: Variable, _) => Some(variable)
        case _ => None
      }

    case StartsWith(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case EndsWith(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case Contains(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case RegexMatch(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case NotEquals(ContainerIndex(variable: Variable, _), _) =>
      Some(variable)

    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsStringRangeNonSeekable {
  def unapply(v: Any) = v match {
    case startsWith@StartsWith(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case contains@Contains(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case contains@EndsWith(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsValueRangeNonSeekable {
  def unapply(v: Any) = v match {
    case GreaterThan(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case GreaterThan(_, prop@ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case GreaterThanOrEqual(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case GreaterThanOrEqual(_, prop@ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case LessThan(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case LessThan(_, prop@ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case LessThanOrEqual(prop@ContainerIndex(variable: Variable, _), _) =>
      Some(variable)
    case LessThanOrEqual(_, prop@ContainerIndex(variable: Variable, _)) =>
      Some(variable)

    case _ =>
      None
  }
}
