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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v2_3.ast._

// This is when dynamic properties are used
object AsDynamicPropertyNonSeekable {
  def unapply(v: Any) = v match {
    case WithSeekableArgs(prop@ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)
    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsDynamicPropertyNonScannable {
  def unapply(v: Any) = v match {

    case func@FunctionInvocation(_, _, IndexedSeq(ContainerIndex(identifier: Identifier, _)))
      if  func.function.contains(functions.Exists) =>
      Some(identifier)

    case Equals(ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)

    case expr: InequalityExpression =>
      expr.lhs match {
        case ContainerIndex(identifier: Identifier, _) => Some(identifier)
        case _ => None
      }

    case StartsWith(ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)

    case RegexMatch(ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)

    case NotEquals(ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)

    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsStringRangeNonSeekable {
  def unapply(v: Any) = v match {
    case startsWith@StartsWith(prop@ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)
    case _ =>
      None
  }
}

// This is when dynamic properties are used
object AsValueRangeNonSeekable {
  def unapply(v: Any) = v match {
    case GreaterThan(prop@ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)
    case GreaterThan(_, prop@ContainerIndex(identifier: Identifier, _)) =>
      Some(identifier)

    case GreaterThanOrEqual(prop@ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)
    case GreaterThanOrEqual(_, prop@ContainerIndex(identifier: Identifier, _)) =>
      Some(identifier)

    case LessThan(prop@ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)
    case LessThan(_, prop@ContainerIndex(identifier: Identifier, _)) =>
      Some(identifier)

    case LessThanOrEqual(prop@ContainerIndex(identifier: Identifier, _), _) =>
      Some(identifier)
    case LessThanOrEqual(_, prop@ContainerIndex(identifier: Identifier, _)) =>
      Some(identifier)

    case _ =>
      None
  }
}
