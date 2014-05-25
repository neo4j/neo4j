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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Property
import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.ast.Property

case object astExpressionDocBuilder extends CachingDocBuilder[Any] {

  import Doc._

  override protected def newNestedDocGenerator = {
    case Identifier(name) => (inner) =>
      text(name)

    case Property(map, PropertyKeyName(name)) => (inner) =>
      inner(map) :: "." :: name

    case LabelName(name) => (inner) =>
      ":" :: name

    case HasLabels(expr, labels) => (inner) =>
      inner(expr) :: breakList(labels.map(inner), break = breakSilent)

    case Not(expr) => (inner) =>
      "NOT" :/: inner(expr)

    case Xor(left, right) => (inner) =>
      inner(left) :/: "XOR" :/: inner(right)

    case Or(left, right) => (inner) =>
      inner(left) :/: "OR" :/: inner(right)

    case And(left, right) => (inner) =>
      inner(left) :/: "AND" :/: inner(right)

    case Equals(left, right) => (inner) =>
      inner(left) :/: "=" :/: inner(right)

    case NotEquals(left, right) => (inner) =>
      inner(left) :/: "<>" :/: inner(right)

    case LessThan(left, right) => (inner) =>
      inner(left) :/: "<" :/: inner(right)

    case LessThanOrEqual(left, right) => (inner) =>
      inner(left) :/: "<=" :/: inner(right)

    case GreaterThan(left, right) => (inner) =>
      inner(left) :/: ">" :/: inner(right)

    case GreaterThanOrEqual(left, right) => (inner) =>
      inner(left) :/: ">=" :/: inner(right)
  }
}
