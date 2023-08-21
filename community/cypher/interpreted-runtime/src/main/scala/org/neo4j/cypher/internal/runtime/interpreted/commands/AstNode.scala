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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.exceptions.CypherTypeException

import scala.reflect.ClassTag

trait AstNode[T] {
  def children: collection.Seq[AstNode[_]]

  def rewrite(f: Expression => Expression): T

  def typedRewrite[R <: T](f: Expression => Expression)(implicit mf: ClassTag[R]): R = rewrite(f) match {
    case value: R => value
    case _        => throw new CypherTypeException("Invalid rewrite")
  }

  def exists(f: Expression => Boolean): Boolean = {
    var iterator = children.iterator
    while (iterator.hasNext) {
      val value = iterator.next()
      value match {
        case e: Expression if f(e) => return true
        case _                     => // continue
      }
      // exhausted current children continue to next
      if (!iterator.hasNext) {
        iterator = value.children.iterator
      }
    }

    // finally check this node
    this match {
      case e: Expression => f(e)
      case _             => false
    }
  }
}
