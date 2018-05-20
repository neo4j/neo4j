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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast

import org.opencypher.v9_0.expressions.{Expression, LogicalProperty, LogicalVariable}

case class NullCheck(offset: Int, inner: Expression) extends RuntimeExpression

// This needs to be used to be able to rewrite an expression declared as a LogicalVariable
case class NullCheckVariable(offset: Int, inner: LogicalVariable) extends RuntimeVariable(inner.name)

// This needs to be used to be able to rewrite an expression declared as a LogicalProperty
case class NullCheckProperty(offset: Int, inner: LogicalProperty) extends RuntimeProperty(inner) {

  // We have to override the implementation in RuntimeProperty for correctness. This smells a bit...
  override def dup(children: Seq[AnyRef]): this.type = {
    val newOffset = children.head.asInstanceOf[Int]
    val newInner = children(1).asInstanceOf[LogicalProperty]
    // We only ever rewrite this with inner already rewritten, so we should not need to copy
    if (offset == newOffset && inner == newInner)
      this
    else
      copy(offset = newOffset, inner = newInner).asInstanceOf[this.type]
  }
}
