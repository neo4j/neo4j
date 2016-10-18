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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_1._
import org.neo4j.cypher.internal.frontend.v3_1.ast._

/**
  * In order to make planning of longer variable length paths more efficient,
  * this rewriter introduced anonymous nodes in variable length paths. This makes
  * it easier, during planning, to solve these paths from both sides and then
  * join in the middle.
  *
  * Long term, the solution should be a bidirectional variable length expand operator.
  *
  * ()-[*..x]->() ==> ()-[*..x/2]->()-[*0..x/2 + x%2]->()
  */
case object variableLengthSplitter extends Rewriter {
  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case pattern@RelationshipChain(leftPattern, rel@RelationshipPattern(None, _, types, Some(Some(range)), props, dir), _)
      if range.lower.isEmpty && range.upper.exists(_.value > 1) =>

      val pos = rel.position

      val totalLength = range.upper.get.value
      val half = totalLength / 2
      val one = UnsignedDecimalIntegerLiteral(1)(pos)
      val lhsRange =
        Some(Some(Range(Some(one), Some(UnsignedDecimalIntegerLiteral(half)(pos)))(pos)))

      val relationshipPattern = rel.copy(length = lhsRange)(pos.bumped())
      val newNode = NodePattern(None, List(), None)(pos.bumped().bumped())
      val newChain = RelationshipChain(leftPattern, relationshipPattern, newNode)(pos)
      val rhsUpper = Some(UnsignedDecimalIntegerLiteral(half + (totalLength % 2))(pos))
      val rhsRange = Range(Some(UnsignedDecimalIntegerLiteral(0)(pos)), rhsUpper)(pos)

      val newShorterVarLength = rel.copy(length = Some(Some(rhsRange)))(pos)

      pattern.copy(element = newChain, relationship = newShorterVarLength)(pos)
  }

  private val doNotDescendInto: (AnyRef) => Boolean = x =>
    x.isInstanceOf[ShortestPaths] ||
    x.isInstanceOf[Expression]

  private val instance = bottomUp(rewriter, doNotDescendInto)
}
