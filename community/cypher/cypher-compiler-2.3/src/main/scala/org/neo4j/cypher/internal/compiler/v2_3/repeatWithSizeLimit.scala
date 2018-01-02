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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.ast.ASTNode
import org.neo4j.cypher.internal.frontend.v2_3.Foldable._
import org.neo4j.cypher.internal.frontend.v2_3.Rewriter

import scala.annotation.tailrec

/*
This rewriter tries to limit rewriters that grow the product AST too much
 */
case class repeatWithSizeLimit(rewriter: Rewriter)(implicit val monitor: AstRewritingMonitor) extends Rewriter {

  private def astNodeSize(value: Any): Int = value.treeFold(1) {
    case _: ASTNode => (acc, children) => children(acc+1)
  }

  final def apply(that: AnyRef): AnyRef = {
    val initialSize = astNodeSize(that)
    val limit = initialSize * initialSize

    innerApply(that, limit)
  }

  @tailrec
  private def innerApply(that: AnyRef, limit: Int): AnyRef = {
    val t = rewriter.apply(that)
    val newSize = astNodeSize(t)

    if (newSize > limit) {
      monitor.abortedRewriting(that)
      that
    }
    else if (t == that) {
      t
    }
    else {
      innerApply(t, limit)
    }
  }
}
