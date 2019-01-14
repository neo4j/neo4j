/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions.{AndedPropertyInequalities, Equals, In, ListLiteral}

case object simplifyPredicates extends Rewriter {
  override def apply(input: AnyRef) = instance.apply(input)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case in@In(exp, ListLiteral(values@Seq(idValueExpr))) if values.size == 1 =>
      Equals(exp, idValueExpr)(in.position)

    // This form is used to make composite index seeks and scans
    case AndedPropertyInequalities(_, _, predicates) if predicates.size == 1 =>
      predicates.head
  })
}
