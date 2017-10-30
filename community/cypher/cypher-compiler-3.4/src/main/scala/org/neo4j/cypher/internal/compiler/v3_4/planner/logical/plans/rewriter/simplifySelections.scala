/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

/**
  * Removes impossible predicates from the plan. Note that this rewriter assumes
  * we have already folded things like `true AND false`, `true OR true`, etcetera.
  */
case object simplifySelections extends Rewriter {

  override def apply(input: AnyRef) = instance.apply(input)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case s@Selection(predicates: Seq[Expression], source) if predicates.forall(isAllFalse) =>
      DropResult(source)(s.solved)
  })

  private def isAllFalse(p: Expression): Boolean = p.treeFold(true) {
    case _: False => (_) => (true, None)
    case Or(l, r) => (acc) =>
      (acc && isAllFalse(l) && isAllFalse(r), None)
    case And(l, r) => (acc) =>
        (acc && (isAllFalse(l) || isAllFalse(r)), None)
    case _ => (_) => (false, None)
  }
}
