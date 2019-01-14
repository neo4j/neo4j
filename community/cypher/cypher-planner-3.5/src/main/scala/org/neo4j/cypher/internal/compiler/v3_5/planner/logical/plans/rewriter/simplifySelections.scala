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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.attribution.SameId
import org.neo4j.cypher.internal.v3_5.util.{Rewriter, bottomUp}

/**
  * Removes impossible predicates from the plan. Note that this rewriter assumes
  * we have already folded things like `true AND false`, `true OR true`, etcetera.
  */
case object simplifySelections extends Rewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case s@Selection(Ands(preds), source) if isFalse(preds) =>
      DropResult(source)(SameId(s.id))

    case Selection(Ands(preds), source) if isTrue(preds) => source
  })

  private def isTrue(predicates: Set[Expression]): Boolean = predicates.forall {
    case _:True => true
    case _ => false
  }

  private def isFalse(predicates: Set[Expression]): Boolean = predicates.forall {
    case _:False => true
    case _ => false
  }
}
