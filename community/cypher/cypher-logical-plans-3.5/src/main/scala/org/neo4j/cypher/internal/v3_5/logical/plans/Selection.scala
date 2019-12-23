/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.neo4j.cypher.internal.v3_5.expressions.{Ands, Expression}
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen

/**
  * For each source row, produce it if all predicates are true.
  */
case class Selection(predicate: Ands,
                     source: LogicalPlan
                    )(implicit idGen: IdGen) extends LogicalPlan(idGen) with LazyLogicalPlan {
  assert(predicate.exprs.nonEmpty, "A selection plan should never be created without predicates")

  val lhs = Some(source)
  def rhs = None

  def numPredicates: Int = predicate.exprs.size

  val availableSymbols: Set[String] = source.availableSymbols
}

object Selection {
  def apply(predicates: Seq[Expression], source: LogicalPlan)(implicit idGen: IdGen): Selection =  {
    assert(predicates.nonEmpty, "A selection plan should never be created without predicates")
    Selection(Ands(predicates.toSet)(predicates.head.position), source)
  }
}

object SelectionMatcher {
  def unapply(arg: Selection): Option[(Seq[Expression], LogicalPlan)] = {
    Some(
      (arg.predicate.exprs.toSeq, arg.source)
      )
  }
}