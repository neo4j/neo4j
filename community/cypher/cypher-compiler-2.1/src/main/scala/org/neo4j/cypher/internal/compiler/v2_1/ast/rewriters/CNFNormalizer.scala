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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.repeat

object CNFNormalizer extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val instance: Rewriter = inSequence(
    deMorganRewriter,
    distributeLawsRewriter,
    flattenBooleanOperators,
    simplifyPredicates
  )
}

object deMorganRewriter extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val step = Rewriter.lift {
    case p@Xor(expr1, expr2) =>
      And(Or(expr1, expr2)(p.position), Not(And(expr1, expr2)(p.position))(p.position))(p.position)
    case p@Not(And(exp1, exp2)) =>
      Or(Not(exp1)(p.position), Not(exp2)(p.position))(p.position)
    case p@Not(Or(exp1, exp2)) =>
      And(Not(exp1)(p.position), Not(exp2)(p.position))(p.position)
  }

  private val instance: Rewriter = repeat(bottomUp(step))
}

object distributeLawsRewriter extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val step = Rewriter.lift {
    case p@Or(exp1, And(exp2, exp3)) => And(Or(exp1, exp2)(p.position), Or(exp1, exp3)(p.position))(p.position)
    case p@Or(And(exp1, exp2), exp3) => And(Or(exp1, exp3)(p.position), Or(exp2, exp3)(p.position))(p.position)
  }

  private val instance: Rewriter = repeat(bottomUp(step))
}

object flattenBooleanOperators extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val firstStep: Rewriter = Rewriter.lift {
    case p@And(lhs, rhs) => Ands(Set(lhs, rhs))(p.position)
    case p@Or(lhs, rhs)  => Ors(Set(lhs, rhs))(p.position)
  }

  private val secondStep: Rewriter = Rewriter.lift {
    case p@Ands(exprs) => Ands(exprs.flatMap {
      case Ands(inner) => inner
      case x => Set(x)
    })(p.position)
    case p@Ors(exprs) => Ors(exprs.flatMap {
      case Ors(inner) => inner
      case x => Set(x)
    })(p.position)
  }

  private val instance = inSequence(bottomUp(firstStep), repeat(bottomUp(secondStep)))
}

object simplifyPredicates extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val T = True()(null)
  private val F = False()(null)

  private val step: Rewriter = Rewriter.lift {
    case Not(Not(exp))                    => exp
    case p@Ands(exps) if exps.contains(T) => Ands(exps.filterNot(T == _))(p.position)
    case p@Ors(exps) if exps.contains(F)  => Ors(exps.filterNot(F == _))(p.position)
    case p@Ors(exps) if exps.contains(T)  => True()(p.position)
    case p@Ands(exps) if exps.contains(F) => False()(p.position)
    case p@Ands(exps) if exps.size == 1   => exps.head
    case p@Ors(exps) if exps.size == 1    => exps.head
  }

  private val instance = repeat(bottomUp(step))
}
