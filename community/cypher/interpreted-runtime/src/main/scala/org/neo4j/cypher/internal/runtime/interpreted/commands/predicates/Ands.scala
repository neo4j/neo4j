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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.SelectivityTracker
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.VariableCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.NonEmptyList

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Ands(predicates: NonEmptyList[Predicate]) extends CompositeBooleanPredicate {
  override def shouldExitWhen = IsFalse
  override def andWith(other: Predicate): Predicate = Ands(predicates :+ other)
  override def rewrite(f: Expression => Expression): Expression = f(Ands(predicates.map(_.rewriteAsPredicate(f))))

  override def toString: String = {
    predicates.foldLeft("") {
      case (acc, next) if acc.isEmpty => next.toString
      case (acc, next)                => s"$acc AND $next"
    }
  }

  override def children: Seq[AstNode[_]] = predicates.toIndexedSeq
}

object Ands {

  def apply(predicates: Predicate*): Predicate = predicates.filterNot(_ == True()).toList match {
    case Nil            => True()
    case single :: Nil  => single
    case manyPredicates => Ands(NonEmptyList.from(manyPredicates))
  }
}

case class AndedPropertyComparablePredicates(
  ident: VariableCommand,
  prop: Expression,
  override val predicates: NonEmptyList[ComparablePredicate]
) extends CompositeBooleanPredicate {

  // some rewriters change the type of this, and we can't allow that
  private def rewriteVariableIfNotTypeChanged(f: Expression => Expression) =
    ident.rewrite(f) match {
      case i: Variable => i
      case _           => ident
    }

  override def rewrite(f: Expression => Expression): Expression =
    f(AndedPropertyComparablePredicates(
      rewriteVariableIfNotTypeChanged(f),
      prop.rewrite(f),
      predicates.map(_.rewriteAsPredicate(f).asInstanceOf[ComparablePredicate])
    ))

  override def shouldExitWhen: IsMatchResult = IsFalse

  override def children: Seq[AstNode[_]] = Seq(ident, prop) ++ predicates.toIndexedSeq
}

case class AndsWithSelectivityTracking(predicates: Vector[Predicate]) extends Predicate {

  private val selectivityTracker: SelectivityTracker = new SelectivityTracker(predicates.size)

  def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    val result = selectivityTracker.getOrder().foldLeft(Try[IsMatchResult](IsTrue)) {
      (previousValue, predicateIndex) =>
        previousValue match {
          case Success(IsFalse) => previousValue
          case _ =>
            Try(predicates(predicateIndex).isMatch(ctx, state)) match {
              case result @ Success(IsFalse) =>
                selectivityTracker.onPredicateResult(predicateIndex, isTrue = false)
                result

              case result =>
                selectivityTracker.onPredicateResult(predicateIndex, isTrue = true)
                result match {
                  case Success(IsUnknown) if previousValue.isSuccess => result
                  case Failure(_) if previousValue.isSuccess         => result
                  case _                                             => previousValue
                }
            }
        }
    }

    selectivityTracker.onRowFinished()

    result match {
      case Failure(e)      => throw e
      case Success(option) => option
    }
  }

  override def children: Seq[AstNode[_]] = predicates

  override def rewrite(f: Expression => Expression): Expression =
    f(AndsWithSelectivityTracking(predicates.map(_.rewriteAsPredicate(f))))

  override def toString: String =
    predicates.mkString(" ~AND ~")

  override def arguments: Seq[Expression] = predicates
}
