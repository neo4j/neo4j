/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.rewriting.rewriters

import org.neo4j.cypher.internal.v4_0.ast.{Clause, Match, Merge, Where}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, _}
import org.neo4j.cypher.internal.v4_0.util._

case class AddUniquenessPredicates(innerVariableNamer: InnerVariableNamer = SameNameNamer) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case m@Match(_, pattern: Pattern, _, where: Option[Where]) =>
      val uniqueRels: Seq[UniqueRel] = collectUniqueRels(pattern)
      if (uniqueRels.size < 2) {
        m
      } else {
        val newWhere = addPredicate(m, uniqueRels, where)
        m.copy(where = newWhere)(m.position)
      }
    case m@Merge(pattern: Pattern, _, where: Option[Where]) =>
      val uniqueRels: Seq[UniqueRel] = collectUniqueRels(pattern)
      if (uniqueRels.size < 2) {
        m
      } else {
        val newWhere = addPredicate(m, uniqueRels, where)
        m.copy(where = newWhere)(m.position)
      }
  }

  private def addPredicate(clause: Clause, uniqueRels:  Seq[UniqueRel], where: Option[Where]): Option[Where] = {
    val maybePredicate: Option[Expression] = createPredicateFor(uniqueRels, clause.position)
    val newWhere: Option[Where] = (where, maybePredicate) match {
      case (Some(oldWhere), Some(newPredicate)) =>
        Some(oldWhere.copy(expression = And(oldWhere.expression, newPredicate)(clause.position))(clause.position))

      case (None, Some(newPredicate)) =>
        Some(Where(expression = newPredicate)(clause.position))

      case (oldWhere, None) => oldWhere
    }
    newWhere
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])

  def collectUniqueRels(pattern: ASTNode): Seq[UniqueRel] =
    pattern.treeFold(Seq.empty[UniqueRel]) {
      case _:ScopeExpression =>
        acc => (acc, None)

      case _: ShortestPaths =>
        acc => (acc, None)

      case RelationshipChain(_, patRel@RelationshipPattern(optIdent, types, _, _, _, _, _), _) =>
        acc => {
          val ident = optIdent.getOrElse(throw new IllegalStateException("This rewriter cannot work with unnamed patterns"))
          (acc :+ UniqueRel(ident, types.toSet, patRel.isSingleLength), Some(identity))
        }
    }

  private def createPredicateFor(uniqueRels: Seq[UniqueRel], pos: InputPosition): Option[Expression] = {
    createPredicatesFor(uniqueRels, pos).reduceOption(expressions.And(_, _)(pos))
  }

  def createPredicatesFor(uniqueRels: Seq[UniqueRel], pos: InputPosition): Seq[Expression] =
    for {
      x <- uniqueRels
      y <- uniqueRels if x.name < y.name && !x.isAlwaysDifferentFrom(y)
    } yield {
      (x.singleLength, y.singleLength) match {
        case (true, true) =>
          Not(Equals(x.variable.copyId, y.variable.copyId)(pos))(pos)

        case (true, false) =>
          val innerY = innerVariableNamer.create(y.variable)
          NoneIterablePredicate(innerY, y.variable.copyId, Some(Equals(x.variable.copyId, innerY.copyId)(pos)))(pos)

        case (false, true) =>
          val innerX = innerVariableNamer.create(x.variable)
          NoneIterablePredicate(innerX, x.variable.copyId, Some(Equals(innerX.copyId, y.variable.copyId)(pos)))(pos)

        case (false, false) =>
          val innerX = innerVariableNamer.create(x.variable)
          val innerY = innerVariableNamer.create(y.variable)
          NoneIterablePredicate(innerX, x.variable.copyId, Some(AnyIterablePredicate(innerY, y.variable.copyId, Some(Equals(innerX.copyId, innerY.copyId)(pos)))(pos)))(pos)
      }
    }

  case class UniqueRel(variable: LogicalVariable, types: Set[RelTypeName], singleLength: Boolean) {
    def name: String = variable.name

    def isAlwaysDifferentFrom(other: UniqueRel): Boolean =
      types.nonEmpty && other.types.nonEmpty && (types intersect other.types).isEmpty
  }
}

trait InnerVariableNamer {
  def create(outer: LogicalVariable): LogicalVariable
}

case object SameNameNamer extends InnerVariableNamer {
  override def create(outer: LogicalVariable): LogicalVariable = outer.copyId
}

class GeneratingNamer() extends InnerVariableNamer {
  private var i = 0
  override def create(outer: LogicalVariable): LogicalVariable = {
    i += 1
    Variable(s"  INNER$i")(outer.position)
  }
}
