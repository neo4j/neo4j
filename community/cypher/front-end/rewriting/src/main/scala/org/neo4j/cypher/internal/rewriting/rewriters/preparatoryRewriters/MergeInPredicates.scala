/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.rewriting.conditions.LiteralsExtracted
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.topDown

/**
 * Merges multiple `IN` predicates of lists of literals into one.
 * <p>
 * Examples:
 * <pre>
 * MATCH (n) WHERE n.prop IN [1,2,3] AND n.prop IN [2,3,4] RETURN n.prop
 * -> MATCH (n) WHERE n.prop IN [2,3]
 *
 * MATCH (n) WHERE n.prop IN [1,2,3] OR n.prop IN [2,3,4] RETURN n.prop
 * -> MATCH (n) WHERE n.prop IN [1,2,3,4]
 *
 * MATCH (n) WHERE n.prop IN [1,2,3] OR NOT n.prop IN [2,3,4] RETURN n.prop
 * -> MATCH (n) WHERE NOT n.prop IN [4]
 *
 * MATCH (n) WHERE n.prop IN [1,2,3] AND n.prop IN [4,5,6] RETURN n.prop
 * -> MATCH (n) WHERE FALSE
 * </pre>
 * Any Expression that is `AND` or `OR` that immediately contains an `IN()` or 
 * `NOT(IN())` is a candidate for a rewrite. All child expressions that are 
 * `IN()` or `NOT(IN())` with list literals on their RHS are added to a list of
 * expressions to rewrite, along with a lookup from their predicand to the new 
 * list literal formed by merging.
 *
 * The AND and OR are further simplified using these operations:
 * <pre>
 *  - P AND P => P
 *  - P OR P => P
 *  - TRUE AND P => P
 *  - FALSE AND P => FALSE
 *  - TRUE OR P => TRUE
 *  - FALSE OR P => P
 * </pre>
 * The rewriter works bottom up, allowing it to make a series of simplifications
 * that ripple upwards. For example:
 * <pre>
 *    And(Not(And(In(v, [1, 2]), In(v, [3, 4]))), Not(Or(In(v, [3, 4]), In([4, 5]))))
 * -> And(Not(    In(v, [])                    ), Not(   In(v, [3, 4, 5])          ))
 * -> And(Not(    False()                      ), Not(   In(v, [3, 4, 5])          ))
 * -> And(        True()                        , Not(   In(v, [3, 4, 5])          ))
 * ->                                             Not(   In(v, [3, 4, 5])          )
 * </pre>
 * NOTE: this rewriter must be applied before auto parameterization, since after
 * that we are just dealing with opaque parameters.
 */
case object MergeInPredicates extends Step with DefaultPostCondition with PreparatoryRewritingRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(!LiteralsExtracted)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    versionOpt: Option[CypherVersion]
  ): Rewriter = instance

  // Rewrites AND / OR that directly contain IN or NOT IN. Binary operators are simplified where
  // one or more operands are TRUE or FALSE e.g. FALSE AND p => FALSE, FALSE OR p => p.
  val instance: Rewriter = bottomUp {
    Rewriter.lift { case AsBinaryBoolean(target) => rewriteBinaryBoolean(target) }
  }

  private def topLevelComparisonExists(expressions: Expression*): Boolean = expressions.exists {
    case AsListComparison(_) => true
    case _                   => false
  }

  private object AsListComparison {

    def unapply(v: Expression): Option[ListComparison] = v match {
      case Not(In(expr, ListLiteral(list))) => Some(NotInList(expr, list.distinct))
      case In(expr, ListLiteral(list))      => Some(InList(expr, list.distinct))
      case _                                => None
    }
  }

  sealed private trait ListComparison {
    def predicand: Expression
    def list: Seq[Expression]
    def rewrite(original: Expression): Expression
  }

  private case class InList(predicand: Expression, list: Seq[Expression]) extends ListComparison {

    def rewrite(original: Expression): Expression = original match {
      case AsComparisonPositions(positions) =>
        if (list.nonEmpty)
          In(predicand, ListLiteral(list)(positions.listPosition))(positions.exprPosition)
        else
          False()(positions.exprPosition.zeroLength)
      case _ =>
        original
    }
  }

  private case class NotInList(predicand: Expression, list: Seq[Expression]) extends ListComparison {

    def rewrite(original: Expression): Expression = original match {
      case AsComparisonPositions(positions) =>
        if (list.nonEmpty)
          Not(
            In(predicand, ListLiteral(list)(positions.listPosition))(positions.exprPosition)
          )(positions.exprPosition)
        else
          True()(positions.exprPosition.zeroLength)
      case _ =>
        original
    }
  }

  /**
   * @param comparisonsToUpdate the IN and NOT(IN()) expressions that need to be rewritten
   * @param updates map of predicands (the LHS of the expressions to be rewritten) to their merged list of literal values
   */
  private case class PendingUpdates(
    comparisonsToUpdate: Seq[Expression],
    updates: Map[Expression, ListComparison]
  ) {

    def addComparison(
      binaryBoolean: BinaryBoolean,
      comparisonExpr: Expression,
      newComparison: ListComparison
    ): PendingUpdates =
      PendingUpdates(
        comparisonsToUpdate :+ comparisonExpr,
        updates.updatedWith(newComparison.predicand) {
          case Some(curComparison) => Some(binaryBoolean.merge(curComparison, newComparison))
          case None                => Some(newComparison)
        }
      )
  }

  private object PendingUpdates {
    def empty: PendingUpdates = PendingUpdates(Seq.empty, Map.empty)
  }

  private case class ComparisonPositions(exprPosition: InputPosition, listPosition: InputPosition)

  private object AsComparisonPositions {

    def unapply(v: Expression): Option[ComparisonPositions] = v match {
      case not @ Not(In(_, list @ ListLiteral(_))) => Some(ComparisonPositions(not.position, list.position))
      case in @ In(_, list @ ListLiteral(_))       => Some(ComparisonPositions(in.position, list.position))
      case _                                       => None
    }
  }

  private def rewriteBinaryBoolean(binaryBoolean: BinaryBoolean): Expression = {

    val rewriter = comparisonRewriter(binaryBoolean)

    val newLhs = binaryBoolean.lhs.endoRewrite(rewriter)
    val newRhs = binaryBoolean.rhs.endoRewrite(rewriter)

    // If both operands of AND / OR are the same, the binary
    // expression can be replaced by one of the operands.
    if (newLhs == newRhs)
      newLhs
    else
      binaryBoolean.copy(newLhs, newRhs)
  }

  private def comparisonRewriter(root: BinaryBoolean) = {

    val PendingUpdates(toUpdate, updates) = comparisonsToUpdate(root)

    topDown {
      Rewriter.lift {
        case node @ AsListComparison(listComparison) if toUpdate.exists(_ eq node) =>
          updates(listComparison.predicand).rewrite(node)
      }
    }
  }

  sealed private trait BinaryBoolean {
    def lhs: Expression
    def rhs: Expression
    def merge(lhs: ListComparison, rhs: ListComparison): ListComparison
    def copy(newLhs: Expression, newRhs: Expression): Expression
  }

  private case class AndBinaryBoolean(and: And) extends BinaryBoolean {
    def lhs: Expression = and.lhs
    def rhs: Expression = and.rhs

    def merge(lhs: ListComparison, rhs: ListComparison): ListComparison =
      (lhs, rhs) match {
        case (l: InList, _: InList)       => l.copy(list = lhs.list.intersect(rhs.list))
        case (l: InList, _: NotInList)    => l.copy(list = lhs.list.diff(rhs.list))
        case (_: NotInList, r: InList)    => r.copy(list = rhs.list.diff(lhs.list))
        case (l: NotInList, _: NotInList) => l.copy(list = lhs.list.concat(rhs.list).distinct)
      }

    def copy(newLhs: Expression, newRhs: Expression): Expression = (newLhs, newRhs) match {
      case (False(), _) | (_, False()) => False()(and.position.zeroLength)
      case (True(), newRhs)            => newRhs
      case (newLhs, True())            => newLhs
      case _                           => and.copy(newLhs, newRhs)(and.position)
    }
  }

  private case class OrBinaryBoolean(or: Or) extends BinaryBoolean {
    def lhs: Expression = or.lhs
    def rhs: Expression = or.rhs

    def merge(lhs: ListComparison, rhs: ListComparison): ListComparison =
      (lhs, rhs) match {
        case (l: InList, _: InList)       => l.copy(list = lhs.list.concat(rhs.list).distinct)
        case (_: InList, r: NotInList)    => r.copy(list = rhs.list.diff(lhs.list))
        case (l: NotInList, _: InList)    => l.copy(list = lhs.list.diff(rhs.list))
        case (l: NotInList, _: NotInList) => l.copy(list = lhs.list.intersect(rhs.list))
      }

    def copy(newLhs: Expression, newRhs: Expression): Expression = (newLhs, newRhs) match {
      case (True(), _) | (_, True()) => True()(or.position.zeroLength)
      case (False(), newRhs)         => newRhs
      case (newLhs, False())         => newLhs
      case _                         => or.copy(newLhs, newRhs)(or.position)
    }
  }

  private object AsBinaryBoolean {

    def unapply(expr: Expression): Option[BinaryBoolean] = expr match {
      case and @ And(lhs, rhs) if topLevelComparisonExists(lhs, rhs) => Some(AndBinaryBoolean(and))
      case or @ Or(lhs, rhs) if topLevelComparisonExists(lhs, rhs)   => Some(OrBinaryBoolean(or))
      case _                                                         => None
    }
  }

  // Lists can be merged across a tree of mixture of AND / OR / NOT if the predicands (the LHS of
  // the IN comparisons) are all the same. For example, (A IN L1 AND (A IN L2 OR A IN L3)) can be
  // rewritten to (A IN (L1 ∩ (L2 ∪ L3))), whereas (A IN L1 AND (B IN L2 OR A IN L3)) can't.
  private def comparisonsToUpdate(binaryBoolean: BinaryBoolean): PendingUpdates = {

    val expressions = Seq(binaryBoolean.lhs, binaryBoolean.rhs)

    val keys = expressions.collect { case AsListComparison(in) => in.predicand }.toSet

    expressions.foldLeft(PendingUpdates.empty)((pendingUpdates, expression) =>
      expression.folder.treeFold(pendingUpdates) {

        case expr @ AsListComparison(listComparison) =>
          if (keys.contains(listComparison.predicand) && listComparison.list.forall(isLiteral))
            acc => SkipChildren(acc.addComparison(binaryBoolean, expr, listComparison))
          else
            acc => SkipChildren(acc)

        case _: Or =>
          binaryBoolean match {
            case _: OrBinaryBoolean  => acc => TraverseChildren(acc)
            case _: AndBinaryBoolean => acc => SkipChildren(acc)
          }

        case _: And =>
          binaryBoolean match {
            case _: AndBinaryBoolean => acc => TraverseChildren(acc)
            case _: OrBinaryBoolean  => acc => SkipChildren(acc)
          }

        case _ =>
          acc => SkipChildren(acc)
      }
    )
  }

  private def isLiteral(v: Expression): Boolean = v match {
    case _: Literal => true
    case _          => false
  }
}
