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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.rewriting.conditions.LiteralsExtracted
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.SensitiveLiteralsExtracted
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
 * MATCH (n) WHERE n.prop IN [1,2,3] AND NOT n.prop IN [2,3,4] RETURN n.prop
 * -> MATCH (n) WHERE n.prop IN [1]
 *
 * MATCH (n) WHERE n.prop IN [1,2,3] AND n.prop IN [] RETURN n.prop
 * -> MATCH (n) WHERE FALSE
 * </pre>
 * Any Expression that is `AND` or `OR` that immediately contains an `IN()` or
 * `NOT(IN())` is a candidate for a rewrite. All child expressions that are
 * `IN()` or `NOT(IN())` with list literals on their RHS are added to a list of
 * expressions to rewrite, along with a lookup from their predicand to the new
 * list literal formed by merging.
 * <p>
 * Merging replaces two comparisons of the same predicand v by one, computing its list by a set operation:
 * <pre>
 *   #  original                   merged           set operation
 *   1  v IN A AND v IN B          v IN A∩B         intersection
 *   2  v IN A AND NOT v IN B      v IN A∖B         difference
 *   3  NOT v IN A AND v IN B      v IN B∖A         difference
 *   4  NOT v IN A AND NOT v IN B  NOT v IN A∪B     union
 *   5  v IN A OR v IN B           v IN A∪B         union
 *   6  v IN A OR NOT v IN B       NOT v IN B∖A     difference
 *   7  NOT v IN A OR v IN B       NOT v IN A∖B     difference
 *   8  NOT v IN A OR NOT v IN B   NOT v IN A∩B     intersection
 * </pre>
 * This is only correct in two-valued logic (TRUE/FALSE).
 * <p>
 * Three-valued logic (TRUE/FALSE/null) has a third value, which can be interpreted as "unknown value".
 * E.g. `[1, null]` can be understood as "1 and some other element". If we now want to test whether 2 is in the
 * list, we cannot say "no" because we don't know what the second element is. Therefore, the result is null.
 * In summary, `v IN list` is
 * <ul>
 *  <li>TRUE if an element equals v,</li>
 *  <li>null if v is null or the list contains null,</li>
 *  <li>FALSE otherwise,</li>
 * </ul>
 * with one exception: `v IN []` is FALSE even for v = null, because the list is empty and therefore does not
 * contain null.
 * <p>
 * For a non-null v, we can understand a list to be the set of its non-null elements plus a default: null if the
 * list contains null, FALSE if not. The merged comparison has a single default, so the merge is only correct if
 * both lists' defaults combine to it:
 * <ul>
 *  <li>union: always,</li>
 *  <li>intersection: only if either both lists contain null or neither does,</li>
 *  <li>difference: only if no list contains null.</li>
 * </ul>
 * For a null v, every comparison with a non-empty list is null. So an empty merged list is only correct if one of
 * the merged comparisons already is the resulting constant, i.e. `v IN []` (FALSE) or `NOT v IN []` (TRUE).
 * <p>
 * Hence two guards:
 * <ul>
 *  <li>Lists containing null are not merged at all, not even for a union or an intersection where it would be
 *      correct. E.g. `v IN [1, 2] AND NOT v IN [2, null]` must not become `v IN [1]`.</li>
 *  <li>An empty merged list only becomes FALSE (for IN) or TRUE (for NOT IN) if one of the merged comparisons
 *      already is that constant. Otherwise, the comparisons are not merged, e.g. `v IN [1] AND v IN [2]` is null,
 *      not FALSE, for a null v.</li>
 * </ul>
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
 *    And(Not(And(In(v, [1, 2]), In(v, [2, 3]))), Not(Or(In(v, [3, 4]), In(v, [4, 5]))))
 * -> And(Not(    In(v, [2])                   ), Not(   In(v, [3, 4, 5])             ))
 * ->     Not(    In(v, [2, 3, 4, 5])                                                  )
 * </pre>
 * NOTE: this rewriter must be applied before auto parameterization, since after
 * that we are just dealing with opaque parameters.
 */
case object mergeInPredicates extends Step with DefaultPostCondition with PreparatoryRewritingRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    !LiteralsExtracted,
    !SensitiveLiteralsExtracted
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(cypherExceptionFactory: CypherExceptionFactory): Rewriter = instance

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
    def isNegated: Boolean
    def rewrite(original: Expression): Expression
  }

  private case class InList(predicand: Expression, list: Seq[Expression]) extends ListComparison {
    def isNegated: Boolean = false

    def rewrite(original: Expression): Expression = original match {
      case AsComparisonPositions(positions) =>
        if (list.nonEmpty)
          In(predicand, ListLiteral(list)(positions.listPosition))(positions.exprPosition)
        else
          False()(positions.exprPosition)
      case _ =>
        original
    }
  }

  private case class NotInList(predicand: Expression, list: Seq[Expression]) extends ListComparison {
    def isNegated: Boolean = true

    def rewrite(original: Expression): Expression = original match {
      case AsComparisonPositions(positions) =>
        if (list.nonEmpty)
          Not(
            In(predicand, ListLiteral(list)(positions.listPosition))(positions.exprPosition)
          )(positions.exprPosition)
        else
          True()(positions.exprPosition)
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

    def hasEmptyListComparison(predicand: Expression, isNegated: Boolean): Boolean =
      comparisonsToUpdate.exists {
        case AsListComparison(comparison) =>
          comparison.predicand == predicand && comparison.list.isEmpty && comparison.isNegated == isNegated
        case _ => false
      }
  }

  private object PendingUpdates {
    def empty: PendingUpdates = PendingUpdates(Seq.empty, Map.empty)
  }

  /**
   * Matches one of the comparisons to update, extracting the merged comparison that replaces it.
   */
  private class MergedComparisonExtractor(pendingUpdates: PendingUpdates) {

    def unapply(comparison: Expression): Option[ListComparison] = comparison match {
      case AsListComparison(listComparison) if pendingUpdates.comparisonsToUpdate.exists(_ eq comparison) =>
        pendingUpdates.updates.get(listComparison.predicand)
      case _ => None
    }
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

    val pendingUpdates = comparisonsToUpdate(root)

    val predicandsToMerge =
      pendingUpdates.updates
        .filter { case (predicand, merged) =>
          // An empty merged list becomes FALSE (IN) or TRUE (NOT IN), which for a null predicand is only
          // equivalent if one of the merged comparisons already was that constant: `IN []` or `NOT IN []`.
          merged.list.nonEmpty || pendingUpdates.hasEmptyListComparison(predicand, merged.isNegated)
        }

    val MergedComparison = new MergedComparisonExtractor(pendingUpdates)

    topDown {
      Rewriter.lift {
        case comparison @ MergedComparison(mergedComparison)
          if predicandsToMerge.contains(mergedComparison.predicand) =>
          mergedComparison.rewrite(comparison)
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
      case (False(), _) | (_, False()) => False()(and.position)
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
      case (True(), _) | (_, True()) => True()(or.position)
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
          if (keys.contains(listComparison.predicand) && listComparison.list.forall(isNonNullLiteral))
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

  private def isNonNullLiteral(v: Expression): Boolean = v match {
    case _: Null    => false
    case _: Literal => true
    case _          => false
  }
}
