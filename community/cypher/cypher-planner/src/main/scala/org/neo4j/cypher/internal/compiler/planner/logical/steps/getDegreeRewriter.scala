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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.steps.getDegreeRewriter.isEligible
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

case object getDegreeRewriter extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val instance = bottomUp(
    rewriter,
    // Do not traverse into NestedPlanExpressions as they have been optimized already by an earlier call to plan
    _.isInstanceOf[NestedPlanExpression]
  )

  private def rewriter = Rewriter.lift {

    // GetDegree > limit
    case e @ GreaterThan(GetDegree(node, typ, dir), limit) =>
      HasDegreeGreaterThan(node, typ, dir, limit)(e.position)
    // limit > GetDegree
    case e @ GreaterThan(limit, GetDegree(node, typ, dir)) =>
      HasDegreeLessThan(node, typ, dir, limit)(e.position)
    // GetDegree >= limit
    case e @ GreaterThanOrEqual(GetDegree(node, typ, dir), limit) =>
      HasDegreeGreaterThanOrEqual(node, typ, dir, limit)(e.position)
    // limit >= GetDegree
    case e @ GreaterThanOrEqual(limit, GetDegree(node, typ, dir)) =>
      HasDegreeLessThanOrEqual(node, typ, dir, limit)(e.position)
    // GetDegree < limit
    case e @ LessThan(GetDegree(node, typ, dir), limit) =>
      HasDegreeLessThan(node, typ, dir, limit)(e.position)
    // limit < GreaterThan
    case e @ LessThan(limit, GetDegree(node, typ, dir)) =>
      HasDegreeGreaterThan(node, typ, dir, limit)(e.position)
    // GetDegree <= limit
    case e @ LessThanOrEqual(GetDegree(node, typ, dir), limit) =>
      HasDegreeLessThanOrEqual(node, typ, dir, limit)(e.position)
    // limit <= GreaterThan
    case e @ LessThanOrEqual(limit, GetDegree(node, typ, dir)) =>
      HasDegreeGreaterThanOrEqual(node, typ, dir, limit)(e.position)
    // GetDegree = limit
    case e @ Equals(GetDegree(node, typ, dir), value) =>
      HasDegree(node, typ, dir, value)(e.position)
    // limit = GreaterThan
    case e @ Equals(value, GetDegree(node, typ, dir)) =>
      HasDegree(node, typ, dir, value)(e.position)

    // SIZE( [p=(a)-[]->() | p] )
    // COUNT { (a)-[]->() }
    case EligibleCountLikeIRExpression(node, types, dir) =>
      calculateUsingGetDegree(node, types, dir)

    // EXISTS( (a)-[]->() ) and EXISTS { (a)-[]->() } rewritten to GetDegree( (a)-[]->() ) > 0
    case EligibleExistsIRExpression(node, types, dir) =>
      existsToUsingHasDegreeGreaterThan(node, types, dir)
  }

  private def calculateUsingGetDegree(
    node: LogicalVariable,
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ): Expression = {
    types
      .map(typ => GetDegree(node, Some(typ), dir)(typ.position))
      .reduceOption[Expression](Add(_, _)(InputPosition.NONE))
      .getOrElse(GetDegree(node, None, dir)(InputPosition.NONE))
  }

  private def existsToUsingHasDegreeGreaterThan(
    node: LogicalVariable,
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ): Expression = {
    val all = types.map(typ =>
      HasDegreeGreaterThan(
        node,
        Some(typ),
        dir,
        SignedDecimalIntegerLiteral("0")(InputPosition.NONE)
      )(
        typ.position
      )
    )
    if (all.isEmpty) {
      HasDegreeGreaterThan(
        node,
        None,
        dir,
        SignedDecimalIntegerLiteral("0")(InputPosition.NONE)
      )(
        InputPosition.NONE
      )
    } else if (all.size == 1) {
      all.head
    } else {
      Ors(all)(InputPosition.NONE)
    }
  }

  def isEligible(
    pe: Expression,
    node: LogicalVariable,
    rel: LogicalVariable,
    otherNode: LogicalVariable,
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ): Option[(LogicalVariable, Seq[RelTypeName], SemanticDirection)] = {
    val peDeps = pe.dependencies
    if (peDeps.contains(node) && !peDeps.contains(rel) && !peDeps.contains(otherNode)) {
      Some((node, types, dir))
    } else if (!peDeps.contains(node) && !peDeps.contains(rel) && peDeps.contains(otherNode)) {
      Some((otherNode, types, dir.reversed))
    } else {
      None
    }
  }
}

object QuerySolvableByGetDegree {

  object SetExtractor {
    def unapplySeq[T](s: Set[T]): Option[Seq[T]] = Some(s.toSeq)
  }

  def unapply(arg: Any)
    : Option[(LogicalVariable, LogicalVariable, LogicalVariable, Seq[RelTypeName], SemanticDirection)] = arg match {
    case RegularSinglePlannerQuery(
        QueryGraph(
          SetExtractor(PatternRelationship(
            relationship,
            (firstNode, secondNode),
            direction,
            types,
            SimplePatternLength
          )),
          SetExtractor(),
          patternNodes,
          SetExtractor(argument),
          Selections.empty,
          IndexedSeq(),
          SetExtractor(),
          SetExtractor(),
          IndexedSeq(),
          SetExtractor()
        ),
        InterestingOrder.empty,
        RegularQueryProjection(_, QueryPagination.empty, Selections.empty, _) | _: AggregatingQueryProjection,
        None,
        None
      ) if patternNodes.contains(argument) && patternNodes == Set(firstNode, secondNode) =>
      Some((firstNode, relationship, secondNode, types, direction))
    case _ => None
  }
}

object ExistsQuerySolvableByGetDegree {

  object SetExtractor {
    def unapplySeq[T](s: Set[T]): Option[Seq[T]] = Some(s.toSeq)
  }

  // TODO REVIEWER: the old AggregatingQueryProjection case looked incorrect to me.
  //                this rewrite started kicking in because interestingOrder is now empty.
  private def eligibleHorizon(horizon: QueryHorizon): Boolean = horizon match {
    case RegularQueryProjection(
        _,
        QueryPagination.empty,
        Selections.empty,
        _
      ) => true
    case AggregatingQueryProjection(groups, _, _, _, _) if groups.nonEmpty => true
    case _                                                                 => false
  }

  def unapply(arg: Any)
    : Option[(LogicalVariable, LogicalVariable, LogicalVariable, Seq[RelTypeName], SemanticDirection)] = arg match {
    case RegularSinglePlannerQuery(
        QueryGraph(
          SetExtractor(PatternRelationship(
            relationship,
            (firstNode, secondNode),
            direction,
            types,
            SimplePatternLength
          )),
          SetExtractor(),
          patternNodes,
          SetExtractor(argument),
          Selections.empty,
          IndexedSeq(),
          SetExtractor(),
          SetExtractor(),
          IndexedSeq(),
          SetExtractor()
        ),
        InterestingOrder.empty,
        horizon,
        None,
        None
      ) if patternNodes.contains(argument) && patternNodes == Set(firstNode, secondNode) && eligibleHorizon(horizon) =>
      Some((firstNode, relationship, secondNode, types, direction))
    case _ => None
  }
}

object EligibleExistsIRExpression {

  def unapply(arg: Any): Option[(LogicalVariable, Seq[RelTypeName], SemanticDirection)] = arg match {
    case e @ ExistsIRExpression(
        ExistsQuerySolvableByGetDegree(
          node,
          rel,
          otherNode,
          types,
          dir
        ),
        _,
        _
      ) =>
      isEligible(e, node, rel, otherNode, types, dir)

    case _ => None
  }
}

object EligibleCountLikeIRExpression {

  def unapply(arg: Any): Option[(LogicalVariable, Seq[RelTypeName], SemanticDirection)] = arg match {
    case Size(
        e @ ListIRExpression(
          QuerySolvableByGetDegree(
            node,
            rel,
            otherNode,
            types,
            dir
          ),
          _,
          _,
          _
        )
      ) =>
      isEligible(e, node, rel, otherNode, types, dir)

    case ce @ CountIRExpression(
        QuerySolvableByGetDegree(
          node,
          rel,
          otherNode,
          types,
          dir
        ),
        _,
        _
      ) =>
      isEligible(ce, node, rel, otherNode, types, dir)

    case _ => None
  }
}
