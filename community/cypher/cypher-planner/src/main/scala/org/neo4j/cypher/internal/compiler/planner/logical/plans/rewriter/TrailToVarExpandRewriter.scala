/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.RewritableTrailQuantifier
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.RewritableTrailRhs
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.VariableGroupings
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Selection.LabelAndRelTypeInfo
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.Trail.VariableGrouping
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LabelAndRelTypeInfos
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.collection.immutable.ListSet

/**
 * Before
 * .trail((a) ((n)-[r]->(m))+ (b))
 * .|.filter(isRepeatTrailUnique(r))
 * .|.expandAll((n)-[r]->(m))
 * .|.argument(n)
 * .lhs(a)
 *
 * After
 * .expandAll((a)-[r*]->(b))
 * .lhs(a)
 *
 * This rewriter should run after [[RemoveUnusedNamedGroupVariablesPhase]], so that unused group variables are pruned.
 * This rewriter should run before [[pruningVarExpander]] so that the [[PruningVarExpand]] optimisation may take place.
 * This rewriter should run before [[VarLengthRewriter]] so that the quantifier may be rewritten.
 * This rewriter should run before [[UniquenessRewriter]] so that relationship uniqueness predicates may be rewritten.
 */
case class TrailToVarExpandRewriter(
  labelAndRelTypeInfos: LabelAndRelTypeInfos,
  otherAttributes: Attributes[LogicalPlan]
) extends Rewriter {

  val instance: Rewriter = topDown {
    Rewriter.lift {
      case trail @ Trail(
          _,
          RewritableTrailRhs(expand),
          RewritableTrailQuantifier(quantifier),
          _,
          _,
          _,
          _,
          VariableGroupings.Empty(),
          VariableGroupings.Single(relationship),
          _,
          _,
          _,
          _
        ) =>
        val varExpand = createVarExpand(trail, expand, quantifier, relationship)
        val expandWithUniqueRel = maybeAddRelUniquenessPredicates(trail, relationship, varExpand)
        val expandWithUniqueGroupRel = maybeAddGroupRelUniquenessPredicates(trail, relationship, expandWithUniqueRel)
        expandWithUniqueGroupRel
    }
  }

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private def createVarExpand(
    trail: Trail,
    trailExpand: Expand,
    trailQuantifier: VarPatternLength,
    trailRelationship: VariableGrouping
  ): LogicalPlan = {
    def getProjectedDir: SemanticDirection = (trailExpand.dir, trail.reverseGroupVariableProjections) match {
      case (SemanticDirection.BOTH, false) => SemanticDirection.OUTGOING
      case (SemanticDirection.BOTH, true)  => SemanticDirection.INCOMING
      case (direction, false)              => direction
      case (direction, true)               => direction.reversed
    }

    VarExpand(
      source = trail.left,
      from = trail.start,
      to = trail.end,
      relName = trailRelationship.groupName,
      dir = trailExpand.dir,
      projectedDir = getProjectedDir,
      types = trailExpand.types,
      length = trailQuantifier,
      mode = ExpandAll,
      nodePredicates = Seq.empty,
      relationshipPredicates = Seq.empty
    )(SameId(trail.id))
  }

  /**
   * If there are other relationship variables in the query, then we may need to add relationship uniqueness
   * predicates. Whether we need to do this or not will depend on whether the relationships are provably disjoint, and
   * also on whether there are any relationship variables bound before the Trail.
   *
   * Trail.previouslyBoundRelationships does the heavy lifting for us. During the planning of Trail, the planner
   * determines whether it needs to populate this field. It will only populate this field if the trail comes after
   * previously bound relationship variables that are not provably disjoint.
   */
  private def maybeAddRelUniquenessPredicates(
    trail: Trail,
    trailRelationship: VariableGrouping,
    source: LogicalPlan
  ): LogicalPlan = {
    def excluded(groupRelationship: Variable, previouslyBoundedRel: Variable): Expression =
      Not(In(previouslyBoundedRel, groupRelationship)(InputPosition.NONE))(InputPosition.NONE)

    if (trail.previouslyBoundRelationships.nonEmpty) {
      val groupRel = Variable(trailRelationship.groupName)(InputPosition.NONE)
      val predicates: Set[Expression] = trail.previouslyBoundRelationships
        .map(boundRel => Variable(boundRel)(InputPosition.NONE))
        .map(boundRel => excluded(groupRel, boundRel))
      appendSelection(source, predicates)
    } else {
      source
    }
  }

  /**
   * See [[maybeAddRelUniquenessPredicates()]].
   */
  private def maybeAddGroupRelUniquenessPredicates(
    trail: Trail,
    trailRelationship: VariableGrouping,
    source: LogicalPlan
  ): LogicalPlan =
    if (trail.previouslyBoundRelationshipGroups.nonEmpty) {
      val groupRel = Variable(trailRelationship.groupName)(InputPosition.NONE)
      val predicates: Set[Expression] = trail.previouslyBoundRelationshipGroups
        .map(boundRel => Variable(boundRel)(InputPosition.NONE))
        .map(boundRel => Disjoint(groupRel, boundRel)(InputPosition.NONE))
      appendSelection(source, predicates)
    } else {
      source
    }

  /**
   * We are able to set empty LabelAndRelTypeInfos. This information is only used by [[SortPredicatesBySelectivity]],
   * which does up look up statistics for relationship uniqueness predicates, even if it exists. Instead, it looks up
   * constant heuristics.
   */
  private def appendSelection(source: LogicalPlan, predicates: Set[Expression]): LogicalPlan = {
    val id = otherAttributes.copy(source.id).id()
    labelAndRelTypeInfos.set(id, Some(LabelAndRelTypeInfo(Map.empty, Map.empty)))
    Selection(Ands(predicates)(InputPosition.NONE), source)(SameId(id))
  }
}

object TrailToVarExpandRewriter {

  object VariableGroupings {

    object Single {

      def unapply(variableGroupings: Set[VariableGrouping]): Option[VariableGrouping] = {
        Option.when(variableGroupings.size == 1)(variableGroupings.head)
      }
    }

    object Empty {

      def unapply(variableGroupings: Set[VariableGrouping]): Boolean = variableGroupings.isEmpty
    }
  }

  object RewritableTrailRhs {

    def unapply(trailRhs: LogicalPlan): Option[Expand] = {
      def isRelationshipUniquenessExpression(expressions: ListSet[Expression]): Boolean =
        expressions.size == 1 && expressions.head.isInstanceOf[IsRepeatTrailUnique]

      trailRhs match {
        case Selection(Ands(expressions), e @ Expand(_: Argument, _, _, _, _, _, ExpandAll))
          if isRelationshipUniquenessExpression(expressions) =>
          Some(e)
        case _ => None
      }
    }
  }

  object RewritableTrailQuantifier {

    def unapply(repetition: Repetition): Option[VarPatternLength] = {
      for {
        min <- Option.when(repetition.min <= Int.MaxValue.toLong)(repetition.min.toInt)
        max <- Option.when(repetition.max.limit.getOrElse(0L) <= Int.MaxValue.toLong)(repetition.max.limit.map(_.toInt))
      } yield VarPatternLength(min, max)
    }
  }
}
