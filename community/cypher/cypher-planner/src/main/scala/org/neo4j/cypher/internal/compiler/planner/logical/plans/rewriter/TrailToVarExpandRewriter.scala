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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.helpers.IterableHelper.RichIterableOnce
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.InlinableRelationshipPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.NodeToRelationshipRewritablePredicates
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.RewritableTrailQuantifier
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.RewritableTrailRhs
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriter.VariableGroupings
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Selection.LabelAndRelTypeInfo
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LabelAndRelTypeInfos
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewriter will sometimes transform a Trail into a VarExpand, like in the example below.
 *
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
 * Trail is more powerful than VarExpand, in the sense that Trail can do more things than VarExpand. We consider Trail
 * and VarExpand to be equivalent when the following conditions are met:
 *  - the Trail relationship pattern contains a single relationship
 *  - the Trail node group variables are not used by downstream logical plans and thus empty
 *  - the Trail inner node variables are only used during path expansion in predicates within the QPP, and the QPP is a single directional relationship, i.e., in case where it can be substituted with startNode/endNode of the relationship.
 *  - the Trail quantifier can be converted losslessly from Long to Int
 *
 * This rewriter should run after [[RemoveUnusedNamedGroupVariablesPhase]], so that unused group variables are pruned.
 * This rewriter should run before [[pruningVarExpander]] so that the [[PruningVarExpand]] optimisation may take place.
 * This rewriter should run before [[VarLengthRewriter]] so that the quantifier may be rewritten.
 * This rewriter should run before [[UniquenessRewriter]] so that relationship uniqueness predicates may be rewritten.
 */
case class TrailToVarExpandRewriter(
  labelAndRelTypeInfos: LabelAndRelTypeInfos,
  otherAttributes: Attributes[LogicalPlan]
) extends Rewriter with TopDownMergeableRewriter {

  override val innerRewriter: Rewriter = Rewriter.lift {
    case trail @ Trail(
        _,
        RewritableTrailRhs(
          expand,
          inlinablePredicates
        ),
        RewritableTrailQuantifier(quantifier),
        _,
        _,
        _,
        _,
        VariableGroupings.Empty(),
        VariableGroupings.Maybe(relationship),
        _,
        _,
        _,
        _
      ) =>
      val varExpandRel = relationship.map(_.group).getOrElse(trail.innerRelationships.head)

      val rewrittenRelationshipPredicates = inlinablePredicates.collect {
        case InlinableRelationshipPredicate(predicate, relationship) =>
          VariablePredicate(relationship, predicate)
        case NodeToRelationshipRewritablePredicates(predicate, startNode, endNode, relationship) =>
          VariablePredicate(
            relationship,
            predicate.endoRewrite(NodeToRelationshipExpressionRewriter(startNode, endNode, relationship))
          )
      }

      val varExpand = createVarExpand(trail, expand, quantifier, rewrittenRelationshipPredicates, varExpandRel)
      val expandWithUniqueRel = maybeAddRelUniquenessPredicates(trail, varExpandRel, varExpand)
      val expandWithUniqueGroupRel = maybeAddGroupRelUniquenessPredicates(trail, varExpandRel, expandWithUniqueRel)
      expandWithUniqueGroupRel
  }

  private val instance: Rewriter = topDown(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private def createVarExpand(
    trail: Trail,
    trailExpand: Expand,
    trailQuantifier: VarPatternLength,
    trailRelationshipPredicates: Seq[VariablePredicate],
    expandRel: LogicalVariable
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
      relName = expandRel,
      dir = trailExpand.dir,
      projectedDir = getProjectedDir,
      types = trailExpand.types,
      length = trailQuantifier,
      mode = ExpandAll,
      nodePredicates = Seq.empty,
      relationshipPredicates = trailRelationshipPredicates
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
    varExpandRel: LogicalVariable,
    source: LogicalPlan
  ): LogicalPlan = {
    def excluded(groupRelationship: LogicalVariable, previouslyBoundedRel: LogicalVariable): Expression =
      NoneOfRelationships(previouslyBoundedRel, groupRelationship)(InputPosition.NONE)

    if (trail.previouslyBoundRelationships.nonEmpty) {
      val predicates: Set[Expression] = trail.previouslyBoundRelationships
        .map(boundRel => excluded(varExpandRel, boundRel))
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
    varExpandRel: LogicalVariable,
    source: LogicalPlan
  ): LogicalPlan =
    if (trail.previouslyBoundRelationshipGroups.nonEmpty) {
      val predicates: Set[Expression] = trail.previouslyBoundRelationshipGroups
        .map(boundRel => Disjoint(varExpandRel, boundRel)(InputPosition.NONE))
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

    object Maybe {

      /**
       * Unapplies Set[VariableGrouping] if the set has none or one entry.
       *
       * @return Some(None)        if variableGroupings.size == 0
       * @return Some(Some(head))  if variableGroupings.size == 1
       * @return None              if none of the above
       */
      def unapply(variableGroupings: Set[VariableGrouping]): Option[Option[VariableGrouping]] = {
        Option.when(variableGroupings.size <= 1)(variableGroupings.headOption)
      }
    }

    object Empty {

      def unapply(variableGroupings: Set[VariableGrouping]): Boolean = variableGroupings.isEmpty
    }
  }

  object RewritableTrailRhs {

    /**
     * This extractor ensures it will never allow a non-rewritable case to be rewritten. The opposite is not
     * true. This extractor will sometimes consider rewritable cases non-rewritable. We tolerate false
     * negatives. This is a tradeoff between code complexity and performance, where we tolerate missing out on a few
     * rare cases if it makes the code significantly more maintainable.
     *
     * This extractor relies on several properties of our compilation pipeline, which are not obvious at first.
     *
     * The first property we rely on has to do with the shape of the RHS of Trail. We assume that very few rewritable
     * cases that survive planning will deviate from the following shape. As a reminder, we require all rewritable
     * QPPs to have a single relationship chain with a single relationship.
     *
     * .trail(...)
     * .|.filter(..., isRepeatTrailUnique(r))
     * .|.expandAll(...)
     * .|.argument(...)
     * .lhs
     *
     * The second property we rely on has to do with the binding order of variables. QPP pre-filter predicates can
     * contain references to variables of the same MATCH clause, as can VarExpand. During LogicalPlanning we are careful
     * to order plans based on their dependencies on unbound variables. The variables that the Trail receives are
     * therefor guaranteed to be solved. Because this rewriter just swaps a Trail for a VarExpand without changing its
     * position in the overarching LogicalPlan, we do not need to worry about binding orders in our rewriter.
     */
    def unapply(trailRhs: LogicalPlan): Option[(Expand, Seq[InlinablePredicate])] = {
      def inlinablePredicate(
        predicate: Expression,
        expand: Expand
      ): Option[InlinablePredicate] = {
        if (isAPredicateOnRelationshipOnly(predicate, expand)) {
          Some(InlinableRelationshipPredicate(predicate, expand.relName))
        } else if (isAPredicateOnInnerNodesOnly(predicate, expand)) {
          expand.dir match {
            case OUTGOING => Some(NodeToRelationshipRewritablePredicates(
                predicate = predicate,
                startNode = expand.from,
                endNode = expand.to,
                relationship = expand.relName
              ))
            case INCOMING =>
              Some(NodeToRelationshipRewritablePredicates(
                predicate = predicate,
                startNode = expand.to,
                endNode = expand.from,
                relationship = expand.relName
              ))
            case BOTH =>
              None
          }
        } else {
          None
        }
      }

      def isAPredicateOnRelationshipOnly(predicate: Expression, expand: Expand): Boolean =
        predicate.dependencies.contains(expand.relName) && !predicate.dependencies.contains(
          expand.from
        ) && !predicate.dependencies.contains(expand.to)

      def isAPredicateOnInnerNodesOnly(predicate: Expression, expand: Expand): Boolean =
        predicate.dependencies.subsetOf(Set(expand.from, expand.to))

      trailRhs match {
        case Selection(Ands(predicates), expand @ Expand(_: Argument, _, _, _, _, _, ExpandAll)) =>
          predicates.view
            .filterNot(_.isInstanceOf[IsRepeatTrailUnique])
            .traverse(inlinablePredicate(_, expand))
            .map(inlinablePredicates => (expand, inlinablePredicates.toSeq))
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

  sealed trait InlinablePredicate

  final case class InlinableRelationshipPredicate(predicate: Expression, relationship: LogicalVariable)
      extends InlinablePredicate

  final case class NodeToRelationshipRewritablePredicates(
    predicate: Expression,
    startNode: LogicalVariable,
    endNode: LogicalVariable,
    relationship: LogicalVariable
  ) extends InlinablePredicate
}
