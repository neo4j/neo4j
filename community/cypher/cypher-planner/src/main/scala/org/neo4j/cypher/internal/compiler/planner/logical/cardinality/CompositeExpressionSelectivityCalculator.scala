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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_RANGE_SEEK_FACTOR
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CompositeExpressionSelectivityCalculator.SelectivitiesForPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CompositeExpressionSelectivityCalculator.selectivityForCompositeIndexPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CompositeExpressionSelectivityCalculator.unwrapPartialPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsStringRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsValueRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PrefixRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.WhereClausePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexMatch
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.topDown

import scala.annotation.tailrec

/**
 * Calculates selectivity, taking composite indexes into account and using ExpressionSelectivityCalculator for everything else.
 *
 * An index can provide us with an existence selectivity on all properties as well as a uniqueness selectivity on all properties.
 *
 * That is, given an INDEX ON :Label(prop1, prop2), we can easily calculate the following selectivities:
 *
 *   selectivity(WHERE prop1 IS NOT NULL AND prop2 IS NOT NULL) = indexPropertyExistsSelectivity(index)
 *   selectivity(WHERE prop1 = 0 AND prop2 = 2)                 = indexPropertyExistsSelectivity(index) * uniqueValueSelectivity(index)
 *
 * In lack of more information, we assume that all properties contribute to the uniqueness selectivity equally and are independently distributed. Because in
 * that case the product of all uniqueness selectivities needs to be the uniqueness selectivity of the whole index, we conclude:
 *
 *   prod_X(uniqueValueSelectivity(propX)) = uniqueValueSelectivity(index)
 *   -> uniqueValueSelectivity(propX) = uniqueValueSelectivity(index) &#94; (1 / index.properties.size)
 *
 * @see #selectivityForCompositeIndexPredicates(SelectivitiesForPredicates, SelectivityCombiner)
 */
case class CompositeExpressionSelectivityCalculator(planContext: PlanContext) extends SelectivityCalculator {

  private val combiner: SelectivityCombiner = IndependenceCombiner

  private val singleExpressionSelectivityCalculator: ExpressionSelectivityCalculator = ExpressionSelectivityCalculator(planContext.statistics, combiner)

  private val nodeIndexMatchCache = CachedFunction[QueryGraph, SemanticTable, IndexCompatiblePredicatesProviderContext, Set[IndexMatch]] {
    (a, b, c) => findNodeIndexMatches(a, b, c)
  }

  private val relationshipIndexMatchCache = CachedFunction[QueryGraph, SemanticTable, IndexCompatiblePredicatesProviderContext, Set[IndexMatch]] {
    (a, b, c) => findRelationshipIndexMatches(a, b, c)
  }

  private val hasCompositeIndexes = planContext.propertyIndexesGetAll().exists(_.properties.size > 1)

  override def apply(
                      selections: Selections,
                      labelInfo: LabelInfo,
                      relTypeInfo: RelTypeInfo,
                      semanticTable: SemanticTable,
                      indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
                    ): Selectivity = {

    // Shortcutting a possibly expensive calculation: If there is nothing to select, we have no change in selectivity.
    if (selections.isEmpty) return Selectivity.ONE

    if (!hasCompositeIndexes) {
      val simpleSelectivities = selections.flatPredicates.map(singleExpressionSelectivityCalculator(_, labelInfo, relTypeInfo)(semanticTable))
      return combiner.andTogetherSelectivities(simpleSelectivities).getOrElse(Selectivity.ONE)
    }

    // The selections we get for cardinality estimation might contain partial predicates.
    // These are not recognized by the Leaf planners, so let's unwrap them.
    val unwrappedSelections = selections.endoRewrite(unwrapPartialPredicates)

    val queryGraphs = getQueryGraphs(labelInfo, relTypeInfo, unwrappedSelections)

    // we search for index matches for each variable individually to increase the chance of cache hits
    val indexMatches = queryGraphs.relQgs.flatMap(relationshipIndexMatchCache(_, semanticTable, indexPredicateProviderContext)) ++
      queryGraphs.nodeQgs.flatMap(nodeIndexMatchCache(_, semanticTable, indexPredicateProviderContext))

    val selectivitiesForPredicates = indexMatches
      .groupBy(_.indexDescriptor)
      .values
      .flatMap { indexMatches =>
        // If we have multiple index matches for the same index, let's pick the match solving the most predicates.
        val indexMatch = indexMatches.maxBy(_.propertyPredicates.flatMap(_.solvedPredicate).size)
        val predicates = indexMatch.propertyPredicates.flatMap(_.solvedPredicate)
        val maybeExistsSelectivity = planContext.statistics.indexPropertyIsNotNullSelectivity(indexMatch.indexDescriptor)
        val maybeUniqueSelectivity = planContext.statistics.uniqueValueSelectivity(indexMatch.indexDescriptor)
        (maybeExistsSelectivity, maybeUniqueSelectivity) match {
          case (Some(existsSelectivity), Some(uniqueSelectivity)) =>
            Some(SelectivitiesForPredicates(
              predicates.toSet,
              existsSelectivity,
              uniqueSelectivity,
              indexMatch.indexDescriptor.properties.size,
            ))
          case _ => None
        }
      }.toSet

    // Keep only index matches that have no overlaps - otherwise the math gets very complicated.
    val disjointPredicatesWithSelectivities = selectivitiesForPredicates.filter {
      case s1@SelectivitiesForPredicates(predicates1, _, _, _) => selectivitiesForPredicates.forall {
        case s2@SelectivitiesForPredicates(predicates2, _, _, _) => s1 == s2 || predicates1.intersect(predicates2).isEmpty
      }
    }

    // For performance, keep only predicates of composite indexes
    val compositeDisjointPredicatesWithSelectivities = disjointPredicatesWithSelectivities.filter {
      case SelectivitiesForPredicates(_, _, _, numberOfIndexedProperties) => numberOfIndexedProperties > 1
    }

    val coveredPredicates = compositeDisjointPredicatesWithSelectivities.flatMap(_.solvedPredicates)
    val notCoveredPredicates = unwrappedSelections.flatPredicates.filter(!coveredPredicates.contains(_))

    // Forward all not covered predicates to the singleExpressionSelectivityCalculator.
    val notCoveredPredicatesSelectivities = notCoveredPredicates.map(singleExpressionSelectivityCalculator(_, labelInfo, relTypeInfo)(semanticTable))
    // Use composite index selectivities for all covered predicates.
    val coveredPredicatesSelectivities = compositeDisjointPredicatesWithSelectivities.map(selectivityForCompositeIndexPredicates(_, combiner))

    combiner.andTogetherSelectivities(notCoveredPredicatesSelectivities ++ coveredPredicatesSelectivities).getOrElse(Selectivity.ONE)
  }

  private def findNodeIndexMatches(queryGraph: QueryGraph,
                                   semanticTable: SemanticTable,
                                   indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): Set[IndexMatch] = {
    NodeIndexLeafPlanner.findIndexMatchesForQueryGraph(queryGraph, semanticTable, planContext, indexPredicateProviderContext).toSet[IndexMatch]
  }

  private def findRelationshipIndexMatches(queryGraph: QueryGraph,
                                           semanticTable: SemanticTable,
                                           indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): Set[IndexMatch] = {
    RelationshipIndexLeafPlanner.findIndexMatchesForQueryGraph(queryGraph, semanticTable, planContext, indexPredicateProviderContext).toSet[IndexMatch]
  }

  private case class NodeRelQgs(nodeQgs: Iterable[QueryGraph], relQgs: Iterable[QueryGraph]) {
    def mapQgs(f: QueryGraph => QueryGraph): NodeRelQgs = NodeRelQgs(nodeQgs.map(f), relQgs.map(f))
  }

  private def getQueryGraphs(labelInfo: LabelInfo,
                             relTypeInfo: RelTypeInfo,
                             unwrappedSelections: Selections): NodeRelQgs = {
    val expressionsContainingVariable = unwrappedSelections.expressionsContainingVariable
    val variables = expressionsContainingVariable.keys
    val nodes = variables.filter(labelInfo.contains)
    val relationships = variables.filter(relTypeInfo.contains)

    def findSelectionsFor(variable: String): Selections = unwrappedSelections.filter(expressionsContainingVariable(variable))

    // Construct query graphs for each variable that can be fed to leaf planners to search for index matches.
    val nodeQgs = nodes.map { n =>
      QueryGraph(
        patternNodes = Set(n),
        selections = findSelectionsFor(n)
      )
    }
    val relQgs = relationships.map { r =>
      QueryGraph(
        patternRelationships = Set(PatternRelationship(r, ("  UNNAMED0", "  UNNAMED1"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = findSelectionsFor(r)
      )
    }

    NodeRelQgs(nodeQgs, relQgs).mapQgs(inlineLabelAndRelTypeInfo(_, labelInfo, relTypeInfo))
  }

  private def inlineLabelAndRelTypeInfo(qg: QueryGraph, labelInfo: LabelInfo, relTypeInfo: RelTypeInfo): QueryGraph = {
    val labelPredicates = labelInfo.collect {
      case (name, labels) if qg.patternNodes.contains(name) => WhereClausePredicate(HasLabels(Variable(name)(InputPosition.NONE), labels.toSeq)(InputPosition.NONE))
    }
    val relTypePredicates = relTypeInfo.collect {
      case (name, relType) if qg.patternRelationships.map(_.name).contains(name) => WhereClausePredicate(HasTypes(Variable(name)(InputPosition.NONE), Seq(relType))(InputPosition.NONE))
    }
    (labelPredicates ++ relTypePredicates).foldLeft(qg) {
      case (qg, predicate) => predicate.addToQueryGraph(qg)
    }
  }
}

object CompositeExpressionSelectivityCalculator {
  val unwrapPartialPredicates: Rewriter = topDown(Rewriter.lift {
    case partial: PartialPredicate[_] => partial.coveredPredicate
  })

  private[cardinality] def selectivityForCompositeIndexPredicates(selectivitiesForPredicates: SelectivitiesForPredicates,
                                                                  combiner: SelectivityCombiner): Selectivity = {
    // We assume that all properties contribute equally to the unique selectivity. That is, they are independent.
    val assumedUniqueSelectivityPerPredicate = selectivitiesForPredicates.uniqueSelectivity ^ (1.0 / selectivitiesForPredicates.numberOfIndexedProperties)

    val selectivitiesAssumingExistence = selectivitiesForPredicates.solvedPredicates.toSeq.map(getPredicateSelectivity(assumedUniqueSelectivityPerPredicate, combiner))

    selectivitiesForPredicates.existsSelectivity * combiner.andTogetherSelectivities(selectivitiesAssumingExistence).getOrElse(Selectivity.ONE)
  }

  @tailrec
  private def getPredicateSelectivity(
                                       assumedUniqueSelectivityPerPredicate: Selectivity,
                                       combiner: SelectivityCombiner,
                                     )
                                     (predicate: Expression): Selectivity = {
    predicate match {
      // SubPredicate(sub, super)
      case partial: PartialPredicate[_] =>
        getPredicateSelectivity(assumedUniqueSelectivityPerPredicate, combiner)(partial.coveredPredicate)

      // WHERE x.prop =/IN ...
      case AsPropertySeekable(seekable) =>
        val sizeHint = seekable.args.sizeHint
        ExpressionSelectivityCalculator.indexSelectivityWithSizeHint(
          sizeHint,
          size => combiner.orTogetherSelectivities(Seq.fill(size)(assumedUniqueSelectivityPerPredicate)).get
        )

      // WHERE x.prop STARTS WITH 'prefix'
      case AsStringRangeSeekable(PrefixRangeSeekable(PrefixRange(StringLiteral(prefix)), _, _, _)) =>
        ExpressionSelectivityCalculator.indexSelectivityForSubstringSargable(Some(prefix))

      // WHERE x.prop STARTS WITH expression
      case AsStringRangeSeekable(PrefixRangeSeekable(_: PrefixRange[_], _, _, _)) =>
        ExpressionSelectivityCalculator.indexSelectivityForSubstringSargable(None)

      // WHERE x.prop CONTAINS 'substring'
      case Contains(Property(Variable(_), _), StringLiteral(substring)) =>
        ExpressionSelectivityCalculator.indexSelectivityForSubstringSargable(Some(substring))

      // WHERE x.prop CONTAINS expression
      case Contains(Property(Variable(_), _), _) =>
        ExpressionSelectivityCalculator.indexSelectivityForSubstringSargable(None)

      // WHERE x.prop ENDS WITH 'substring'
      case EndsWith(Property(Variable(_), _), StringLiteral(substring)) =>
        ExpressionSelectivityCalculator.indexSelectivityForSubstringSargable(Some(substring))

      // WHERE x.prop ENDS WITH expression
      case EndsWith(Property(Variable(_), _), _) =>
        ExpressionSelectivityCalculator.indexSelectivityForSubstringSargable(None)

      // WHERE distance(p.prop, otherPoint) <, <= number that could benefit from an index
      case AsDistanceSeekable(_) =>
        Selectivity(DEFAULT_RANGE_SEEK_FACTOR)

      // WHERE x.prop <, <=, >=, > that could benefit from an index
      case AsValueRangeSeekable(seekable) =>
        ExpressionSelectivityCalculator.getPropertyPredicateRangeSelectivity(seekable, assumedUniqueSelectivityPerPredicate)

      // WHERE x.prop IS NOT NULL
      case AsPropertyScannable(_) =>
        Selectivity.ONE

      case _ =>
        DEFAULT_PREDICATE_SELECTIVITY
    }
  }

  /**
   * Predicates solved by one index.
   *
   * @param solvedPredicates          predicates in the query graph's selections which are solved by the given index
   * @param existsSelectivity         selectivity for existence of all properties
   * @param uniqueSelectivity         selectivity for uniqueness of all properties (given they exist)
   * @param numberOfIndexedProperties the number of properties in the index (may differ from solvedPredicates.size)
   */
  private[cardinality] case class SelectivitiesForPredicates(solvedPredicates: Set[Expression],
                                                             existsSelectivity: Selectivity,
                                                             uniqueSelectivity: Selectivity,
                                                             numberOfIndexedProperties: Int)
}
