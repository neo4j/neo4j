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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RichLabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.QueryGraphPredicates.DisjunctiveHasLabels
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.QueryGraphPredicates.PredicatesWithDisjunctiveLabelInfos
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.VariableList
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

/**
 * Predicates of a query graph partitioned in order to calculate its cardinality.
 *
 * @param localLabelInfo      node labels explicitly defined in HasLabels predicates inside of the query graph.
 * @param localOnlyLabelInfo  node labels that are in [[QueryGraphPredicates.localLabelInfo]] but are not previously known nodes labels (in previousLabelInfo, passed as an argument to [[QueryGraphPredicates.partitionSelections]])
 *                            localLabelInfo \ previousLabelInfo
 * @param allLabelInfo        previously known nodes labels, passed as an argument to [[QueryGraphPredicates.partitionSelections]], merged with [[localLabelInfo]].
 *                            localLabelInfo U previousLabelInfo
 * @param previousLabelInfo   previously known nodes labels
 * @param uniqueRelationships relationships with Unique predicates as introduced by AddUniquenessPredicates.
 * @param otherPredicates     kitchen sink, all the predicates that weren't picked up in the other parameters.
 */
case class QueryGraphPredicates(
  localLabelInfo: LabelInfo,
  localOnlyLabelInfo: LabelInfo,
  allLabelInfo: LabelInfo,
  previousLabelInfo: LabelInfo,
  uniqueRelationships: Set[LogicalVariable],
  otherPredicates: Set[Predicate]
) {

  /**
   * We obtain the labelInfo (meaning that we know that a node has a label) from the top-level hasLabel predicates.
   * That is, if we have a:A|B, we do not include that in the label info at all.
   *
   * This value allows to iterate through all the different (disjunctive/ored) label infos that arise from looking at one option at a time.
   */
  lazy val distributeLabelDisjunctionAsLabelInfo: PredicatesWithDisjunctiveLabelInfos = {

    case class DisjunctiveHasLabelPredicate(predicate: Predicate, labelInfos: Set[(LogicalVariable, LabelName)])

    val disjunctiveHasLabelPredicates =
      otherPredicates.collect {
        // if this is dependent on only one variable, then all hasLabels refer to the same variable
        case pred @ Predicate(SetExtractor(_), DisjunctiveHasLabels(labelInfos)) =>
          DisjunctiveHasLabelPredicate(pred, labelInfos)
      }

    val thisWithoutDisjunctiveHasLabels =
      copy(otherPredicates = otherPredicates -- disjunctiveHasLabelPredicates.map(_.predicate))

    val distributedLabelInfo =
      disjunctiveHasLabelPredicates
        .map(_.labelInfos)
        .foldLeft(Set(Set.empty[(LogicalVariable, LabelName)])) {
          case (acc, right) =>
            for (l <- acc; r <- right) yield l + r
        }
    val distributedPredicatesWithLabelInfo =
      distributedLabelInfo.map { labelInfo =>
        thisWithoutDisjunctiveHasLabels.copy(
          localLabelInfo = thisWithoutDisjunctiveHasLabels.localLabelInfo.extend(labelInfo),
          localOnlyLabelInfo = thisWithoutDisjunctiveHasLabels.localOnlyLabelInfo.extend(labelInfo),
          allLabelInfo = thisWithoutDisjunctiveHasLabels.allLabelInfo.extend(labelInfo)
        )
      }

    PredicatesWithDisjunctiveLabelInfos(thisWithoutDisjunctiveHasLabels, distributedPredicatesWithLabelInfo.toSeq)
  }
}

object QueryGraphPredicates {

  /**
   * @param basePredicates base predicates from which the disjunction of labels was removed
   * @param predicatesWithLabelDisjunctionsDistributed predicates that each have additional label info from distributing label info
   */
  case class PredicatesWithDisjunctiveLabelInfos(
    basePredicates: QueryGraphPredicates,
    predicatesWithLabelDisjunctionsDistributed: Seq[QueryGraphPredicates]
  )

  /**
   * Matches on the label names in `Ors(ListSet(HasLabels(...), ..., HasLabels(...)))`.
   */
  object DisjunctiveHasLabels {

    def unapply(arg: Ors): Option[Set[(LogicalVariable, LabelName)]] =
      arg.exprs.foldLeft(Option(Set.empty[(LogicalVariable, LabelName)])) {
        case (Some(previous), HasLabels(variable: LogicalVariable, Seq(labelName))) =>
          Some(previous + (variable -> labelName))
        case _ => None
      }
  }

  def partitionSelections(
    previousLabelInfo: LabelInfo,
    localLabelInfo: LabelInfo,
    selections: Selections
  ): QueryGraphPredicates = {
    val (uniqueRelationships, otherPredicates) =
      selections.predicates.foldLeft((Set.empty[LogicalVariable], Set.empty[Predicate])) {
        case ((uniqueRelationships, otherPredicates), Predicate(_, HasLabels(_: Variable, _))) =>
          (uniqueRelationships, otherPredicates)
        case ((uniqueRelationships, otherPredicates), Predicate(_, Unique(VariableList(relationships)))) =>
          (uniqueRelationships ++ relationships, otherPredicates)
        case ((uniqueRelationships, otherPredicates), otherPred) => (uniqueRelationships, otherPredicates + otherPred)
      }

    val localOnlyLabelInfo = localLabelInfo.fuseLeft(previousLabelInfo)(_ -- _)

    QueryGraphPredicates(
      localLabelInfo = localLabelInfo,
      localOnlyLabelInfo = localOnlyLabelInfo,
      allLabelInfo = localLabelInfo.fuse(previousLabelInfo)(_ ++ _),
      previousLabelInfo = previousLabelInfo,
      uniqueRelationships = uniqueRelationships,
      otherPredicates = otherPredicates
    )
  }

  val empty: QueryGraphPredicates =
    QueryGraphPredicates(LabelInfo.empty, LabelInfo.empty, LabelInfo.empty, LabelInfo.empty, Set.empty, Set.empty)
}
