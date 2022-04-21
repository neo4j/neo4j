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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * For every source row, traverse all the relationships of 'from' which fulfill the
 * provided constraints. Produce one row per traversed relationships, and add the
 * relationship and end node as values on the produced rows.
 */
case class Expand(
  override val source: LogicalPlan,
  from: String,
  dir: SemanticDirection,
  types: Seq[RelTypeName],
  to: String,
  relName: String,
  mode: ExpansionMode = ExpandAll
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + relName + to
}

/**
 * This works exactly like Expand, but if no matching relationships are found, a single
 * row is produced instead populated by the argument, and the 'relName' and 'to' variables
 * are set to NO_VALUE.
 */
case class OptionalExpand(
  override val source: LogicalPlan,
  from: String,
  dir: SemanticDirection,
  types: Seq[RelTypeName],
  to: String,
  relName: String,
  mode: ExpansionMode = ExpandAll,
  predicate: Option[Expression] = None
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + relName + to
}

abstract class AbstractVarExpand(
  val from: String,
  val types: Seq[RelTypeName],
  val to: String,
  val nodePredicate: Option[VariablePredicate],
  val relationshipPredicate: Option[VariablePredicate],
  idGen: IdGen
) extends LogicalUnaryPlan(idGen) {

  def withNewPredicates(
    newNodePredicate: Option[VariablePredicate],
    newRelationshipPredicate: Option[VariablePredicate]
  )(idGen: IdGen): AbstractVarExpand
}

/**
 * For every source row, explore all homogeneous paths starting in 'from', that fulfill the provided
 * criteria. Paths are homogeneous in that all relationships have to fulfill the same relationship
 * predicate, and all nodes have to fulfill the same node predicate. For each explored
 * path that is longer or equal to length.min, and shorter than length.max, a row is produced.
 *
 * The relationships and end node of the corresponding path are added to the produced row.
 */
case class VarExpand(
  override val source: LogicalPlan,
  override val from: String,
  dir: SemanticDirection,
  projectedDir: SemanticDirection,
  override val types: Seq[RelTypeName],
  override val to: String,
  relName: String,
  length: VarPatternLength,
  mode: ExpansionMode = ExpandAll,
  override val nodePredicate: Option[VariablePredicate] = None,
  override val relationshipPredicate: Option[VariablePredicate] = None
)(implicit idGen: IdGen) extends AbstractVarExpand(from, types, to, nodePredicate, relationshipPredicate, idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + relName + to

  override def withNewPredicates(
    newNodePredicate: Option[VariablePredicate],
    newRelationshipPredicate: Option[VariablePredicate]
  )(idGen: IdGen): VarExpand =
    copy(nodePredicate = newNodePredicate, relationshipPredicate = newRelationshipPredicate)(idGen)

}

/**
 * In essence a VarExpand, where some paths are not explored if they could not produce an unseen
 * end node. Used to serve DISTINCT VarExpands where the individual paths are not of interest. This
 * operator does not guarantee unique end nodes, but it will produce less of them than the regular
 * VarExpand.
 *
 * Only the end node is added to produced rows.
 */
case class PruningVarExpand(
  override val source: LogicalPlan,
  override val from: String,
  dir: SemanticDirection,
  override val types: Seq[RelTypeName],
  override val to: String,
  minLength: Int,
  maxLength: Int,
  override val nodePredicate: Option[VariablePredicate] = None,
  override val relationshipPredicate: Option[VariablePredicate] = None
)(implicit idGen: IdGen)
    extends AbstractVarExpand(from, types, to, nodePredicate, relationshipPredicate, idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + to

  override def withNewPredicates(
    newNodePredicate: Option[VariablePredicate],
    newRelationshipPredicate: Option[VariablePredicate]
  )(idGen: IdGen): PruningVarExpand =
    copy(nodePredicate = newNodePredicate, relationshipPredicate = newRelationshipPredicate)(idGen)
}

/**
 * In essence a VarExpand, where some paths are not explored if they could not produce an unseen
 * end node. Used to serve DISTINCT VarExpands where the individual paths are not of interest. This
 * operator does guarantee unique end nodes for a given input, and it will produce less of them than the regular
 * VarExpand.
 *
 * Only the end node is added to produced rows.
 */
case class BFSPruningVarExpand(
  override val source: LogicalPlan,
  override val from: String,
  dir: SemanticDirection,
  override val types: Seq[RelTypeName],
  override val to: String,
  includeStartNode: Boolean,
  maxLength: Int,
  override val nodePredicate: Option[VariablePredicate] = None,
  override val relationshipPredicate: Option[VariablePredicate] = None
)(implicit idGen: IdGen)
    extends AbstractVarExpand(from, types, to, nodePredicate, relationshipPredicate, idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + to

  override def withNewPredicates(
    newNodePredicate: Option[VariablePredicate],
    newRelationshipPredicate: Option[VariablePredicate]
  )(idGen: IdGen): BFSPruningVarExpand =
    copy(nodePredicate = newNodePredicate, relationshipPredicate = newRelationshipPredicate)(idGen)

}

sealed trait ExpansionMode

/**
 * Expand relationships (a)-[r]-(b) for a given a, and populate r and b
 */
case object ExpandAll extends ExpansionMode

/**
 * Expand relationships (a)-[r]-(b) for a given a and b, and populate r
 */
case object ExpandInto extends ExpansionMode

case class VariablePredicate(variable: LogicalVariable, predicate: Expression)
