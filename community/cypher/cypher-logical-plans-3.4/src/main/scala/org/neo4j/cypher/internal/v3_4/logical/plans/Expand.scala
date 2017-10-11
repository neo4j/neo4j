/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, IdName, PlannerQuery, VarPatternLength}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, RelTypeName, SemanticDirection, Variable}

/**
  * For every source row, traverse all the relationships of 'from' which fulfill the
  * provided constraints. Produce one row per traversed relationships, and add the
  * relationship and end node as values on the produced rows.
  */
case class Expand(
                   source: LogicalPlan,
                   from: IdName,
                   dir: SemanticDirection,
                   types: Seq[RelTypeName],
                   to: IdName,
                   relName: IdName,
                   mode: ExpansionMode = ExpandAll
                 )(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(source)
  override def rhs = None
  override def availableSymbols: Set[IdName] = source.availableSymbols + relName + to
}

/**
  * This works exactly like Expand, but if no matching relationships are found, a single
  * row is produced instead populated by the argument.
  */
case class OptionalExpand(
                           source: LogicalPlan,
                           from: IdName,
                           dir: SemanticDirection,
                           types: Seq[RelTypeName],
                           to: IdName,
                           relName: IdName,
                           mode: ExpansionMode = ExpandAll,
                           predicates: Seq[Expression] = Seq.empty
                         )(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(source)
  override def rhs = None
  override def availableSymbols: Set[IdName] = source.availableSymbols + relName + to
}

/**
  * For every source row, explore all homogeneous paths starting in 'from', that fulfill the provided
  * criteria. Paths are homogeneous in that all relationships have to fulfill the same relationship
  * predicate, and all nodes except 'from' have to fulfill the same node predicate. For each explored
  * path that is longer or equal to length.min, and shorter than length.max, a row is produced.
  *
  * The relationships and end node of the corresponding path are added to the produced row.
  */
case class VarExpand(source: LogicalPlan,
                     from: IdName,
                     dir: SemanticDirection,
                     projectedDir: SemanticDirection,
                     types: Seq[RelTypeName],
                     to: IdName,
                     relName: IdName,
                     length: VarPatternLength,
                     mode: ExpansionMode = ExpandAll,
                     tempNode: IdName,
                     tempEdge: IdName,
                     nodePredicate: Expression,
                     edgePredicate: Expression,
                     legacyPredicates: Seq[(Variable, Expression)])
                    (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {
  override val lhs = Some(source)
  override def rhs = None

  override def availableSymbols: Set[IdName] = source.availableSymbols + relName + to
}

/**
  * In essence a VarExpand, where some paths are not explored if they could not produce an unseen
  * end node. Used to serve DISTINCT VarExpands where the individual paths are not of interest. This
  * operator does not guarantee unique end nodes, but it will produce less of them than the regular
  * VarExpand.
  *
  * Only the end node is added to produced rows.
  */
case class PruningVarExpand(source: LogicalPlan,
                            from: IdName,
                            dir: SemanticDirection,
                            types: Seq[RelTypeName],
                            to: IdName,
                            minLength: Int,
                            maxLength: Int,
                            predicates: Seq[(Variable, Expression)] = Seq.empty)
                           (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(source)
  override def rhs = None

  override def availableSymbols: Set[IdName] = source.availableSymbols + to
}

/**
  * Another variant of VarExpand, where paths are not explored if they could not produce an unseen
  * end node. A more powerful version of PruningVarExpand, which keeps more state. Guaranteed to
  * produce unique end nodes.
  *
  * Only the end node is added to produced rows.
  */
case class FullPruningVarExpand(source: LogicalPlan,
                                from: IdName,
                                dir: SemanticDirection,
                                types: Seq[RelTypeName],
                                to: IdName,
                                minLength: Int,
                                maxLength: Int,
                                predicates: Seq[(Variable, Expression)] = Seq.empty)
                               (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(source)
  override def rhs = None

  override def availableSymbols: Set[IdName] = source.availableSymbols + to
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
