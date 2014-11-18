/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Multiplier, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, Selections, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics._
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, functions}

trait Expression2Selectivity {
  def apply(exp: Expression)(implicit semanticTable: SemanticTable, selections: Selections): Selectivity
}

case class ExpressionSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) extends Expression2Selectivity  {
  def apply(exp: Expression)(implicit semanticTable: SemanticTable, selections: Selections): Selectivity = exp match {
    // WHERE a:Label
    case HasLabels(_, label :: Nil) =>
      calculateSelectivityForLabel(label.id)

    // WHERE false
    case False() =>
      Selectivity(0)

    // WHERE x.prop IN [...]
    case In(Property(Identifier(name), propertyKey), Collection(expressions)) =>
      calculateSelectivityForPropertyEquality(name, expressions, selections, propertyKey)

    // WHERE NOT [...]
    case Not(inner) =>
      apply(inner).negate

    case Ors(expressions) =>
      val selectivities = expressions.toSeq.map(apply)
      combiner.orTogetherSelectivities(selectivities).get // We can trust the AST to never have empty ORs

    // WHERE id(x) = {param}
    case In(func@FunctionInvocation(_, _, IndexedSeq(_)), Parameter(_))
      if func.function == Some(functions.Id) =>
      DEFAULT_NUMBER_OF_ID_LOOKUPS / stats.nodesWithLabelCardinality(None)

    // WHERE id(x) IN [...]
    case In(func@FunctionInvocation(_, _, IndexedSeq(_)), c: Collection)
      if func.function == Some(functions.Id) =>
      c.expressions.size / stats.nodesWithLabelCardinality(None)

    // Implicit relation uniqueness predicates
    case NotEquals(lhs: Identifier, rhs: Identifier)
      if semanticTable.isRelationship(lhs) && semanticTable.isRelationship(rhs) =>
        GraphStatistics.DEFAULT_REL_UNIQUENESS_SELECTIVITY // This should not be the default. Instead, we should figure
                                                           // out the number of matching relationships and use it

    // WHERE <expr> = <expr>
    case _: Equals =>
      GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY

    // WHERE <expr> >= <expr>
    case _: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual =>
      GraphStatistics.DEFAULT_RANGE_SELECTIVITY

    case _ =>
      GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY
  }

  private def calculateSelectivityForLabel(label: Option[LabelId]): Selectivity = {
    val nodeCardinality: Cardinality = stats.nodesWithLabelCardinality(None)
    if (nodeCardinality == Cardinality.EMPTY) {
      return 1.0
    }

    val labelCardinality: Cardinality = label.map(l => stats.nodesWithLabelCardinality(Some(l))).getOrElse(0.0)
    labelCardinality / nodeCardinality
  }

  private def calculateSelectivityForPropertyEquality(identifier: String, expressions: Seq[Expression], selections: Selections, propertyKey: PropertyKeyName)
                                                     (implicit semanticTable: SemanticTable): Selectivity = {
    val labels = selections.labelsOnNode(IdName(identifier))
    val indexSelectivities: Seq[Selectivity] = labels.toSeq.flatMap {
      labelName =>
        (labelName.id, propertyKey.id) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            stats.indexSelectivity(labelId, propertyKeyId)

          case _ =>
            Some(Selectivity(0))
        }
    }

    val expandedSelectivities = Stream.from(0).take(expressions.size).flatMap(_ => indexSelectivities)

    val selectivity: Option[Selectivity] = combiner.orTogetherSelectivities(expandedSelectivities)

    selectivity.
      getOrElse(DEFAULT_EQUALITY_SELECTIVITY * Multiplier(expressions.size)) // If no index exist, use default equality selectivity
  }
}
