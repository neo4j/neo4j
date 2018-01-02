/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality

import java.math
import java.math.RoundingMode

import org.neo4j.cypher.internal.compiler.v2_3.PrefixRange
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, _}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics._
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticTable, LabelId}

trait Expression2Selectivity {
  def apply(exp: Expression)(implicit semanticTable: SemanticTable, selections: Selections): Selectivity
}

case class ExpressionSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) extends Expression2Selectivity {

  def apply(exp: Expression)(implicit semanticTable: SemanticTable, selections: Selections): Selectivity = exp match {
    // WHERE a:Label
    case HasLabels(_, label :: Nil) =>
      calculateSelectivityForLabel(label.id)

    // WHERE false
    case False() =>
      Selectivity.ZERO

    // SubPredicate(sub, super)
    case partial: PartialPredicate[_] =>
      apply(partial.coveredPredicate)

    // WHERE x.prop =/IN ...
    case AsPropertySeekable(seekable) =>
      calculateSelectivityForPropertyEquality(seekable.name, seekable.args.sizeHint, selections, seekable.propertyKey)

    // WHERE x.prop STARTS WITH ...
    case AsStringRangeSeekable(seekable@PrefixRangeSeekable(PrefixRange(StringLiteral(prefix)), _, _, _)) =>
      calculateSelectivityForPrefixRangeSeekable(seekable.name, selections, seekable.propertyKey, Some(prefix))

    // WHERE x.prop STARTS WITH ...
    case AsStringRangeSeekable(seekable@PrefixRangeSeekable(_:PrefixRange[_], _, _, _)) =>
      calculateSelectivityForPrefixRangeSeekable(seekable.name, selections, seekable.propertyKey, None)

    // WHERE x.prop <, <=, >=, > that could benefit from an index
    case AsValueRangeSeekable(seekable@InequalityRangeSeekable(_, _, _)) =>
      calculateSelectivityForValueRangeSeekable(seekable, selections)

    // WHERE has(x.prop)
    case AsPropertyScannable(scannable) =>
      calculateSelectivityForPropertyExistence(scannable.name, selections, scannable.propertyKey)

    // Implicit relation uniqueness predicates
    case Not(Equals(lhs: Identifier, rhs: Identifier))
      if areRelationships(semanticTable, lhs, rhs) =>
      GraphStatistics.DEFAULT_REL_UNIQUENESS_SELECTIVITY // This should not be the default. Instead, we should figure

    // WHERE NOT [...]
    case Not(inner) =>
      apply(inner).negate

    case Ors(expressions) =>
      val selectivities = expressions.toSeq.map(apply)
      combiner.orTogetherSelectivities(selectivities).get // We can trust the AST to never have empty ORs

    // WHERE id(x) =/IN [...]
    case AsIdSeekable(seekable) =>
      (seekable.args.sizeHint.map(Cardinality(_)).getOrElse(DEFAULT_NUMBER_OF_ID_LOOKUPS) / stats.nodesWithLabelCardinality(None)) getOrElse Selectivity.ONE

    // WHERE <expr> = <expr>
    case _: Equals =>
      GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY

    // WHERE <expr> >= <expr>
    case _: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual =>
      GraphStatistics.DEFAULT_RANGE_SELECTIVITY

    case _ =>
      GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY
  }

  def areRelationships(semanticTable: SemanticTable, lhs: Identifier, rhs: Identifier): Boolean = {
    val l = semanticTable.isRelationship(lhs)
    val r = semanticTable.isRelationship(rhs)
    l && r
  }

  private def calculateSelectivityForLabel(label: Option[LabelId]): Selectivity = {
    val labelCardinality: Cardinality = label.map(l => stats.nodesWithLabelCardinality(Some(l))).getOrElse(Cardinality.EMPTY)
    labelCardinality / stats.nodesWithLabelCardinality(None) getOrElse Selectivity.ONE
  }

  private def calculateSelectivityForPropertyEquality(identifier: String,
                                                      sizeHint: Option[Int],
                                                      selections: Selections,
                                                      propertyKey: PropertyKeyName)
                                                     (implicit semanticTable: SemanticTable): Selectivity = {
    val labels = selections.labelsOnNode(IdName(identifier))
    val indexSelectivities = labels.toSeq.flatMap {
      labelName =>
        (labelName.id, propertyKey.id) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            stats.indexSelectivity(labelId, propertyKeyId)

          case _ =>
            Some(Selectivity.ZERO)
        }
    }

    val itemSelectivity = combiner.orTogetherSelectivities(indexSelectivities).getOrElse(DEFAULT_EQUALITY_SELECTIVITY)
    val size = sizeHint.getOrElse(DEFAULT_NUMBER_OF_INDEX_LOOKUPS.amount.toInt)
    val selectivity = combiner.orTogetherSelectivities(1.to(size).map(_ => itemSelectivity)).getOrElse(DEFAULT_EQUALITY_SELECTIVITY)

    selectivity
  }

  private def calculateSelectivityForPrefixRangeSeekable(identifier: String,
                                                   selections: Selections,
                                                   propertyKey: PropertyKeyName,
                                                   prefix: Option[String])
                                                  (implicit semanticTable: SemanticTable): Selectivity = {
    /*
     * c = DEFAULT_RANGE_SEEK_FACTOR
     * p = prefix length
     * f in (0,c) = (1 / p) * c
     * e in [0,1] = selectivity for equality comparison
     * x in [0,1] = selectivity for property existence
     * s in (0,1) = (1 - e) * f
     * return min(x, e + s in (0,1))
     */
    val equality = math.BigDecimal.valueOf(calculateSelectivityForPropertyEquality(identifier, None, selections, propertyKey).factor)
    val prefixLength = math.BigDecimal.valueOf(prefix match {
      case Some(n) => n.length + 1
      case None => DEFAULT_PREFIX_LENGTH
    })
    val factor = math.BigDecimal.ONE.divide(prefixLength, 17, RoundingMode.HALF_UP)
      .multiply(math.BigDecimal.valueOf(DEFAULT_RANGE_SEEK_FACTOR)).stripTrailingZeros()
    val slack = BigDecimalCombiner.negate(equality).multiply(factor)
    val result = Selectivity.of(equality.add(slack).doubleValue()).get

    //we know for sure we are no worse than a propertyExistence check
    val existence = calculateSelectivityForPropertyExistence(identifier, selections, propertyKey)
    if (existence < result) existence else result
  }

  private def calculateSelectivityForValueRangeSeekable(seekable: InequalityRangeSeekable,
                                                        selections: Selections)
                                                       (implicit semanticTable: SemanticTable): Selectivity = {
    val name = seekable.ident.name
    val propertyKeyName = seekable.expr.property.propertyKey
    val equality = math.BigDecimal.valueOf(calculateSelectivityForPropertyEquality(name, None, selections, propertyKeyName).factor)
    val factor = math.BigDecimal.valueOf(DEFAULT_RANGE_SEEK_FACTOR)
    val slack = BigDecimalCombiner.negate(equality).multiply(factor)
    val result = Selectivity.of(equality.add(slack).doubleValue()).getOrElse(Selectivity.ONE)
    result
  }

  private def calculateSelectivityForPropertyExistence(identifier: String,
                                                      selections: Selections,
                                                      propertyKey: PropertyKeyName)
                                                     (implicit semanticTable: SemanticTable): Selectivity = {
    val labels = selections.labelsOnNode(IdName(identifier))
    val indexPropertyExistsSelectivities = labels.toSeq.flatMap {
      labelName =>
        (labelName.id, propertyKey.id) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            stats.indexPropertyExistsSelectivity(labelId, propertyKeyId)

          case _ =>
            Some(Selectivity.ZERO)
        }
    }

    val result = combiner.orTogetherSelectivities(indexPropertyExistsSelectivities).getOrElse(DEFAULT_PROPERTY_SELECTIVITY)
    result
  }
}
