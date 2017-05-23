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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.cardinality

import java.math
import java.math.RoundingMode

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v3_3.spi.GraphStatistics._
import org.neo4j.cypher.internal.compiler.v3_3.PrefixRange
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality, IdName, Selections, Selectivity}

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

    // WHERE x.prop STARTS WITH 'prefix'
    case AsStringRangeSeekable(seekable@PrefixRangeSeekable(PrefixRange(StringLiteral(prefix)), _, _, _)) =>
      calculateSelectivityForSubstringSargable(seekable.name, selections, seekable.propertyKey, Some(prefix))

    // WHERE x.prop STARTS WITH expression
    case AsStringRangeSeekable(seekable@PrefixRangeSeekable(_:PrefixRange[_], _, _, _)) =>
      calculateSelectivityForSubstringSargable(seekable.name, selections, seekable.propertyKey, None)

    // WHERE x.prop CONTAINS 'substring'
    case Contains(Property(Variable(name), propertyKey), StringLiteral(substring)) =>
      calculateSelectivityForSubstringSargable(name, selections, propertyKey, Some(substring))

    // WHERE x.prop CONTAINS expression
    case Contains(Property(Variable(name), propertyKey), expr) =>
      calculateSelectivityForSubstringSargable(name, selections, propertyKey, None)

    // WHERE x.prop ENDS WITH 'substring'
    case EndsWith(Property(Variable(name), propertyKey), StringLiteral(substring)) =>
      calculateSelectivityForSubstringSargable(name, selections, propertyKey, Some(substring))

    // WHERE x.prop ENDS WITH expression
    case EndsWith(Property(Variable(name), propertyKey), expr) =>
      calculateSelectivityForSubstringSargable(name, selections, propertyKey, None)

    // WHERE x.prop <, <=, >=, > that could benefit from an index
    case AsValueRangeSeekable(seekable@InequalityRangeSeekable(_, _, _)) =>
      calculateSelectivityForValueRangeSeekable(seekable, selections)

    // WHERE has(x.prop)
    case AsPropertyScannable(scannable) =>
      calculateSelectivityForPropertyExistence(scannable.name, selections, scannable.propertyKey)

    // Implicit relation uniqueness predicates
    case Not(Equals(lhs: Variable, rhs: Variable))
      if areRelationships(semanticTable, lhs, rhs) =>
      GraphStatistics.DEFAULT_REL_UNIQUENESS_SELECTIVITY // This should not be the default. Instead, we should figure

    // WHERE NOT [...]
    case Not(inner) =>
      apply(inner).negate

    case Ors(expressions) =>
      val selectivities = expressions.toIndexedSeq.map(apply)
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

  def areRelationships(semanticTable: SemanticTable, lhs: Variable, rhs: Variable): Boolean = {
    val l = semanticTable.isRelationship(lhs)
    val r = semanticTable.isRelationship(rhs)
    l && r
  }

  private def calculateSelectivityForLabel(label: Option[LabelId]): Selectivity = {
    val labelCardinality = label.map(l => stats.nodesWithLabelCardinality(Some(l))).getOrElse(Cardinality.SINGLE)
    labelCardinality / stats.nodesWithLabelCardinality(None) getOrElse Selectivity.ONE
  }

  private def calculateSelectivityForPropertyEquality(variable: String,
                                                      sizeHint: Option[Int],
                                                      selections: Selections,
                                                      propertyKey: PropertyKeyName)
                                                     (implicit semanticTable: SemanticTable): Selectivity = {
    val labels = selections.labelsOnNode(IdName(variable))
    val indexSelectivities = labels.toIndexedSeq.flatMap {
      labelName =>
        (labelName.id, propertyKey.id) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            val descriptor = IndexDescriptor(labelId, propertyKeyId)
            val selectivities: Option[Selectivity] = stats.indexSelectivity(descriptor)
            selectivities

          case _ =>
            Some(Selectivity.ZERO)
        }
    }

    val itemSelectivity = combiner.orTogetherSelectivities(indexSelectivities).getOrElse(DEFAULT_EQUALITY_SELECTIVITY)
    val size = sizeHint.getOrElse(DEFAULT_NUMBER_OF_INDEX_LOOKUPS.amount.toInt)
    val selectivity = combiner.orTogetherSelectivities(1.to(size).map(_ => itemSelectivity)).getOrElse(DEFAULT_EQUALITY_SELECTIVITY)

    selectivity
  }

  private def calculateSelectivityForSubstringSargable(variable: String,
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
    val equality = math.BigDecimal.valueOf(calculateSelectivityForPropertyEquality(variable, None, selections, propertyKey).factor)
    val prefixLength = math.BigDecimal.valueOf(prefix match {
      case Some(n) => n.length + 1
      case None => DEFAULT_PREFIX_LENGTH
    })
    val factor = math.BigDecimal.ONE.divide(prefixLength, 17, RoundingMode.HALF_UP)
      .multiply(math.BigDecimal.valueOf(DEFAULT_RANGE_SEEK_FACTOR)).stripTrailingZeros()
    val slack = BigDecimalCombiner.negate(equality).multiply(factor)
    val result = Selectivity.of(equality.add(slack).doubleValue()).get

    //we know for sure we are no worse than a propertyExistence check
    val existence = calculateSelectivityForPropertyExistence(variable, selections, propertyKey)
    if (existence < result) existence else result
  }

  private def calculateSelectivityForValueRangeSeekable(seekable: InequalityRangeSeekable,
                                                        selections: Selections)
                                                       (implicit semanticTable: SemanticTable): Selectivity = {
    val name = seekable.ident.name
    val propertyKeyName = seekable.expr.property.propertyKey
    val equalitySelectivity = calculateSelectivityForPropertyEquality(name, Some(1), selections, propertyKeyName).factor

    val equality = math.BigDecimal.valueOf(equalitySelectivity)
    val factor = math.BigDecimal.valueOf(DEFAULT_RANGE_SEEK_FACTOR)
    val negatedEquality = BigDecimalCombiner.negate(equality)

    val base = if (seekable.hasEquality) equality else math.BigDecimal.valueOf(0)
    val selectivity = base.add(BigDecimalCombiner.andTogetherBigDecimals(
      Seq(math.BigDecimal.valueOf(seekable.expr.inequalities.size), factor, negatedEquality)
    ).get)
    val result = Selectivity.of(selectivity.doubleValue()).getOrElse(Selectivity.ONE)
    result
  }

  private def calculateSelectivityForPropertyExistence(variable: String,
                                                      selections: Selections,
                                                      propertyKey: PropertyKeyName)
                                                     (implicit semanticTable: SemanticTable): Selectivity = {
    val labels = selections.labelsOnNode(IdName(variable))
    val indexPropertyExistsSelectivities = labels.toIndexedSeq.flatMap {
      labelName =>
        (labelName.id, propertyKey.id) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            val descriptor = IndexDescriptor(labelId, propertyKeyId)
            val selectivity: Option[Selectivity] = stats.indexPropertyExistsSelectivity(descriptor)
            selectivity

          case _ =>
            Some(Selectivity.ZERO)
        }
    }

    val result = combiner.orTogetherSelectivities(indexPropertyExistsSelectivities).getOrElse(DEFAULT_PROPERTY_SELECTIVITY)
    result
  }
}
