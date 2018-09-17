/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.cardinality

import java.math
import java.math.RoundingMode

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans._
import org.neo4j.cypher.internal.ir.v3_5.Selections
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics._
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor
import org.neo4j.cypher.internal.v3_5.logical.plans.PrefixRange
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.Cardinality
import org.opencypher.v9_0.util.LabelId
import org.opencypher.v9_0.util.Selectivity

trait Expression2Selectivity {
  def apply(exp: Expression)(implicit semanticTable: SemanticTable, selections: Selections): Selectivity
}

case class ExpressionSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) extends Expression2Selectivity {

  def apply(exp: Expression)(implicit semanticTable: SemanticTable, selections: Selections): Selectivity = exp match {
    // WHERE a:Label
    case HasLabels(_, label :: Nil) =>
      calculateSelectivityForLabel(semanticTable.id(label))

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
    case AsValueRangeSeekable(seekable) =>
      calculateSelectivityForValueRangeSeekable(seekable, selections)

      // WHERE distance(p.prop, otherPoint) <, <= number that could benefit from an index
    case AsDistanceSeekable(seekable) =>
      calculateSelectivityForPointDistanceSeekable(seekable, selections)

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
      (seekable.args.sizeHint.map(Cardinality(_)).getOrElse(DEFAULT_NUMBER_OF_ID_LOOKUPS) / stats.nodesAllCardinality()) getOrElse Selectivity.ONE

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
    val labelCardinality =
    if(label.isEmpty){
      Cardinality.SINGLE
    } else {
      label.map(l => stats.nodesWithLabelCardinality(Some(l))).getOrElse(Cardinality.SINGLE)
    }
    labelCardinality / stats.nodesAllCardinality() getOrElse Selectivity.ONE
  }

  private def calculateSelectivityForPropertyExistence(variable: String,
                                                       selections: Selections,
                                                       propertyKey: PropertyKeyName)
                                                      (implicit semanticTable: SemanticTable): Selectivity = {
    val indexPropertyExistsSelectivities = indexPropertyExistsSelectivitiesFor(variable, selections, propertyKey)
    combiner.orTogetherSelectivities(indexPropertyExistsSelectivities).getOrElse(DEFAULT_PROPERTY_SELECTIVITY)
  }

  private def indexPropertyExistsSelectivitiesFor(variable: String,
                                                  selections: Selections,
                                                  propertyKey: PropertyKeyName)
                                                 (implicit semanticTable: SemanticTable): Seq[Selectivity] = {
    val labels: Set[LabelName] = selections.labelsOnNode(variable)
    labels.toIndexedSeq.flatMap {
      labelName =>
        (semanticTable.id(labelName), semanticTable.id(propertyKey)) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            val descriptor = IndexDescriptor(labelId, Seq(propertyKeyId))
            stats.indexPropertyExistsSelectivity(descriptor)

          case _ =>
            Some(Selectivity.ZERO)
        }
    }
  }

  private def calculateSelectivityForPropertyEquality(variable: String,
                                                      sizeHint: Option[Int],
                                                      selections: Selections,
                                                      propertyKey: PropertyKeyName)
                                                     (implicit semanticTable: SemanticTable): Selectivity = {
    val labels = selections.labelsOnNode(variable)
    val indexSelectivities = labels.toIndexedSeq.flatMap {
      labelName =>
        (semanticTable.id(labelName), semanticTable.id(propertyKey)) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            val descriptor = IndexDescriptor(labelId, Seq(propertyKeyId))
            for {
              propExists <-stats.indexPropertyExistsSelectivity(descriptor)
              propEqualsValue <- stats.uniqueValueSelectivity(descriptor)
              combinedSelectivity <- combiner.andTogetherSelectivities(Seq(propExists, propEqualsValue))
            } yield combinedSelectivity

          case _ =>
            Some(Selectivity.ZERO)
        }
    }

    val itemSelectivity = combiner.orTogetherSelectivities(indexSelectivities).getOrElse(DEFAULT_EQUALITY_SELECTIVITY)
    val size = sizeHint.getOrElse(DEFAULT_LIST_CARDINALITY.amount.toInt)
    if (size == 0) {
      Selectivity.ZERO
    } else {
      combiner.orTogetherSelectivities(1.to(size).map(_ => itemSelectivity)).getOrElse(DEFAULT_EQUALITY_SELECTIVITY)
    }
  }

  private def calculateSelectivityForValueRangeSeekable(seekable: InequalityRangeSeekable,
                                                        selections: Selections)
                                                       (implicit semanticTable: SemanticTable): Selectivity = {
    val defaultEq = DEFAULT_EQUALITY_SELECTIVITY
    val defaultRange = DEFAULT_RANGE_SELECTIVITY * Selectivity(1.0 / Math.min(seekable.expr.inequalities.size, 2))
    val default = if (seekable.hasEquality)
      // If the sum should ever (by changing the constants) be more than 1 we default to 1
      Selectivity.of(defaultEq.factor + defaultRange.factor).getOrElse(Selectivity.ONE)
    else
      defaultRange

    val labels: Set[LabelName] = selections.labelsOnNode(seekable.ident.name)
    val indexRangeSelectivities: Seq[Selectivity] = labels.toIndexedSeq.flatMap {
      labelName =>
        (semanticTable.id(labelName), semanticTable.id(seekable.expr.property.propertyKey)) match {
          case (Some(labelId), Some(propertyKeyId)) =>
            val descriptor = IndexDescriptor(labelId, Seq(propertyKeyId))

            for {
              propertyExistsSelectivity <- stats.indexPropertyExistsSelectivity(descriptor)
              propEqValueSelectivity <- stats.uniqueValueSelectivity(descriptor)
              val pExists = math.BigDecimal.valueOf(propertyExistsSelectivity.factor)
              val pEq = math.BigDecimal.valueOf(propEqValueSelectivity.factor)
              val pNeq = BigDecimalCombiner.negate(pEq)
              val pNeqRange = pNeq
                .multiply(math.BigDecimal.valueOf(DEFAULT_RANGE_SEEK_FACTOR))
                .divide(math.BigDecimal.valueOf(Math.min(seekable.expr.inequalities.size, 2)), 17, RoundingMode.HALF_UP)

              val pRange = if (seekable.hasEquality) pEq.add(pNeqRange) else pNeqRange
              val pRangeByLabel = pRange.multiply(pExists)
              indexSelectivity <-Selectivity.of(pRangeByLabel.doubleValue())
            } yield indexSelectivity

          case _ =>
            Some(Selectivity.ZERO)
        }
    }
    combiner.orTogetherSelectivities(indexRangeSelectivities).getOrElse(default)
  }

  private def calculateSelectivityForPointDistanceSeekable(seekable: PointDistanceSeekable,
                                                        selections: Selections)
                                                       (implicit semanticTable: SemanticTable): Selectivity = {
    val indexPropertyExistsSelectivities = indexPropertyExistsSelectivitiesFor(seekable.ident.name, selections, seekable.propertyKeyName)
    val indexDistanceSelectivities = indexPropertyExistsSelectivities.map(_ * Selectivity(DEFAULT_RANGE_SEEK_FACTOR))
    combiner.orTogetherSelectivities(indexDistanceSelectivities).getOrElse(DEFAULT_RANGE_SELECTIVITY)
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
}
