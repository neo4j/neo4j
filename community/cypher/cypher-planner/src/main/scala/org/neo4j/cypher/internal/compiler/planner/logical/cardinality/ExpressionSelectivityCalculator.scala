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
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_LIST_CARDINALITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_NUMBER_OF_ID_LOOKUPS
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_PROPERTY_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_RANGE_SEEK_FACTOR
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_RANGE_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_REL_UNIQUENESS_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_STRING_LENGTH
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_TYPE_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsStringRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsValueRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.InequalityRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PointDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PrefixRangeSeekable
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity

case class ExpressionSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) {

  def apply(exp: Expression, labelInfo: LabelInfo, relTypeInfo: RelTypeInfo)(implicit semanticTable: SemanticTable): Selectivity = exp match {
    // WHERE a:Label
    case HasLabels(_, label :: Nil) =>
      calculateSelectivityForLabel(semanticTable.id(label))

    // WHERE false
    case False() =>
      Selectivity.ZERO

    // SubPredicate(sub, super)
    case partial: PartialPredicate[_] =>
      apply(partial.coveredPredicate, labelInfo, relTypeInfo)

    // WHERE x.prop =/IN ...
    case AsPropertySeekable(seekable) =>
      calculateSelectivityForPropertyEquality(seekable.name, seekable.args.sizeHint, labelInfo, relTypeInfo, seekable.propertyKey)

    // WHERE x.prop STARTS WITH 'prefix'
    case AsStringRangeSeekable(seekable@PrefixRangeSeekable(PrefixRange(StringLiteral(prefix)), _, _, _)) =>
      calculateSelectivityForSubstringSargable(seekable.name, labelInfo, relTypeInfo, seekable.propertyKeyName, Some(prefix))

    // WHERE x.prop STARTS WITH expression
    case AsStringRangeSeekable(seekable@PrefixRangeSeekable(_:PrefixRange[_], _, _, _)) =>
      calculateSelectivityForSubstringSargable(seekable.name, labelInfo, relTypeInfo, seekable.propertyKeyName, None)

    // WHERE x.prop CONTAINS 'substring'
    case Contains(Property(Variable(name), propertyKey), StringLiteral(substring)) =>
      calculateSelectivityForSubstringSargable(name, labelInfo, relTypeInfo, propertyKey, Some(substring))

    // WHERE x.prop CONTAINS expression
    case Contains(Property(Variable(name), propertyKey), expr) =>
      calculateSelectivityForSubstringSargable(name, labelInfo, relTypeInfo, propertyKey, None)

    // WHERE x.prop ENDS WITH 'substring'
    case EndsWith(Property(Variable(name), propertyKey), StringLiteral(substring)) =>
      calculateSelectivityForSubstringSargable(name, labelInfo, relTypeInfo, propertyKey, Some(substring))

    // WHERE x.prop ENDS WITH expression
    case EndsWith(Property(Variable(name), propertyKey), expr) =>
      calculateSelectivityForSubstringSargable(name, labelInfo, relTypeInfo, propertyKey, None)

    // WHERE distance(p.prop, otherPoint) <, <= number that could benefit from an index
    case AsDistanceSeekable(seekable) =>
      calculateSelectivityForPointDistanceSeekable(seekable, labelInfo, relTypeInfo)

    // WHERE x.prop <, <=, >=, > that could benefit from an index
    case AsValueRangeSeekable(seekable) =>
      calculateSelectivityForValueRangeSeekable(seekable, labelInfo, relTypeInfo)

    // WHERE has(x.prop)
    case AsPropertyScannable(scannable) =>
      calculateSelectivityForPropertyExistence(scannable.name, labelInfo, relTypeInfo, scannable.propertyKey)

    // Implicit relation uniqueness predicates
    case Not(Equals(lhs: Variable, rhs: Variable))
      if areRelationships(semanticTable, lhs, rhs) =>
      DEFAULT_REL_UNIQUENESS_SELECTIVITY // This should not be the default. Instead, we should figure

    // WHERE NOT [...]
    case Not(inner) =>
      apply(inner, labelInfo, relTypeInfo).negate

    case Ors(expressions) =>
      val selectivities = expressions.toIndexedSeq.map(apply(_, labelInfo, relTypeInfo))
      combiner.orTogetherSelectivities(selectivities).get // We can trust the AST to never have empty ORs

    // WHERE id(x) =/IN [...]
    case AsIdSeekable(seekable) =>
      (seekable.args.sizeHint.map(Cardinality(_)).getOrElse(DEFAULT_NUMBER_OF_ID_LOOKUPS) / stats.nodesAllCardinality()) getOrElse Selectivity.ONE

    // WHERE <expr> = <expr>
    case _: Equals =>
      DEFAULT_EQUALITY_SELECTIVITY

    // WHERE <expr> >= <expr>
    case _: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual =>
      DEFAULT_RANGE_SELECTIVITY

    case _: AssertIsNode =>
      Selectivity.ONE

    case _ =>
      DEFAULT_PREDICATE_SELECTIVITY
  }

  private def areRelationships(semanticTable: SemanticTable, lhs: Variable, rhs: Variable): Boolean = {
    val l = semanticTable.isRelationship(lhs)
    val r = semanticTable.isRelationship(rhs)
    l && r
  }

  private def calculateSelectivityForLabel(label: Option[LabelId]): Selectivity = {
    val labelCardinality = stats.nodesWithLabelCardinality(label)
    labelCardinality / stats.nodesAllCardinality() getOrElse Selectivity.ONE
  }

  private def calculateSelectivityForPropertyExistence(variable: String,
                                                       labelInfo: LabelInfo,
                                                       relTypeInfo: RelTypeInfo,
                                                       propertyKey: PropertyKeyName)
                                                      (implicit semanticTable: SemanticTable): Selectivity = {
    val indexPropertyExistsSelectivities = indexPropertyExistsSelectivitiesFor(variable, labelInfo, relTypeInfo, propertyKey)
    combiner.orTogetherSelectivities(indexPropertyExistsSelectivities).getOrElse(DEFAULT_PROPERTY_SELECTIVITY)
  }

  private def indexPropertyExistsSelectivitiesFor(variable: String,
                                                  labelInfo: LabelInfo,
                                                  relTypeInfo: RelTypeInfo,
                                                  propertyKey: PropertyKeyName)
                                                 (implicit semanticTable: SemanticTable): Seq[Selectivity] = {
    val labels = labelInfo.getOrElse(variable, Set.empty)
    val relTypes = relTypeInfo.get(variable)

    (labels ++ relTypes).toIndexedSeq.flatMap { name =>
      val ids = name match {
        case labelName: LabelName => (semanticTable.id(labelName), semanticTable.id(propertyKey))
        case relTypeName: RelTypeName => (semanticTable.id(relTypeName), semanticTable.id(propertyKey))
      }

      ids match {
        case (Some(labelId: LabelId), Some(propertyKeyId)) =>
          val descriptor = IndexDescriptor.forLabel(labelId, Seq(propertyKeyId))
          stats.indexPropertyExistsSelectivity(descriptor)

        case (Some(relTypeId: RelTypeId), Some(propertyKeyId)) =>
          val descriptor = IndexDescriptor.forRelType(relTypeId, Seq(propertyKeyId))
          stats.indexPropertyExistsSelectivity(descriptor)

        case _ => Some(Selectivity.ZERO)
      }
    }
  }

  private def calculateSelectivityForPropertyEquality(variable: String,
                                                      sizeHint: Option[Int],
                                                      labelInfo: LabelInfo,
                                                      relTypeInfo: RelTypeInfo,
                                                      propertyKey: PropertyKeyName)
                                                     (implicit semanticTable: SemanticTable): Selectivity = {
    sizeHint.getOrElse(DEFAULT_LIST_CARDINALITY.amount.toInt) match {
      case 0    => Selectivity.ZERO
      case size =>
        val labels = labelInfo.getOrElse(variable, Set.empty)
        val relTypes = relTypeInfo.get(variable)
        val indexSelectivities = (labels ++ relTypes).toIndexedSeq.flatMap { name =>

          val descriptor: Option[IndexDescriptor] = (name, semanticTable.id(propertyKey)) match {
            case (labelName: LabelName, Some(propKeyId))     => semanticTable.id(labelName).map(id => IndexDescriptor.forLabel(id, Seq(propKeyId)))
            case (relTypeName: RelTypeName, Some(propKeyId)) => semanticTable.id(relTypeName).map(id => IndexDescriptor.forRelType(id, Seq(propKeyId)))
            case _ => None
          }

          descriptor.flatMap(indexSelectivityForPropertyEquality(_, size))
        }

        combiner.orTogetherSelectivities(indexSelectivities)
                .orElse(defaultSelectivityForPropertyEquality(size))
                .getOrElse(DEFAULT_PREDICATE_SELECTIVITY)
    }
  }

  private def indexSelectivityForPropertyEquality(descriptor: IndexDescriptor, size: Int): Option[Selectivity] =
    selectivityForPropertyEquality(stats.indexPropertyExistsSelectivity(descriptor), stats.uniqueValueSelectivity(descriptor), size)


  private def defaultSelectivityForPropertyEquality(size: Int): Option[Selectivity] =
    selectivityForPropertyEquality(Some(DEFAULT_PROPERTY_SELECTIVITY), Some(DEFAULT_EQUALITY_SELECTIVITY), size)

  private def selectivityForPropertyEquality(propertySelectivity: Option[Selectivity], uniqueValueSelectivity: Option[Selectivity], size: Int): Option[Selectivity] = for {
    propExists <- propertySelectivity
    propEqualsSingleValue <- uniqueValueSelectivity
    propEqualsAnyValue <- combiner.orTogetherSelectivities(Seq.fill(size)(propEqualsSingleValue))
    combinedSelectivity <- combiner.andTogetherSelectivities(Seq(propExists, propEqualsAnyValue))
  } yield combinedSelectivity

  private def calculateSelectivityForValueRangeSeekable(seekable: InequalityRangeSeekable,
                                                        labelInfo: LabelInfo,
                                                        relTypeInfo: RelTypeInfo)
                                                       (implicit semanticTable: SemanticTable): Selectivity = {
    def default = {
      val defaultRange = DEFAULT_RANGE_SELECTIVITY * Selectivity(1.0 / Math.min(seekable.expr.inequalities.size, 2))
      if (seekable.hasEquality) {
        // If the sum should ever (by changing the constants) be more than 1 we default to 1
        Selectivity.of(DEFAULT_EQUALITY_SELECTIVITY.factor + defaultRange.factor).getOrElse(Selectivity.ONE)
      } else {
        defaultRange
      }
    }

    val labels = labelInfo.getOrElse(seekable.ident.name, Set.empty)
    val relTypes = relTypeInfo.get(seekable.ident.name)
    val indexRangeSelectivities: Seq[Selectivity] = (labels ++ relTypes).toIndexedSeq.flatMap { name =>
      val ids = name match {
        case labelName: LabelName => (semanticTable.id(labelName), semanticTable.id(seekable.expr.property.propertyKey))
        case relTypeName: RelTypeName => (semanticTable.id(relTypeName), semanticTable.id(seekable.expr.property.propertyKey))
      }

      ids match {
        case (Some(labelOrRelTypeId), Some(propertyKeyId)) =>
          val descriptor = labelOrRelTypeId match {
            case labelId: LabelId => IndexDescriptor.forLabel(labelId, Seq(propertyKeyId))
            case relTypeId: RelTypeId => IndexDescriptor.forRelType(relTypeId, Seq(propertyKeyId))
          }

          for {
            propertyExistsSelectivity <- stats.indexPropertyExistsSelectivity(descriptor)
            propEqValueSelectivity <- stats.uniqueValueSelectivity(descriptor)
          } yield {
            val pNeq = propEqValueSelectivity.negate
            val pNeqRange = pNeq.factor * DEFAULT_RANGE_SEEK_FACTOR / Math.min(seekable.expr.inequalities.size, 2)

            val pRange = Selectivity(if (seekable.hasEquality) propEqValueSelectivity.factor + pNeqRange else pNeqRange)
            val pRangeBounded = Selectivity(math.max(propEqValueSelectivity.factor, pRange.factor))
            pRangeBounded * propertyExistsSelectivity
          }

        case _ => Some(Selectivity.ZERO)
      }
    }

    combiner.orTogetherSelectivities(indexRangeSelectivities).getOrElse(default)
  }

  private def calculateSelectivityForPointDistanceSeekable(seekable: PointDistanceSeekable,
                                                           labelInfo: LabelInfo,
                                                           relTypeInfo: RelTypeInfo)
                                                          (implicit semanticTable: SemanticTable): Selectivity = {
    val indexPropertyExistsSelectivities = indexPropertyExistsSelectivitiesFor(seekable.ident.name, labelInfo, relTypeInfo, seekable.propertyKeyName)
    val indexDistanceSelectivities = indexPropertyExistsSelectivities.map(_ * Selectivity(DEFAULT_RANGE_SEEK_FACTOR))
    combiner.orTogetherSelectivities(indexDistanceSelectivities).getOrElse(DEFAULT_RANGE_SELECTIVITY)
  }

  private def calculateSelectivityForSubstringSargable(variable: String,
                                                       labelInfo: LabelInfo,
                                                       relTypeInfo: RelTypeInfo,
                                                       propertyKey: PropertyKeyName,
                                                       maybeString: Option[String])
                                                      (implicit semanticTable: SemanticTable): Selectivity = {
    val stringLength = maybeString match {
      case Some(n) => n.length
      case None => DEFAULT_STRING_LENGTH
    }

    def default = if (stringLength == 0) {
      // This is equal to exists && isString
      DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_TYPE_SELECTIVITY
    } else {
      // This is equal to range, but anti-proportional to the string length
      Selectivity(DEFAULT_RANGE_SELECTIVITY.factor / stringLength)
    }

    val indexPropertyExistsSelectivities = indexPropertyExistsSelectivitiesFor(variable, labelInfo, relTypeInfo, propertyKey)
    val indexSubstringSelectivities = indexPropertyExistsSelectivities.map { exists =>
      if (stringLength == 0) {
        // This is equal to exists && isString
        exists * DEFAULT_TYPE_SELECTIVITY
      } else {
        // This is equal to range, but anti-proportional to the string length
        val res = exists.factor * DEFAULT_RANGE_SEEK_FACTOR / stringLength
        Selectivity(res)
      }
    }
    combiner.orTogetherSelectivities(indexSubstringSelectivities).getOrElse(default)
  }
}
