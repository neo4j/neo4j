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

import org.apache.commons.math3.special.Erf.erf
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
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
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator.defaultSelectivityForPropertyEquality
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator.getPropertyPredicateRangeSelectivity
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator.getStringLength
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator.indexSelectivityForSubstringSargable
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator.indexSelectivityWithSizeHint
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator.selectivityForPropertyEquality
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator.subqueryCardinalityToExistsSelectivity
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsBoundingBoxSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsElementIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsStringRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsValueRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.IdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.InequalityRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PointBoundingBoxSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PointDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PrefixRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.VarLengthBound
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.EntityType
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.WithSizeHint
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.StringType

case class ExpressionSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) {

  /**
   * Index type priority to be used to calculate selectivities of exists predicates, given that a substring predicate is used.
    */
  private val indexTypesPriorityForSubstringSargable: Seq[IndexType] = Seq(
    IndexType.Text,
    IndexType.Range
  )

  private val indexTypesPriorityForPropertyExistence: Seq[IndexType] = Seq(
    IndexType.Range,
    IndexType.Text,
    IndexType.Point
  )

  private def indexTypesForPropertyEquality(propertyType: CypherType): Seq[IndexType] = Seq(
    Some(IndexType.Range),
    // We cannot use a text index for cardinality estimation unless we know that the property type is a String
    if (propertyType == CTString) Some(IndexType.Text) else None
  ).flatten

  private val indexTypesForRangeSeeks: Seq[IndexType] = Seq(
    IndexType.Range
  )

  private val indexTypesPriorityForPointPredicates: Seq[IndexType] = Seq(
    IndexType.Point,
    IndexType.Range
  )

  def apply(exp: Expression, labelInfo: LabelInfo, relTypeInfo: RelTypeInfo)(
    implicit semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Selectivity = exp match {
    // WHERE a:Label
    case HasLabels(_, label :: Nil) =>
      calculateSelectivityForLabel(semanticTable.id(label))

    // WHERE true
    case True() =>
      Selectivity.ONE

    // WHERE false
    case False() =>
      Selectivity.ZERO

    // SubPredicate(sub, super)
    case partial: PartialPredicate[_] =>
      apply(partial.coveredPredicate, labelInfo, relTypeInfo)

    // WHERE x.prop =/IN ...
    case AsPropertySeekable(seekable) =>
      calculateSelectivityForPropertyEquality(
        seekable.name,
        seekable.propertyValueType(semanticTable),
        seekable.args.sizeHint,
        labelInfo,
        relTypeInfo,
        seekable.propertyKey
      )

    // WHERE x.prop STARTS WITH expression
    case AsStringRangeSeekable(seekable @ PrefixRangeSeekable(PrefixRange(prefix), _, _, _)) =>
      calculateSelectivityForSubstringSargable(
        seekable.name,
        labelInfo,
        relTypeInfo,
        seekable.propertyKeyName,
        prefix,
        prefix = true
      )

    // WHERE x.prop CONTAINS expression
    case Contains(Property(Variable(name), propertyKey), substring) =>
      calculateSelectivityForSubstringSargable(name, labelInfo, relTypeInfo, propertyKey, substring)

    // WHERE x.prop ENDS WITH expression
    case EndsWith(Property(Variable(name), propertyKey), substring) =>
      calculateSelectivityForSubstringSargable(name, labelInfo, relTypeInfo, propertyKey, substring)

    // WHERE x.prop =~ expression
    case RegexMatch(Property(Variable(name), propertyKey), _) =>
      // as we cannot reason about the regular expression that we compare with, then we should probably treat it just like
      // a string comparison where we know nothing about the substring to compare with
      calculateSelectivityForSubstringSargable(
        name,
        labelInfo,
        relTypeInfo,
        propertyKey,
        Variable("")(InputPosition.NONE)
      )

    // WHERE distance(p.prop, otherPoint) <, <= number that could benefit from an index
    case AsDistanceSeekable(seekable) =>
      calculateSelectivityForPointDistanceSeekable(seekable, labelInfo, relTypeInfo)

    // WHERE point.withinBBox(p.prop, ll, ur) that could benefit from an index
    case AsBoundingBoxSeekable(seekable) =>
      calculateSelectivityForPointBoundingBoxSeekable(seekable, labelInfo, relTypeInfo)

    // WHERE x.prop <, <=, >=, > that could benefit from an index
    case AsValueRangeSeekable(seekable) =>
      calculateSelectivityForValueRangeSeekable(seekable, labelInfo, relTypeInfo)

    // WHERE x.prop IS NOT NULL
    case AsPropertyScannable(scannable) =>
      calculateSelectivityForPropertyExistence(scannable.name, labelInfo, relTypeInfo, scannable.propertyKey)

    // Implicit relation uniqueness predicates
    case _: DifferentRelationships =>
      // This should not be the default. Instead, we should figure out the number of matching relationships and use it
      DEFAULT_REL_UNIQUENESS_SELECTIVITY

    case _: Unique | _: IsRepeatTrailUnique =>
      // These are currently only generated for var-length or QPP uniqueness predicates and
      // those are already included in the calculations in PatternRelationshipMultiplierCalculator.
      // We should revisit this when doing Cardinality estimation for QPPs.
      Selectivity.ONE

    case _: VarLengthBound =>
      // These are inserted by AddVarLengthPredicates and taken care of in the cardinality estimation of the referenced var-length relationship.
      Selectivity.ONE

    // WHERE NOT [...]
    case Not(inner) =>
      apply(inner, labelInfo, relTypeInfo).negate

    case Ors(expressions) =>
      val selectivities = expressions.toIndexedSeq.map(apply(_, labelInfo, relTypeInfo))
      combiner.orTogetherSelectivities(selectivities).get // We can trust the AST to never have empty ORs

    // WHERE id(x) =/IN [...]
    case AsIdSeekable(seekable) =>
      calculateSelectivityForIdSeekable(seekable)

    // WHERE elementId(x) =/IN [...]
    case AsElementIdSeekable(seekable) =>
      calculateSelectivityForIdSeekable(seekable)

    // WHERE <expr> = <expr>
    case _: Equals =>
      DEFAULT_EQUALITY_SELECTIVITY

    // WHERE <expr> >= <expr>
    case _: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual =>
      DEFAULT_RANGE_SELECTIVITY

    case x: ExistsIRExpression =>
      val subqueryCardinality = cardinalityModel.apply(
        x.query,
        labelInfo,
        relTypeInfo,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )
      subqueryCardinalityToExistsSelectivity(subqueryCardinality)

    case _: AssertIsNode =>
      Selectivity.ONE

    case _ =>
      DEFAULT_PREDICATE_SELECTIVITY
  }

  private def calculateSelectivityForLabel(label: Option[LabelId]): Selectivity = {
    val labelCardinality = stats.nodesWithLabelCardinality(label)
    labelCardinality / stats.nodesAllCardinality() getOrElse Selectivity.ONE
  }

  private def calculateSelectivityForPropertyExistence(
    variable: String,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    propertyKey: PropertyKeyName
  )(implicit semanticTable: SemanticTable): Selectivity = {
    val indexTypesAbleToAnswerIsNotNull: Set[IndexType] = Set(IndexType.Range)

    val indexPropertyExistsSelectivities =
      multipleIndexPropertyExistsSelectivitiesFor(
        variable,
        labelInfo,
        relTypeInfo,
        propertyKey,
        indexTypesPriorityForPropertyExistence
      ).map {
        case (selectivity, indexType) =>
          if (indexTypesAbleToAnswerIsNotNull.contains(indexType))
            selectivity
          else
            Selectivity
              .of(
                selectivity.factor + (selectivity.negate * DEFAULT_PROPERTY_SELECTIVITY).factor
              ) // not as accurate as RANGE, but can be an improvement over the default value
              .getOrElse(DEFAULT_PROPERTY_SELECTIVITY)
      }

    combiner.orTogetherSelectivities(indexPropertyExistsSelectivities).getOrElse(DEFAULT_PROPERTY_SELECTIVITY)
  }

  private def multipleIndexPropertyExistsSelectivitiesFor(
    variable: String,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    propertyKey: PropertyKeyName,
    indexTypesPriorityOrder: Seq[IndexType]
  )(implicit semanticTable: SemanticTable): Seq[(Selectivity, IndexType)] = {
    val labels = labelInfo.getOrElse(variable, Set.empty)
    val relTypes = relTypeInfo.get(variable)

    val entityTypeAndPropertyIds: Seq[(NameId, PropertyKeyId)] = (labels ++ relTypes).toIndexedSeq.flatMap {
      case labelName: LabelName => for {
          labelId <- semanticTable.id(labelName)
          propId <- semanticTable.id(propertyKey)
        } yield (labelId, propId)

      case relTypeName: RelTypeName => for {
          relTypeId <- semanticTable.id(relTypeName)
          propId <- semanticTable.id(propertyKey)
        } yield (relTypeId, propId)
    }

    entityTypeAndPropertyIds.flatMap { case (entityTypeId, propertyKeyId) =>
      val selectivitiesInIndexPriorityOrder = for {
        indexType <- indexTypesPriorityOrder
        selectivity <- indexPropertyIsNotNullSelectivity(indexType, entityTypeId, propertyKeyId)
      } yield (selectivity, indexType)

      selectivitiesInIndexPriorityOrder.headOption
    }
  }

  private def indexPropertyIsNotNullSelectivity(
    indexType: IndexType,
    entityTypeId: NameId,
    propertyKeyId: PropertyKeyId
  ): Option[Selectivity] = {
    entityTypeId match {
      case labelId: LabelId =>
        val descriptor = IndexDescriptor.forLabel(indexType, labelId, Seq(propertyKeyId))
        stats.indexPropertyIsNotNullSelectivity(descriptor)

      case relTypeId: RelTypeId =>
        val descriptor = IndexDescriptor.forRelType(indexType, relTypeId, Seq(propertyKeyId))
        stats.indexPropertyIsNotNullSelectivity(descriptor)

      case _ => Some(Selectivity.ZERO)
    }
  }

  private def calculateSelectivityForPropertyEquality(
    variable: String,
    cypherType: CypherType,
    sizeHint: Option[Int],
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    propertyKey: PropertyKeyName
  )(implicit semanticTable: SemanticTable): Selectivity = {
    val indexTypesToConsider = indexTypesForPropertyEquality(cypherType)
    indexSelectivityWithSizeHint(
      sizeHint,
      { size =>
        val labels = labelInfo.getOrElse(variable, Set.empty)
        val relTypes = relTypeInfo.get(variable)
        val indexSelectivities = (labels ++ relTypes).toIndexedSeq.flatMap { name =>
          def descriptorCreator(nameId: Option[NameId], propertyKeyId: PropertyKeyId) =
            nameId.map(id => indexTypesToConsider.map(IndexDescriptor(_, EntityType.of(id), Seq(propertyKeyId))))
              .getOrElse(Seq.empty)

          val descriptors: Seq[IndexDescriptor] = (name, semanticTable.id(propertyKey)) match {
            case (labelName: LabelName, Some(propKeyId)) => descriptorCreator(semanticTable.id(labelName), propKeyId)
            case (relTypeName: RelTypeName, Some(propKeyId)) =>
              descriptorCreator(semanticTable.id(relTypeName), propKeyId)
            case _ => Seq.empty
          }

          descriptors.flatMap(indexSelectivityForPropertyEquality(_, size)).headOption
        }

        combiner.orTogetherSelectivities(indexSelectivities)
          .orElse(defaultSelectivityForPropertyEquality(size, combiner))
          .getOrElse(DEFAULT_PREDICATE_SELECTIVITY)
      }
    )
  }

  private def indexSelectivityForPropertyEquality(descriptor: IndexDescriptor, size: Int): Option[Selectivity] =
    selectivityForPropertyEquality(
      stats.indexPropertyIsNotNullSelectivity(descriptor),
      stats.uniqueValueSelectivity(descriptor),
      size,
      combiner
    )

  private def calculateSelectivityForValueRangeSeekable(
    seekable: InequalityRangeSeekable,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo
  )(implicit semanticTable: SemanticTable): Selectivity = {
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
        case relTypeName: RelTypeName =>
          (semanticTable.id(relTypeName), semanticTable.id(seekable.expr.property.propertyKey))
      }

      ids match {
        case (Some(labelOrRelTypeId), Some(propertyKeyId)) =>
          for {
            descriptor <-
              indexTypesForRangeSeeks.map(IndexDescriptor.forNameId(_, labelOrRelTypeId, Seq(propertyKeyId)))
            propertyExistsSelectivity <- stats.indexPropertyIsNotNullSelectivity(descriptor)
            propEqValueSelectivity <- stats.uniqueValueSelectivity(descriptor)
          } yield {
            val pRangeBounded: Selectivity = getPropertyPredicateRangeSelectivity(seekable, propEqValueSelectivity)
            pRangeBounded * propertyExistsSelectivity
          }

        case _ => Some(Selectivity.ZERO)
      }
    }

    combiner.orTogetherSelectivities(indexRangeSelectivities).getOrElse(default)
  }

  private def calculateSelectivityForPointDistanceSeekable(
    seekable: PointDistanceSeekable,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo
  )(implicit semanticTable: SemanticTable): Selectivity = {
    val indexPropertyExistsSelectivities =
      multipleIndexPropertyExistsSelectivitiesFor(
        seekable.ident.name,
        labelInfo,
        relTypeInfo,
        seekable.propertyKeyName,
        indexTypesPriorityForPointPredicates
      )

    val indexDistanceSelectivities = indexPropertyExistsSelectivities.map { case (selectivity, _) =>
      selectivity * Selectivity(DEFAULT_RANGE_SEEK_FACTOR)
    }
    combiner.orTogetherSelectivities(indexDistanceSelectivities).getOrElse(DEFAULT_RANGE_SELECTIVITY)
  }

  private def calculateSelectivityForPointBoundingBoxSeekable(
    seekable: PointBoundingBoxSeekable,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo
  )(implicit semanticTable: SemanticTable): Selectivity = {
    // NOTE this equivalent to using two inequalities, like p1 <= n.prop <= p2
    def default = {
      val defaultRange = DEFAULT_RANGE_SELECTIVITY * Selectivity(0.5)
      Selectivity.of(DEFAULT_EQUALITY_SELECTIVITY.factor + defaultRange.factor).getOrElse(Selectivity.ONE)
    }

    // NOTE this equivalent to using two inequalities, like p1 <= n.prop <= p2
    def getPropertyPredicateRangeSelectivity(propEqValueSelectivity: Selectivity): Selectivity = {
      val pNeq = propEqValueSelectivity.negate
      val pNeqRange = pNeq.factor * DEFAULT_RANGE_SEEK_FACTOR / 2

      val pRange = Selectivity(propEqValueSelectivity.factor + pNeqRange)
      Selectivity(math.max(propEqValueSelectivity.factor, pRange.factor))
    }

    val labels = labelInfo.getOrElse(seekable.ident.name, Set.empty)
    val relTypes = relTypeInfo.get(seekable.ident.name)
    val indexRangeSelectivities: Seq[Selectivity] = (labels ++ relTypes).toIndexedSeq.flatMap { name =>
      val ids = name match {
        case labelName: LabelName => (semanticTable.id(labelName), semanticTable.id(seekable.property.propertyKey))
        case relTypeName: RelTypeName =>
          (semanticTable.id(relTypeName), semanticTable.id(seekable.property.propertyKey))
      }

      ids match {
        case (Some(labelOrRelTypeId), Some(propertyKeyId)) =>
          val selectivitiesInIndexPriorityOrder = for {
            descriptor <- indexTypesPriorityForPointPredicates.map(IndexDescriptor.forNameId(
              _,
              labelOrRelTypeId,
              Seq(propertyKeyId)
            ))
            propertyExistsSelectivity <- stats.indexPropertyIsNotNullSelectivity(descriptor)
            propEqValueSelectivity <- stats.uniqueValueSelectivity(descriptor)
          } yield {
            val pRangeBounded: Selectivity = getPropertyPredicateRangeSelectivity(propEqValueSelectivity)
            pRangeBounded * propertyExistsSelectivity
          }
          selectivitiesInIndexPriorityOrder.headOption

        case _ => Some(Selectivity.ZERO)
      }
    }
    combiner.orTogetherSelectivities(indexRangeSelectivities).getOrElse(default)
  }

  private def calculateSelectivityForSubstringSargable(
    variable: String,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    propertyKey: PropertyKeyName,
    stringExpression: Expression,
    prefix: Boolean = false
  )(implicit semanticTable: SemanticTable): Selectivity = {
    val stringLength = getStringLength(stringExpression)

    def default =
      if (stringLength == 0) {
        // This is equal to exists && isString
        DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_TYPE_SELECTIVITY
      } else {
        // This is equal to range, but anti-proportional to the string length
        Selectivity(DEFAULT_RANGE_SELECTIVITY.factor / stringLength)
      }

    val indexPropertyExistsSelectivities =
      multipleIndexPropertyExistsSelectivitiesFor(
        variable,
        labelInfo,
        relTypeInfo,
        propertyKey,
        indexTypesPriorityForSubstringSargable
      )

    val indexSubstringSelectivities = indexPropertyExistsSelectivities.map { case (exists, indexType) =>
      exists * indexSelectivityForSubstringSargable(stringLength, indexType)
    }
    combiner.orTogetherSelectivities(indexSubstringSelectivities).getOrElse(default)
  }

  private def calculateSelectivityForIdSeekable(seekable: IdSeekable)(implicit
  semanticTable: SemanticTable): Selectivity = {
    val lookups = seekable.args.sizeHint.map(Cardinality(_)).getOrElse(DEFAULT_NUMBER_OF_ID_LOOKUPS)
    if (semanticTable.isNodeNoFail(seekable.ident)) {
      (lookups / stats.nodesAllCardinality()) getOrElse Selectivity.ONE
    } else {
      (lookups / stats.patternStepCardinality(None, None, None)) getOrElse Selectivity.ONE
    }
  }
}

object ExpressionSelectivityCalculator {

  /**
   * The selectivity that a string starts with, contains or ends with a certain substring of length `stringLength`,
   * given that the property IS NOT NULL.
   */
  def indexSelectivityForSubstringSargable(stringLength: Int, indexType: IndexType): Selectivity = {
    if (stringLength == 0) {
      // selectivity is only that the property is of type string
      indexType match {
        case IndexType.Text => Selectivity.ONE
        case _              => DEFAULT_TYPE_SELECTIVITY
      }
    } else {
      // This is equal to range, but anti-proportional to the string length
      Selectivity(DEFAULT_RANGE_SEEK_FACTOR / stringLength)
    }
  }

  /**
   * The selectivity that a string starts with, contains or ends with a certain substring,
   * given that the property IS NOT NULL.
   */
  def indexSelectivityForSubstringSargable(
    stringExpression: Expression,
    indexType: IndexType = IndexType.Range
  ): Selectivity = {
    indexSelectivityForSubstringSargable(getStringLength(stringExpression), indexType)
  }

  def getStringLength(stringExpression: Expression): Int = {
    stringExpression match {
      case StringLiteral(value)                                => value.length
      case Parameter(_, _: StringType, WithSizeHint(sizeHint)) => sizeHint
      case _                                                   => DEFAULT_STRING_LENGTH
    }
  }

  /**
   * Estimates the seekable predicate's selectivity assuming existence of the predicate's property.
   * @param seekable the predicate
   * @param propEqValueSelectivity selectivity for equality on that property
   */
  def getPropertyPredicateRangeSelectivity(
    seekable: InequalityRangeSeekable,
    propEqValueSelectivity: Selectivity
  ): Selectivity = {
    val pNeq = propEqValueSelectivity.negate
    val pNeqRange = pNeq.factor * DEFAULT_RANGE_SEEK_FACTOR / Math.min(seekable.expr.inequalities.size, 2)

    val pRange = Selectivity(if (seekable.hasEquality) propEqValueSelectivity.factor + pNeqRange else pNeqRange)
    Selectivity(math.max(propEqValueSelectivity.factor, pRange.factor))
  }

  /**
   * Calculate a selectivity of an index with an optional list size hint.
   * @param sizeHint an optional hint for the size of the list
   * @param selectivityCalculator calculate the selectivity given a size
   */
  def indexSelectivityWithSizeHint(sizeHint: Option[Int], selectivityCalculator: Int => Selectivity): Selectivity = {
    sizeHint.getOrElse(DEFAULT_LIST_CARDINALITY.amount.toInt) match {
      case 0    => Selectivity.ZERO
      case size => selectivityCalculator(size)
    }
  }

  /**
   * Given the cardinality of a subquery `x`, returns the selectivity of `EXISTS { x }`.
   *
   * To avoid estimations of 0.0 for `NOT EXISTS { ... }`, we assume that the `subqueryCardinality`
   * is the median of a log-normally distributed function and return the probability of a sample
   * of that function being greater than 1.0.
   */
  def subqueryCardinalityToExistsSelectivity(subqueryCardinality: Cardinality): Selectivity = {
    Selectivity(probLognormalGreaterThan1(subqueryCardinality.amount))
  }

  /**
   * For a given median `median` of a log-normally distributed function with sigma 2.0,
   * return the probability that a sample of that function is greater than 1.
   *
   * {{{
   *   lognormal_cdf_σ_µ(x) = (1 + erf((ln(x)-µ) / (σ * Math.sqrt(2)) )) * 0.5
   *
   *   median(lognormal_cdf_σ_µ(x)) = e^µ // solving for µ
   *   µ = ln(median(...))
   *
   *   P(lognormal_2.0_µ(x)) >= 1 = 1 - P(lognormal_2.0_µ(x) < 1)
   *                              = 1 - lognormal_cdf_2.0_µ(1.0)
   *                              = 1 - (1 + erf((ln(1.0)-µ) / (2.0 * Math.sqrt(2)) )) * 0.5
   *                              = 1 - (1 + erf((-µ) / (2.0 * Math.sqrt(2)) )) * 0.5
   *                              = 1 - (1 + erf((-ln(median)) / (2.0 * Math.sqrt(2)) )) * 0.5
   * }}}
   */
  def probLognormalGreaterThan1(median: Double): Double = {
    val sigma = 2.0
    1 - (1 + erf((-Math.log(median)) / (sigma * Math.sqrt(2)))) * 0.5
  }

  def defaultSelectivityForPropertyEquality(size: Int, combiner: SelectivityCombiner): Option[Selectivity] =
    selectivityForPropertyEquality(
      Some(DEFAULT_PROPERTY_SELECTIVITY),
      Some(DEFAULT_EQUALITY_SELECTIVITY),
      size,
      combiner
    )

  def selectivityForPropertyEquality(
    propertySelectivity: Option[Selectivity],
    uniqueValueSelectivity: Option[Selectivity],
    size: Int,
    combiner: SelectivityCombiner
  ): Option[Selectivity] = for {
    propExists <- propertySelectivity
    propEqualsSingleValue <- uniqueValueSelectivity
    propEqualsAnyValue <- combiner.orTogetherSelectivities(Seq.fill(size)(propEqualsSingleValue))
    combinedSelectivity <- combiner.andTogetherSelectivities(Seq(propExists, propEqualsAnyValue))
  } yield combinedSelectivity
}
