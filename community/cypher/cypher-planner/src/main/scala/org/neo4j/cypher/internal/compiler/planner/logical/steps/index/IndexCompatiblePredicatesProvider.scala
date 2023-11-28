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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsBoundingBoxSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsStringRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsValueRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexRequirement
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.MultipleExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NonSeekablePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NotExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.SingleExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProvider.allPossibleRangeIndexRequirements
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProvider.findExplicitCompatiblePredicates
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialDistanceSeekWrapper
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexQuery.IndexQueryType
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider
import org.neo4j.values.storable.ValueCategory

trait IndexCompatiblePredicatesProvider {

  /**
   * Collects predicates which could be used to justify the use of an index. Some may be collected from the predicates provided here. Others might be
   * implicitly inferred (e.g. through constraints, see implicitIndexCompatiblePredicates)
   *
   * @param predicates            selection predicates from the where clause
   * @param argumentIds           argument ids provided to this sub-plan
   * @param semanticTable         semantic table
   * @param planContext           planContext to ask for indexes
   */
  private[index] def findIndexCompatiblePredicates(
    predicates: Set[Expression],
    argumentIds: Set[String],
    semanticTable: SemanticTable,
    planContext: PlanContext,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext
  ): Set[IndexCompatiblePredicate] = {
    val arguments: Set[LogicalVariable] = argumentIds.map(varFor)

    val explicitCompatiblePredicates = findExplicitCompatiblePredicates(arguments, predicates, semanticTable)

    def valid(ident: LogicalVariable, dependencies: Set[LogicalVariable]): Boolean =
      !arguments.contains(ident) && dependencies.subsetOf(arguments)
    val implicitCompatiblePredicates = implicitIndexCompatiblePredicates(
      planContext,
      indexPredicateProviderContext,
      predicates,
      explicitCompatiblePredicates,
      valid
    )

    // Any predicate may be solved partially (i.e. n.prop IS NOT NULL) by a RANGE index scan.
    val partialCompatiblePredicates = {

      // These are non-RANGE compatible predicates downgraded to a RANGE-scannable predicates.
      val downgradedToRangeScannable = explicitCompatiblePredicates.collect {
        case predicate if !predicate.indexRequirements.subsetOf(allPossibleRangeIndexRequirements) =>
          predicate.convertToRangeScannable
      }

      // These partial RANGE predicates are already covered by an equivalent or better RANGE-compatible predicate.
      val alreadyCovered = explicitCompatiblePredicates.collect {
        case predicate if predicate.indexRequirements.subsetOf(allPossibleRangeIndexRequirements) =>
          predicate.convertToRangeScannable
      }

      // `downgradedToRangeScannable` and `alreadyCovered` might overlap.
      // For a predicate like n.prop < 'hello', we find two explicit compatible predicates, one RANGE-seekable and one TEXT-scannable.
      // Downgrading TEXT-scannable to RANGE-scannable produces a predicate that is already covered by our RANGE-seekable predicate.
      // To avoid considering these redundant predicates - exclude them.
      downgradedToRangeScannable -- alreadyCovered
    }

    explicitCompatiblePredicates ++ implicitCompatiblePredicates ++ partialCompatiblePredicates
  }

  /**
   * Find any implicit index compatible predicates.
   *
   * @param planContext                  planContext to ask for indexes
   * @param predicates                   the predicates in the query
   * @param explicitCompatiblePredicates the explicit index compatible predicates that were extracted from predicates
   * @param valid                        a test that can be applied to check if an implicit predicate is valid
   *                                     based on its variable and dependencies as arguments to the lambda function.
   */
  protected def implicitIndexCompatiblePredicates(
    planContext: PlanContext,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    predicates: Set[Expression],
    explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
    valid: (LogicalVariable, Set[LogicalVariable]) => Boolean
  ): Set[IndexCompatiblePredicate]
}

object IndexCompatiblePredicatesProvider {

  def findExplicitCompatiblePredicates(
    arguments: Set[LogicalVariable],
    predicates: Set[Expression],
    semanticTable: SemanticTable
  ): Set[IndexCompatiblePredicate] = {
    def valid(ident: LogicalVariable, dependencies: Set[LogicalVariable]): Boolean =
      !arguments.contains(ident) && dependencies.subsetOf(arguments)

    predicates.flatMap {
      // n.prop IN [ ... ]
      case predicate @ AsPropertySeekable(seekable: PropertySeekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.args.asQueryExpression
        val exactness = queryExpression match {
          case _: SingleQueryExpression[_] => SingleExactPredicate
          case _                           => MultipleExactPredicate
        }
        Set(IndexCompatiblePredicate(
          seekable.ident,
          seekable.expr,
          predicate,
          queryExpression,
          predicateExactness = exactness,
          solvedPredicate = Some(predicate),
          dependencies = seekable.dependencies,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXACT)),
          cypherType = seekable.propertyValueType(semanticTable)
        ))

      // ... = n.prop
      // In some rare cases, we can't rewrite these predicates cleanly,
      // and so planning needs to search for these cases explicitly
      case predicate @ Equals(lhs, prop @ Property(variable: LogicalVariable, _))
        if valid(variable, lhs.dependencies) =>
        val expr = SingleQueryExpression(lhs)
        val seekable = PropertySeekable(prop, variable, SingleSeekableArg(lhs))
        Set(IndexCompatiblePredicate(
          variable,
          prop,
          predicate,
          expr,
          predicateExactness = SingleExactPredicate,
          solvedPredicate = Some(predicate),
          dependencies = lhs.dependencies,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXACT)),
          cypherType = seekable.propertyValueType(semanticTable)
        ))

      // n.prop STARTS WITH "prefix%..."
      case predicate @ AsStringRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(
          seekable.ident,
          seekable.property,
          predicate,
          queryExpression,
          predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate),
          dependencies = seekable.dependencies,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.STRING_PREFIX)),
          cypherType = seekable.propertyValueType(semanticTable)
        ))

      // n.prop < |<=| >| >= value
      case predicate @ AsValueRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        val cypherType = seekable.propertyValueType(semanticTable)
        val rangePredicate = IndexCompatiblePredicate(
          seekable.ident,
          seekable.property,
          predicate,
          queryExpression,
          predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate),
          dependencies = seekable.dependencies,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.RANGE)),
          cypherType = cypherType
        )

        Set(rangePredicate) ++
          Option.when(cypherType.isSubtypeOf(CTString))(rangePredicate.convertToTextScannable) ++
          Option.when(cypherType.isSubtypeOf(CTPoint))(rangePredicate.convertToPointScannable)

      case predicate @ AsBoundingBoxSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(
          seekable.ident,
          seekable.property,
          predicate,
          queryExpression,
          predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate),
          dependencies = seekable.dependencies,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.BOUNDING_BOX)),
          cypherType = seekable.propertyValueType(semanticTable)
        ))

      // An index seek for this will almost satisfy the predicate, but with the possibility of some false positives.
      // Since it reduces the cardinality to almost the level of the predicate, we can use the predicate to calculate cardinality,
      // but not mark it as solved, since the planner will still need to solve it with a Filter.
      case predicate @ AsDistanceSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(
          seekable.ident,
          seekable.property,
          predicate,
          queryExpression,
          predicateExactness = NotExactPredicate,
          solvedPredicate = Some(PartialDistanceSeekWrapper(predicate)),
          dependencies = seekable.dependencies,
          // Distance on an index level uses IndexQueryType.BOUNDING_BOX
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.BOUNDING_BOX)),
          cypherType = seekable.propertyValueType(semanticTable)
        ))

      // n.prop ENDS WITH 'substring'
      case predicate @ EndsWith(prop @ Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(
          variable,
          prop,
          predicate,
          ExistenceQueryExpression(),
          predicateExactness = NonSeekablePredicate,
          solvedPredicate = Some(predicate),
          dependencies = expr.dependencies,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.STRING_SUFFIX)),
          cypherType = CTString
        ))

      // n.prop CONTAINS 'substring'
      case predicate @ Contains(prop @ Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(
          variable,
          prop,
          predicate,
          ExistenceQueryExpression(),
          predicateExactness = NonSeekablePredicate,
          solvedPredicate = Some(predicate),
          dependencies = expr.dependencies,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.STRING_CONTAINS)),
          cypherType = CTString
        ))

      // MATCH (n:User) WHERE n.prop IS NOT NULL RETURN n
      case predicate @ AsPropertyScannable(scannable) if valid(scannable.ident, Set.empty) =>
        val explicitlyScannableRangePredicate = IndexCompatiblePredicate(
          scannable.ident,
          scannable.property,
          predicate,
          ExistenceQueryExpression(),
          predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate),
          dependencies = Set.empty,
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
          cypherType = scannable.cypherType
        )

        val finalPredicate = scannable.cypherType match {
          case StringType(_) => explicitlyScannableRangePredicate.convertToTextScannable
          case PointType(_)  => explicitlyScannableRangePredicate.convertToPointScannable
          case _             => explicitlyScannableRangePredicate.convertToRangeScannable
        }

        Set(finalPredicate)

      case _ => Set.empty[IndexCompatiblePredicate]
    }
  }

  private val allPossibleRangeIndexRequirements: Set[IndexRequirement] = {
    val rangeIndexCapability: IndexCapability = RangeIndexProvider.CAPABILITY
    val supportedQueryTypes = IndexQueryType.values().collect {
      case queryType if rangeIndexCapability.isQuerySupported(queryType, ValueCategory.ANYTHING) =>
        IndexRequirement.SupportsIndexQuery(queryType)
    }

    Set(IndexRequirement.HasType(IndexType.Range)) ++ supportedQueryTypes
  }
}

/**
 * @param aggregatingProperties A set of all properties over which aggregation is performed,
 *                              where we potentially could use an IndexScan.
 *                              E.g. WITH n.prop1 AS prop RETURN min(prop), count(m.prop2) => Set(PropertyAccess("n", "prop1"), PropertyAccess("m", "prop2"))
 * @param outerPlanHasUpdates   A flag indicating whether we have planned updates earlier in the query
 */
final case class IndexCompatiblePredicatesProviderContext(
  aggregatingProperties: Set[PropertyAccess],
  outerPlanHasUpdates: Boolean
)

object IndexCompatiblePredicatesProviderContext {

  val default: IndexCompatiblePredicatesProviderContext =
    IndexCompatiblePredicatesProviderContext(aggregatingProperties = Set.empty, outerPlanHasUpdates = false)
}
