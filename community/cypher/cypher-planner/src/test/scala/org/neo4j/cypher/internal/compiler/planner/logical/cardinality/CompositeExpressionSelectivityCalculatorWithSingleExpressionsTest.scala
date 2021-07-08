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

import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.EntityType.Node
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.EntityType.Relationship
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Selectivity

/**
 * Test that CompositeExpressionSelectivityCalculator returns the same results as ExpressionSelectivityCalculator for single expressions.
 */
class CompositeExpressionSelectivityCalculatorWithSingleExpressionsTest extends ExpressionSelectivityCalculatorTest {
  override protected def setUpCalculator(labelInfo: LabelInfo = Map.empty, relTypeInfo: RelTypeInfo = Map.empty, stats: GraphStatistics = mockStats()): Expression => Selectivity = {
    val semanticTable = setupSemanticTable()
    val combiner = IndependenceCombiner
    val compositeCalculator = CompositeExpressionSelectivityCalculator(stats, combiner)
    exp: Expression => {
      compositeCalculator(Selections.from(exp), labelInfo, relTypeInfo, semanticTable, mockPlanContext(stats), IndexCompatiblePredicatesProviderContext.default)
    }
  }

  private def getNameId(descriptor: IndexDescriptor): Int = descriptor.entityType match {
    case Node(labelId) => labelId.id
    case Relationship(relTypeId) => relTypeId.id
  }

  private def mockPlanContext(stats: GraphStatistics): PlanContext = new NotImplementedPlanContext {
    val indexMap: Map[Int, IndexDescriptor] = stats match {
      case mockStats(_, _, _, indexCardinalities, _) => indexCardinalities.keys.map(desc => getNameId(desc) -> desc).toMap
      case _ => Map.empty
    }
    override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = Set.empty

    override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = indexMap.get(labelId).iterator

    override def indexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = indexMap.get(relTypeId).iterator

    override def getRelationshipPropertiesWithExistenceConstraint(labelName: String): Set[String] = Set.empty

    override def txStateHasChanges(): Boolean = false
  }
}
