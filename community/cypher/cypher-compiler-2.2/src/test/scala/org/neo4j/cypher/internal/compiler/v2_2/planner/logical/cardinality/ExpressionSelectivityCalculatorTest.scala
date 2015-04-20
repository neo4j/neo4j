/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.mockito.Mockito
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Selectivity
import org.neo4j.cypher.internal.compiler.v2_2.{PropertyKeyId, LabelId}
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Predicate, Selections, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

class ExpressionSelectivityCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should consider parameter expressions when calculating index selectivity") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelIds.put("Page", LabelId(0))
    semanticTable.resolvedPropertyKeyNames.put("title", PropertyKeyId(0))

    implicit val selections = Selections(Set(Predicate(Set(IdName("n")), HasLabels(ident("n"), Seq(LabelName("Page")_))_)))

    val stats = mock[GraphStatistics]
    Mockito.when(stats.nodesWithLabelCardinality(None)).thenReturn(1000.0)
    Mockito.when(stats.indexSelectivity(LabelId(0), PropertyKeyId(0))).thenReturn(Some(Selectivity(0.1d)))

    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val result = calculator(In(Property(ident("n"), PropertyKeyName("title")_)_, Parameter("titles")_)_)

    result.factor should equal (0.92 +- 0.01)
  }
}
