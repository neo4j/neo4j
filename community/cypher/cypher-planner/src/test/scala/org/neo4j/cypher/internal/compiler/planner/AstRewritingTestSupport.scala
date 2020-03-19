/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.ParserFixture
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.Identifiable
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherTestSupport

trait LogicalPlanConstructionTestSupport extends CypherTestSupport {
  self: AstConstructionTestSupport =>

  implicit val idGen: SequentialIdGen = new SequentialIdGen()

  implicit protected def idSymbol(name: Symbol): String = name.name

  trait StubAttribute[KEY <: Identifiable, VALUE] extends Default[KEY, VALUE] {
    override def set(id: Id, t: VALUE): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): VALUE = defaultValue

    override def copy(from: Id, to: Id): Unit = {}
  }

  class StubSolveds extends Solveds with StubAttribute[LogicalPlan, PlannerQueryPart] {
    override def defaultValue: SinglePlannerQuery = SinglePlannerQuery.empty
  }

  class StubCardinalities extends Cardinalities with StubAttribute[LogicalPlan, Cardinality] {
    override def defaultValue: Cardinality = 0.0
  }

  class StubProvidedOrders extends ProvidedOrders with StubAttribute[LogicalPlan, ProvidedOrder] {
    override def defaultValue: ProvidedOrder = ProvidedOrder.empty
  }

  class StubLeveragedOrders extends LeveragedOrders with StubAttribute[LogicalPlan, Boolean]

  def newStubbedPlanningAttributes: PlanningAttributes = PlanningAttributes(new StubSolveds, new StubCardinalities, new StubProvidedOrders, new StubLeveragedOrders)
}

trait AstRewritingTestSupport extends CypherTestSupport with AstConstructionTestSupport {
  val parser: CypherParser = ParserFixture.parser
}
