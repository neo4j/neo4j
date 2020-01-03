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

import org.neo4j.cypher.internal.ir.{PlannerQueryPart, ProvidedOrder, SinglePlannerQuery}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.parser.{CypherParser, ParserFixture}
import org.neo4j.cypher.internal.v4_0.util.Cardinality
import org.neo4j.cypher.internal.v4_0.util.attribution.{Id, SequentialIdGen}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherTestSupport

import scala.language.implicitConversions

trait LogicalPlanConstructionTestSupport extends CypherTestSupport {
  self: AstConstructionTestSupport =>

  implicit val idGen: SequentialIdGen = new SequentialIdGen()

  implicit protected def idSymbol(name: Symbol): String = name.name

  class StubSolveds extends Solveds {
    override def set(id: Id, t: PlannerQueryPart): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): SinglePlannerQuery = SinglePlannerQuery.empty

    override def copy(from: Id, to: Id): Unit = {}
  }

  class StubCardinalities extends Cardinalities {
    override def set(id: Id, t: Cardinality): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): Cardinality = 0.0

    override def copy(from: Id, to: Id): Unit = {}
  }

  class StubProvidedOrders extends ProvidedOrders {
    override def set(id: Id, t: ProvidedOrder): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): ProvidedOrder = ProvidedOrder.empty

    override def copy(from: Id, to: Id): Unit = {}
  }
}

trait AstRewritingTestSupport extends CypherTestSupport with AstConstructionTestSupport {
  val parser: CypherParser = ParserFixture.parser
}
