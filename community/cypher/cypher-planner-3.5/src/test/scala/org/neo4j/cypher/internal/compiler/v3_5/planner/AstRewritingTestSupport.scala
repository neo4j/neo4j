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
package org.neo4j.cypher.internal.compiler.v3_5.planner

import org.neo4j.cypher.internal.ir.v3_5.PlannerQuery
import org.neo4j.cypher.internal.ir.v3_5.ProvidedOrder
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v3_5.logical.plans.GetValue
import org.neo4j.cypher.internal.v3_5.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.v3_5.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.v3_5.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions.LabelToken
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.v3_5.parser.ParserFixture
import org.neo4j.cypher.internal.v3_5.util.Cardinality
import org.neo4j.cypher.internal.v3_5.util.LabelId
import org.neo4j.cypher.internal.v3_5.util.PropertyKeyId
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherTestSupport

import scala.language.implicitConversions

trait LogicalPlanConstructionTestSupport extends CypherTestSupport {
  self: AstConstructionTestSupport =>

  implicit val idGen = new SequentialIdGen()

  implicit protected def idSymbol(name: Symbol): String = name.name

  class StubSolveds extends Solveds {
    override def set(id: Id, t: PlannerQuery): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): PlannerQuery = PlannerQuery.empty

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

  def nodeIndexScan(node: String, label: String, property: String) =
    NodeIndexScan(node, LabelToken(label, LabelId(1)), IndexedProperty(PropertyKeyToken(property, PropertyKeyId(1)), GetValue), Set.empty, IndexOrderNone)

  def cached(varAndProp: String): CachedNodeProperty = {
    val array = varAndProp.split("\\.", 2)
    val (v, prop) = (array(0), array(1))
    CachedNodeProperty(v, PropertyKeyName(prop)(pos))(pos)
  }

}

trait AstRewritingTestSupport extends CypherTestSupport with AstConstructionTestSupport {
  val parser = ParserFixture.parser
}
