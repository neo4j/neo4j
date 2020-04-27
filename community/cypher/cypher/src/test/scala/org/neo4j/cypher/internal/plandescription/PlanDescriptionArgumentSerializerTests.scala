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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.PlanDescriptionArgumentSerializer.serialize
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PlanDescriptionArgumentSerializerTests extends CypherFunSuite {
  test("serialization should leave numeric arguments as numbers") {
    serialize(DbHits(12)) shouldBe a [java.lang.Number]
    serialize(Rows(12)) shouldBe a [java.lang.Number]
    serialize(EstimatedRows(12)) shouldBe a [java.lang.Number]
  }

  test("should serialize provided order") {
    serialize(Order(ProvidedOrder.asc(varFor("a")).desc(varFor("b")).asc(prop("c","foo")))) should be("a ASC, b DESC, c.foo ASC")
    serialize(Order(ProvidedOrder.empty)) should be("")
    serialize(Order(ProvidedOrder.asc(varFor("  FRESHID42")))) should be("anon[42] ASC")
  }

  private val pos: InputPosition = DummyPosition(0)
  private def varFor(name: String): Variable = Variable(name)(pos)
  private def prop(varName: String, propName: String): Property = Property(varFor(varName), PropertyKeyName(propName)(pos))(pos)
}
