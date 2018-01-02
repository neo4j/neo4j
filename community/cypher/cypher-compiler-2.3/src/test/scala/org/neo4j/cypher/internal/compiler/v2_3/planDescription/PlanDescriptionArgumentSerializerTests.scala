/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planDescription

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{ExpandExpression,
EstimatedRows, DbHits, Rows}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class PlanDescriptionArgumentSerializerTests extends CypherFunSuite {

  val serialize = PlanDescriptionArgumentSerializer.serialize _

  test("serialization should leave numeric arguments as numbers") {
    serialize(new DbHits(12)) shouldBe a [java.lang.Number]
    serialize(new Rows(12)) shouldBe a [java.lang.Number]
    serialize(new EstimatedRows(12)) shouldBe a [java.lang.Number]
  }

  test("ExpandExpression should look like Cypher syntax") {
    serialize(new ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, false)) should equal ("(a)-[r:LIKES|:LOVES]->(b)")
  }
}
