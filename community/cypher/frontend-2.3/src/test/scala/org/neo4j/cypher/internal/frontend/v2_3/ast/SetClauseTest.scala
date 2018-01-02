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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticState}

class SetClauseTest extends CypherFunSuite {

  test("shouldHaveMergedTypesOfAllAlternativesInSimpleCase") {

    val mapLiteral = DummyExpression(CTMap)
    val property = Property(mapLiteral, PropertyKeyName("key")(DummyPosition(3)))(DummyPosition(5))
    val setItem = SetPropertyItem(property, DummyExpression(CTAny))(DummyPosition(42))
    val setClause = SetClause(Seq(setItem))(DummyPosition(6))


    val result = setClause.semanticCheck(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.msg should startWith("Type mismatch: expected Node or Relationship but was Map")
  }
}
