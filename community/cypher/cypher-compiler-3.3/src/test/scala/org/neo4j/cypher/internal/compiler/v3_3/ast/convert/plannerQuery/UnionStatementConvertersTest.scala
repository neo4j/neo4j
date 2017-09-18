/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.ast.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.RegularQueryProjection

class UnionStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("RETURN 1 as x UNION RETURN 2 as x") {
    val query = buildPlannerUnionQuery("RETURN 1 as x UNION RETURN 2 as x")
    query.distinct should equal(true)
    query.queries should have size 2

    val q1 = query.queries.head
    q1.queryGraph.patternNodes shouldBe empty
    q1.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("1")(pos))))

    val q2 = query.queries.last
    q2.queryGraph.patternNodes shouldBe empty
    q2.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("2")(pos))))
  }

  test("RETURN 1 as x UNION ALL RETURN 2 as x UNION ALL RETURN 3 as x") {
    val query = buildPlannerUnionQuery("RETURN 1 as x UNION ALL RETURN 2 as x UNION ALL RETURN 3 as x")
    query.distinct should equal(false)
    query.queries should have size 3

    val q1 = query.queries.head
    q1.queryGraph.patternNodes shouldBe empty
    q1.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("1")(pos))))

    val q2 = query.queries.tail.head
    q2.queryGraph.patternNodes shouldBe empty
    q2.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("2")(pos))))

    val q3 = query.queries.last
    q3.queryGraph.patternNodes shouldBe empty
    q3.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("3")(pos))))
  }

}
