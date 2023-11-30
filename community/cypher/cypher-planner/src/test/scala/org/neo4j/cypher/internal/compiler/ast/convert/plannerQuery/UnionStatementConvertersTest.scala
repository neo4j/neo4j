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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("RETURN 1 as x UNION RETURN 2 as x") {
    val query = buildPlannerQuery("RETURN 1 as x UNION RETURN 2 as x")
    query should be(a[UnionQuery])
    val unionQuery = query.asInstanceOf[UnionQuery]
    unionQuery.distinct should equal(true)

    unionQuery.lhs should be(a[SinglePlannerQuery])
    val q1 = unionQuery.lhs.asInstanceOf[SinglePlannerQuery]
    q1.queryGraph.patternNodes shouldBe empty
    q1.horizon should equal(RegularQueryProjection(Map(v"x" -> literalInt(1))))

    val q2 = unionQuery.rhs
    q2.queryGraph.patternNodes shouldBe empty
    q2.horizon should equal(RegularQueryProjection(Map(v"x" -> literalInt(2))))
  }

  test("RETURN 1 as x UNION ALL RETURN 2 as x UNION ALL RETURN 3 as x") {
    val query = buildPlannerQuery("RETURN 1 as x UNION ALL RETURN 2 as x UNION ALL RETURN 3 as x")
    query should be(a[UnionQuery])
    val unionQuery = query.asInstanceOf[UnionQuery]
    unionQuery.distinct should equal(false)

    unionQuery.lhs should be(a[UnionQuery])
    val innerUnion = unionQuery.lhs.asInstanceOf[UnionQuery]
    innerUnion.distinct should equal(false)

    innerUnion.lhs should be(a[SinglePlannerQuery])
    val q1 = innerUnion.lhs.asInstanceOf[SinglePlannerQuery]
    q1.queryGraph.patternNodes shouldBe empty
    q1.horizon should equal(RegularQueryProjection(Map(v"x" -> literalInt(1))))

    val q2 = innerUnion.rhs
    q2.queryGraph.patternNodes shouldBe empty
    q2.horizon should equal(RegularQueryProjection(Map(v"x" -> literalInt(2))))

    val q3 = unionQuery.rhs
    q3.queryGraph.patternNodes shouldBe empty
    q3.horizon should equal(RegularQueryProjection(Map(v"x" -> literalInt(3))))
  }

}
