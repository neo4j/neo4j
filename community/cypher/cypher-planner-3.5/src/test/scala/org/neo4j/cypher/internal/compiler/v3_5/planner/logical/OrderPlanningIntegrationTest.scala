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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite


class OrderPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {
  test("ORDER BY previously unprojected column in WITH") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.age RETURN a.name")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("  FRESHID30" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("  FRESHID30")))
    val resultProjection = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(resultProjection)
  }

  test("ORDER BY previously unprojected column in WITH and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.age RETURN a.name, a.age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("  FRESHID30" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("  FRESHID30")))
    val resultProjection = Projection(sort, Map("a.name" -> nameProperty, "a.age" -> ageProperty))

    plan should equal(resultProjection)
  }

  test("ORDER BY previously unprojected column in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a, a.age AS age ORDER BY age RETURN a.name, age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("age")))
    val resultProjection = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(resultProjection)
  }

  test("ORDER BY renamed column old name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY a RETURN b.name, age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("b"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("b" -> varFor("a")))
    val sort = Sort(projection, Seq(Ascending("b")))
    val projection2 = Projection(sort, Map("age" -> ageProperty))
    val result = Projection(projection2, Map("b.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY renamed column new name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY b RETURN b.name, age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("b"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("b" -> varFor("a")))
    val sort = Sort(projection, Seq(Ascending("b")))
    val projection2 = Projection(sort, Map("age" -> ageProperty))
    val result = Projection(projection2, Map("b.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY renamed column expression with old name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY a.foo, a.age + 5 RETURN b.name, age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("b"), PropertyKeyName("name") _) _
    val fooProperty = Property(varFor("b"), PropertyKeyName("foo") _) _

    val projection = Projection(labelScan, Map("b" -> varFor("a"), "age" -> ageProperty))
    val projection2 = Projection(projection, Map("  FRESHID49" -> fooProperty, "  FRESHID60" -> Add(varFor("age"), SignedDecimalIntegerLiteral("5") _) _))
    val sort = Sort(projection2, Seq(Ascending("  FRESHID49"), Ascending("  FRESHID60")))
    val result = Projection(sort, Map("b.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY renamed column expression with new name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY b.foo, b.age + 5 RETURN b.name, age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageAProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val ageBProperty = Property(varFor("b"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("b"), PropertyKeyName("name") _) _
    val fooProperty = Property(varFor("b"), PropertyKeyName("foo") _) _

    val projection = Projection(labelScan, Map("b" -> varFor("a")))
    val projection2 = Projection(projection, Map("  FRESHID49" -> fooProperty, "  FRESHID60" -> Add(ageBProperty, SignedDecimalIntegerLiteral("5") _) _))
    val sort = Sort(projection2, Seq(Ascending("  FRESHID49"), Ascending("  FRESHID60")))
    val projection3 = Projection(sort, Map("age" -> ageAProperty))
    val result = Projection(projection3, Map("b.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN a.name ORDER BY a.age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("  FRESHID37" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("  FRESHID37")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN and return that column") {
    val plan = new given().getLogicalPlanFor("""MATCH (a:A) RETURN a.name, a.age ORDER BY a.age""")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN a.name, a.age AS age ORDER BY age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("age")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN *") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN * ORDER BY a.age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _

    val projection = Projection(labelScan, Map("  FRESHID32" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("  FRESHID32")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected column in RETURN * and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN *, a.age ORDER BY a.age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected column in RETURN * and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN *, a.age AS age ORDER BY age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _

    val projection = Projection(labelScan, Map("age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected column with expression in WITH") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.age + 4 RETURN a.name")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _

    val projection = Projection(labelScan, Map("  FRESHID34" -> Add(ageProperty, SignedDecimalIntegerLiteral("4") _) _))
    val sort = Sort(projection, Seq(Ascending("  FRESHID34")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected DISTINCT column in WITH and project and return it") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH DISTINCT a.age AS age ORDER BY age RETURN age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _

    val distinct = Distinct(labelScan, Map("age" -> ageProperty))
    val sort = Sort(distinct, Seq(Ascending("age")))

    plan should equal(sort)
  }

  test("ORDER BY column that isn't referenced in WITH DISTINCT") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH DISTINCT a.name AS name, a ORDER BY a.age RETURN name")._2

    val labelScan = NodeByLabelScan("  a@7", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("  a@43"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("  a@7"), PropertyKeyName("name") _) _

    val distinct = Distinct(labelScan, Map("name" -> nameProperty, "  a@43" -> varFor("  a@7")))
    val projection = Projection(distinct, Map("  FRESHID55" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("  FRESHID55")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected AGGREGATING column in WITH and project and return it") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a.name AS name, sum(a.age) AS age ORDER BY age RETURN name, age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _
    val ageSum = FunctionInvocation(Namespace(List()) _, FunctionName("sum") _, distinct = false, Vector(ageProperty))_

    val aggregation = Aggregation(labelScan, Map("name" -> nameProperty), Map("age" -> ageSum))
    val sort = Sort(aggregation, Seq(Ascending("age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected GROUPING column in WITH and project and return it") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a.name AS name, sum(a.age) AS age ORDER BY name RETURN name, age")._2

    val labelScan = NodeByLabelScan("a", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("a"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("a"), PropertyKeyName("name") _) _
    val ageSum = FunctionInvocation(Namespace(List()) _, FunctionName("sum") _, distinct = false, Vector(ageProperty)) _

    val aggregation = Aggregation(labelScan, Map("name" -> nameProperty), Map("age" -> ageSum))
    val sort = Sort(aggregation, Seq(Ascending("name")))

    plan should equal(sort)
  }

  test("ORDER BY column that isn't referenced in WITH GROUP BY") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a.name AS name, a, sum(a.age) AS age ORDER BY a.foo RETURN name, age")._2

    val labelScan = NodeByLabelScan("  a@7", LabelName("A") _, Set.empty)
    val ageProperty = Property(varFor("  a@7"), PropertyKeyName("age") _) _
    val nameProperty = Property(varFor("  a@7"), PropertyKeyName("name") _) _
    val fooProperty = Property(varFor("  a@34"), PropertyKeyName("foo") _) _
    val ageSum = FunctionInvocation(Namespace(List()) _, FunctionName("sum") _, distinct = false, Vector(ageProperty)) _

    val aggregation = Aggregation(labelScan, Map("name" -> nameProperty, "  a@34" -> varFor("  a@7")), Map("age" -> ageSum))
    val projection = Projection(aggregation, Map("  FRESHID65" -> fooProperty))
    val sort = Sort(projection, Seq(Ascending("  FRESHID65")))

    plan should equal(sort)
  }
}
