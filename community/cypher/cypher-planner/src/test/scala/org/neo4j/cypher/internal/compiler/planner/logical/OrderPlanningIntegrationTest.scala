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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.createQueryGraphSolverWithComponentConnectorPlanner
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OrderPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("ORDER BY previously unprojected column in WITH") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.age RETURN a.name")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))
    val resultProjection = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(resultProjection)
  }

  test("ORDER BY previously unprojected column in WITH and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.age RETURN a.name, a.age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = cachedNodeProp("a", "age")
    val nameProperty = prop("a", "name")

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))
    val resultProjection = Projection(sort, Map("a.name" -> nameProperty, "a.age" -> ageProperty))

    plan should equal(resultProjection)
  }

  test("ORDER BY previously unprojected column in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a, a.age AS age ORDER BY age RETURN a.name, age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")

    val projection = Projection(labelScan, Map("age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("age")))
    val resultProjection = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(resultProjection)
  }

  test("ORDER BY renamed column old name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY a RETURN b.name, age", stripProduceResults = false)._2

    plan should equal(
      new LogicalPlanBuilder()
        .produceResults("`b.name`", "age")
        .projection("b.name AS `b.name`")
        .projection("a AS b", "a.age AS age")
        .nodeByLabelScan("a", "A", IndexOrderAscending)
        .build()
    )
  }

  test("ORDER BY renamed column new name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY b RETURN b.name, age", stripProduceResults = false)._2

    plan should equal(
      new LogicalPlanBuilder()
        .produceResults("`b.name`", "age")
        .projection("b.name AS `b.name`")
        .projection("a AS b", "a.age AS age")
        .nodeByLabelScan("a", "A", IndexOrderAscending)
        .build()
    )
  }

  test("ORDER BY renamed column expression with old name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY a.foo, a.age + 5 RETURN b.name, age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("b", "name")
    val fooProperty = prop("b", "foo")

    val projection = Projection(labelScan, Map("b" -> varFor("a"), "age" -> ageProperty))
    val projection2 = Projection(projection, Map("b.foo" -> fooProperty, "age + 5" -> add(varFor("age"), literalInt(5))))
    val sort = Sort(projection2, Seq(Ascending("b.foo"), Ascending("age + 5")))
    val result = Projection(sort, Map("b.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY renamed column expression with new name in WITH and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a AS b, a.age AS age ORDER BY b.foo, b.age + 5 RETURN b.name, age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageAProperty = cachedNodeProp("a", "age")
    val nameProperty = prop("b", "name")
    val fooProperty = prop("b", "foo")

    val projection = Projection(labelScan, Map("b" -> varFor("a")))
    val projection2 = Projection(projection, Map("b.foo" -> fooProperty, "b.age + 5" -> add(ageAProperty, literalInt(5))))
    val sort = Sort(projection2, Seq(Ascending("b.foo"), Ascending("b.age + 5")))
    val projection3 = Projection(sort, Map("age" -> ageAProperty))
    val result = Projection(projection3, Map("b.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN a.name ORDER BY a.age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN and return that column") {
    val plan = new given().getLogicalPlanFor("""MATCH (a:A) RETURN a.name, a.age ORDER BY a.age""")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN a.name, a.age AS age ORDER BY age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")

    val projection = Projection(labelScan, Map("age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("age")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected column in RETURN *") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN * ORDER BY a.age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected column in RETURN * and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN *, a.age ORDER BY a.age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")

    val projection = Projection(labelScan, Map("a.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("a.age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected column in RETURN * and project and return that column") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) RETURN *, a.age AS age ORDER BY age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")

    val projection = Projection(labelScan, Map("age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending("age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected column with expression in WITH") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.age + 4 RETURN a.name")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")

    val projection = Projection(labelScan, Map("a.age + 4" -> add(ageProperty, literalInt(4))))
    val sort = Sort(projection, Seq(Ascending("a.age + 4")))
    val result = Projection(sort, Map("a.name" -> nameProperty))

    plan should equal(result)
  }

  test("ORDER BY previously unprojected DISTINCT column in WITH and project and return it") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH DISTINCT a.age AS age ORDER BY age RETURN age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")

    val distinct = Distinct(labelScan, Map("age" -> ageProperty))
    val sort = Sort(distinct, Seq(Ascending("age")))

    plan should equal(sort)
  }

  test("ORDER BY column that isn't referenced in WITH DISTINCT") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH DISTINCT a.name AS name, a ORDER BY a.age RETURN name")._2

    val aAt7 = "  a@7"
    val aAt43 = "  a@43"
    val labelScan = NodeByLabelScan(aAt7, labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop(aAt43, "age")
    val nameProperty = prop(aAt7, "name")

    val distinct = Distinct(labelScan, Map("name" -> nameProperty, aAt43 -> varFor(aAt7)))
    val projection = Projection(distinct, Map(s"$aAt43.age" -> ageProperty))
    val sort = Sort(projection, Seq(Ascending(s"$aAt43.age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected AGGREGATING column in WITH and project and return it") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a.name AS name, sum(a.age) AS age ORDER BY age RETURN name, age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")
    val ageSum = sum(ageProperty)

    val aggregation = Aggregation(labelScan, Map("name" -> nameProperty), Map("age" -> ageSum))
    val sort = Sort(aggregation, Seq(Ascending("age")))

    plan should equal(sort)
  }

  test("ORDER BY previously unprojected GROUPING column in WITH and project and return it") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a.name AS name, sum(a.age) AS age ORDER BY name RETURN name, age")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop("a", "age")
    val nameProperty = prop("a", "name")
    val ageSum = sum(ageProperty)

    val aggregation = Aggregation(labelScan, Map("name" -> nameProperty), Map("age" -> ageSum))
    val sort = Sort(aggregation, Seq(Ascending("name")))

    plan should equal(sort)
  }

  test("ORDER BY column that isn't referenced in WITH GROUP BY") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a.name AS name, a, sum(a.age) AS age ORDER BY a.foo RETURN name, age")._2

    val aAt7 = "  a@7"
    val aAt34 = "  a@34"
    val labelScan = NodeByLabelScan(aAt7, labelName("A"), Set.empty, IndexOrderNone)
    val ageProperty = prop(aAt7, "age")
    val nameProperty = prop(aAt7, "name")
    val fooProperty = prop(aAt34, "foo")
    val ageSum = sum(ageProperty)

    val aggregation = Aggregation(labelScan, Map("name" -> nameProperty, aAt34 -> varFor(aAt7)), Map("age" -> ageSum))
    val projection = Projection(aggregation, Map(s"$aAt34.foo" -> fooProperty))
    val sort = Sort(projection, Seq(Ascending(s"$aAt34.foo")))

    plan should equal(sort)
  }

  test("should use ordered aggregation if there is one grouping column, ordered") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.foo RETURN a.foo, count(a.foo)")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val fooProperty = cachedNodeProp("a", "foo")
    val fooCount = count(fooProperty)

    val projection = Projection(labelScan, Map("a.foo" -> fooProperty))
    val sort = Sort(projection, Seq(Ascending("a.foo")))
    val aggregation = OrderedAggregation(sort, Map("a.foo" -> fooProperty), Map("count(a.foo)" -> fooCount), Seq(fooProperty))

    plan should equal(aggregation)
  }

  test("should use ordered aggregation if there are two grouping columns, one ordered") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.foo RETURN a.foo, a.bar, count(a.foo)")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val fooProperty = cachedNodeProp("a", "foo")
    val barProperty = prop("a", "bar")
    val fooCount = count(fooProperty)

    val projection = Projection(labelScan, Map("a.foo" -> fooProperty))
    val sort = Sort(projection, Seq(Ascending("a.foo")))
    val aggregation = OrderedAggregation(sort, Map("a.foo" -> fooProperty, "a.bar" -> barProperty), Map("count(a.foo)" -> fooCount), Seq(fooProperty))

    plan should equal(aggregation)
  }

  test("should use ordered aggregation if there are two grouping columns, both ordered") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.foo, a.bar RETURN a.foo, a.bar, count(a.foo)")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val fooProperty = cachedNodeProp("a", "foo")
    val barProperty = cachedNodeProp("a", "bar")
    val fooCount = count(fooProperty)

    val projection = Projection(labelScan, Map("a.foo" -> fooProperty, "a.bar" -> barProperty))
    val sort = Sort(projection, Seq(Ascending("a.foo"), Ascending("a.bar")))
    val aggregation = OrderedAggregation(sort, Map("a.foo" -> fooProperty, "a.bar" -> barProperty), Map("count(a.foo)" -> fooCount), Seq(fooProperty, barProperty))

    plan should equal(aggregation)
  }

  test("should use ordered distinct if there is one grouping column, ordered") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.foo RETURN DISTINCT a.foo")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val fooProperty = cachedNodeProp("a", "foo")

    val projection = Projection(labelScan, Map("a.foo" -> fooProperty))
    val sort = Sort(projection, Seq(Ascending("a.foo")))
    val distinct = OrderedDistinct(sort, Map("a.foo" -> fooProperty), Seq(fooProperty))

    plan should equal(distinct)
  }

  test("should use ordered distinct if there are two grouping columns, one ordered") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.foo RETURN DISTINCT a.foo, a.bar")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val fooProperty = cachedNodeProp("a", "foo")
    val barProperty = prop("a", "bar")

    val projection = Projection(labelScan, Map("a.foo" -> fooProperty))
    val sort = Sort(projection, Seq(Ascending("a.foo")))
    val distinct = OrderedDistinct(sort, Map("a.foo" -> fooProperty, "a.bar" -> barProperty), Seq(fooProperty))

    plan should equal(distinct)
  }

  test("should use ordered distinct if there are two grouping columns, both ordered") {
    val plan = new given().getLogicalPlanFor("MATCH (a:A) WITH a ORDER BY a.foo, a.bar RETURN DISTINCT a.foo, a.bar")._2

    val labelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val fooProperty = cachedNodeProp("a", "foo")
    val barProperty = cachedNodeProp("a", "bar")

    val projection = Projection(labelScan, Map("a.foo" -> fooProperty, "a.bar" -> barProperty))
    val sort = Sort(projection, Seq(Ascending("a.foo"), Ascending("a.bar")))
    val distinct = OrderedDistinct(sort, Map("a.foo" -> fooProperty, "a.bar" -> barProperty), Seq(fooProperty, barProperty))

    plan should equal(distinct)
  }

  private val idpGiven = new given {
    cardinality = mapCardinality {
      case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("u") => 2.0
      case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("p") => 10.0
      case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 10.0
      case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("u", "p") => 20.0
      case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("p", "b") => 100.0
      case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("u", "p", "b") => 200.0
      case _ => throw new IllegalStateException("Unexpected PlannerQuery")
    }
  }

  test("should mark leveragedOrder in collect with ORDER BY") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) WITH a ORDER BY a.age RETURN collect(a.name)")
    val leveragedOrders = attrs.leveragedOrders

    leveragedOrders.get(plan.id) should be(true)
  }

  test("should not mark leveragedOrder in count with ORDER BY") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) WITH a ORDER BY a.age RETURN count(a.name)")
    val leveragedOrders = attrs.leveragedOrders

    leveragedOrders.get(plan.id) should be(false)
  }

  test("should not mark leveragedOrder in collect with no ORDER BY") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) RETURN collect(a.name)")
    val leveragedOrders = attrs.leveragedOrders

    leveragedOrders.get(plan.id) should be(false)
  }

  test("should mark leveragedOrder if using ORDER BY in RETURN") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) RETURN a ORDER BY a.age", stripProduceResults = false)
    val leveragedOrders = attrs.leveragedOrders

    leveragedOrders.get(plan.id) should be(true)
  }

  test("should mark leveragedOrder if using ORDER BY in RETURN after a WITH") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) WITH a AS a, 1 AS foo RETURN a ORDER BY a.age", stripProduceResults = false)
    val leveragedOrders = attrs.leveragedOrders

    leveragedOrders.get(plan.id) should be(true)
  }

  test("should not mark leveragedOrder if using ORDER BY in RETURN in a UNION") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) RETURN a ORDER BY a.age UNION RETURN 1 AS a", stripProduceResults = false)
    val leveragedOrders = attrs.leveragedOrders

    leveragedOrders.get(plan.id) should be(false)
  }

  test("should mark leveragedOrder in LIMIT with ORDER BY") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) WITH a ORDER BY a.age LIMIT 1 RETURN a.name")
    val leveragedOrders = attrs.leveragedOrders

    val projection = plan.asInstanceOf[Projection]
    val top = projection.lhs.get.asInstanceOf[Top]

    leveragedOrders.get(projection.id) should be(false)
    leveragedOrders.get(top.id) should be(true)
  }

  test("should mark leveragedOrder in SKIP with ORDER BY") {
    val (_, plan, _, attrs) = new given().getLogicalPlanFor("MATCH (a) WITH a ORDER BY a.age SKIP 1 RETURN a.name")
    val leveragedOrders = attrs.leveragedOrders

    val projection = plan.asInstanceOf[Projection]
    val skip = projection.source.asInstanceOf[Skip]
    val sort = skip.source.asInstanceOf[Sort]

    leveragedOrders.get(projection.id) should be(false)
    leveragedOrders.get(skip.id) should be(true)
    leveragedOrders.get(sort.id) should be(true)
  }

  test("Should plan sort before first expand when sorting on property") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name, b.title
        |ORDER BY u.name""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Selection(_,
              Expand(
                Sort(_,Seq(Ascending("u.name"))), _, _, _, _, _, _
              )
            ), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort before first expand when sorting on node") {
    // Not having a label on u, otherwise we can take the order from the label scan and don't need to sort at all,
    // which would make this test useless.
    val query =
      """MATCH (u)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name, b.title
        |ORDER BY u""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Selection(_,
              Expand(
                Sort(_,Seq(Ascending("u"))), _, _, _, _, _, _
              )
            ), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort before first expand when sorting on renamed property") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name AS name, b.title
        |ORDER BY name""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Selection(_,
              Expand(
                Sort(_,Seq(Ascending("name"))), _, _, _, _, _, _
              )
            ), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort before first expand when sorting on the old name of a renamed property") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name AS name, b.title
        |ORDER BY u.name""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Selection(_,
              Expand(
                Sort(_,Seq(Ascending("name"))), _, _, _, _, _, _
              )
            ), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort before first expand when sorting on a property of a renamed node") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u AS v, b.title
        |ORDER BY v.name""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Selection(_,
              Expand(
                Sort(_,Seq(Ascending("v.name"))), _, _, _, _, _, _
              )
            ), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort after expand if lower cardinality") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name, b.title
        |ORDER BY u.name""".stripMargin
    val plan = new given {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("u") => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("p") => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("u", "p") => 5.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("p", "b") => 100.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("u", "p", "b") => 50.0
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
    }.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Sort(_,Seq(Ascending("u.name"))), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should not expand from lowest cardinality point, if sorting can be avoided, when sorting on 1 variables.") {
    val query =
      """MATCH (a:A)-->(b:B)-->(c:C)
        |WHERE exists(a.prop) AND exists(b.prop) AND exists(c.prop)
        |RETURN a, b, c
        |ORDER BY a.prop""".stripMargin
    val plan = new given {
      // Make it cheapest to start on b
      labelCardinality = Map("A" -> 200.0, "B" -> 100.0, "C" -> 200.0)
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("B", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("C", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query)._2

    // Different join and expand variants are OK, but it should maintain the
    // order from a
    plan.treeCount {
      case _: Sort => true
      case _: PartialSort => true
    } shouldBe 0
  }

  test("Should not plan cartesian product with lowest cardinality point leftmost, if sorting can be avoided, when sorting on 1 variables.") {
    val query =
      """MATCH (a:A), (b:B), (c:C)
        |WHERE exists(a.prop) AND exists(b.prop) AND exists(c.prop)
        |RETURN a, b, c
        |ORDER BY a.prop""".stripMargin
    val plan = new given {
      // Make it cheapest to start on b
      labelCardinality = Map("A" -> 200.0, "B" -> 100.0, "C" -> 200.0)
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("B", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("C", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query, queryGraphSolver = createQueryGraphSolverWithComponentConnectorPlanner())._2

    // Different cartesian product variants are OK, but it should maintain the
    // order from a
    plan.treeCount {
      case _: Sort => true
      case _: PartialSort => true
    } shouldBe 0
  }

  test("Should not plan value hash join with lowest cardinality point leftmost, if sorting can be avoided, when sorting on 1 variables.") {
    val query =
      """MATCH (a:A), (b:B)
        |WHERE exists(a.prop) AND exists(b.prop) AND a.foo = b.foo
        |RETURN a, b
        |ORDER BY a.prop""".stripMargin

    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 0.5,
      hasLabels("b", "B") -> 0.9,
      equals(prop("a", "foo"), prop("b", "foo")) -> 0.4,
      exists(prop("a", "prop"))-> 0.4,
      exists(prop("b", "prop"))-> 0.4,
    )

    val plan = new given {
      cardinality = selectivitiesCardinality(selectivities, qg => Math.pow(100.0, qg.connectedComponents.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("B", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query)._2

    // It should maintain the
    // order from a
    plan.treeCount {
      case _: Sort => true
      case _: PartialSort => true
    } shouldBe 0
  }

  test("Should not expand from lowest cardinality point, if sorting can be avoided, when sorting on 2 variables.") {
    val query =
      """MATCH (a:A)-->(b:B)-->(c:C)
        |WHERE exists(a.prop) AND exists(b.prop) AND exists(c.prop)
        |RETURN a, b, c
        |ORDER BY a.prop, c.prop""".stripMargin
    val plan = new given {
      // Make it cheapest to start on b
      labelCardinality = Map("A" -> 200.0, "B" -> 100.0, "C" -> 200.0)
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("B", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("C", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query)._2

    // Different join and expand variants are OK, but it should maintain the
    // order from a and only need to partially sort c
    plan.treeCount {
      case _: Sort => true
    } shouldBe 0
    plan.treeCount {
      case PartialSort(_, Seq(Ascending("a.prop")), Seq(Ascending("c.prop"))) => true
    } shouldBe 1
  }

  test("Should not plan cartesian product with lowest cardinality point leftmost, if sorting can be avoided, when sorting on 2 variables.") {
    val query =
      """MATCH (a:A), (b:B), (c:C)
        |WHERE exists(a.prop) AND exists(b.prop) AND exists(c.prop)
        |RETURN a, b, c
        |ORDER BY a.prop, c.prop""".stripMargin
    val plan = new given {
      // Make it cheapest to start on b
      labelCardinality = Map("A" -> 200.0, "B" -> 100.0, "C" -> 200.0)
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("B", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("C", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query, queryGraphSolver = createQueryGraphSolverWithComponentConnectorPlanner())._2

    // Different cartesian product variants are OK, but it should maintain the
    // order from a and only need to partially sort c
    plan.treeCount {
      case _: Sort => true
    } shouldBe 0
    plan.treeCount {
      case PartialSort(_, Seq(Ascending("a.prop")), Seq(Ascending("c.prop"))) => true
    } shouldBe 1
  }

  test("Should not plan value hash join with lowest cardinality point leftmost, if sorting can be avoided, when sorting on 2 variables.") {
    val query =
      """MATCH (a:A), (b:B)
        |WHERE exists(a.prop) AND exists(b.prop) AND a.foo = b.foo
        |RETURN a, b
        |ORDER BY a.prop, b.prop""".stripMargin

    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 0.5,
      hasLabels("b", "B") -> 0.9,
      equals(prop("a", "foo"), prop("b", "foo")) -> 0.4,
      exists(prop("a", "prop"))-> 0.4,
      exists(prop("b", "prop"))-> 0.4,
    )

    val plan = new given {
      cardinality = selectivitiesCardinality(selectivities, qg => Math.pow(100.0, qg.connectedComponents.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("B", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query, queryGraphSolver = createQueryGraphSolverWithComponentConnectorPlanner())._2

    // It should maintain the
    // order from a and only need to partially sort b
    plan.treeCount {
      case _: Sort => true
    } shouldBe 0
    plan.treeCount {
      case PartialSort(_, Seq(Ascending("a.prop")), Seq(Ascending("b.prop"))) => true
    } shouldBe 1
  }

  test("Should not expand (longer pattern) from lowest cardinality point, if sorting can be avoided, when sorting on 2 variables.") {
    val query =
      """MATCH (a:A)-->(b:B)-->(c:C)--(d:D)--(e:E)
        |WHERE exists(a.prop) AND exists(b.prop) AND exists(c.prop) AND exists(d.prop) AND exists(e.prop)
        |RETURN a, b, c
        |ORDER BY a.prop, e.prop""".stripMargin

    // Make it most expensive to start on a, but only by a very small margin
    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 1.0,
      hasLabels("b", "B") -> 0.99,
      hasLabels("c", "C") -> 0.99,
      hasLabels("d", "D") -> 0.99,
      hasLabels("e", "E") -> 0.99,
    ).withDefaultValue(1.0)

    val plan = new given {
      // The more nodes, the less row
      cardinality = selectivitiesCardinality(selectivities, qg => 1000000 * Math.pow(0.7, qg.patternNodes.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("B", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("C", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("D", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("E", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query)._2

    // Different join and expand variants are OK, but it should maintain the
    // order from a and only need to partially sort e
    plan.treeCount {
      case _: Sort => true
    } shouldBe 0
    plan should beLike {
      case PartialSort(_, Seq(Ascending("a.prop")), Seq(Ascending("e.prop"))) => ()
    }
  }

  test("Should choose larger index on the same variable, if sorting can be avoided, when sorting on 2 variables.") {
    val query =
      """MATCH (a:A)-->(b)-->(c)
        |WHERE exists(a.prop) AND exists(a.foo)
        |RETURN a, b, c
        |ORDER BY a.prop, c.prop""".stripMargin

    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 1.0,
      Exists(prop("a", "prop"))(pos) -> 0.5,
      Exists(prop("a", "foo"))(pos) -> 0.1 // Make it cheapest to start on a.foo.
    ).withDefaultValue(1.0)

    val plan = new given {
      cardinality = selectivitiesCardinality(selectivities, qg => Math.pow(100.0, qg.patternNodes.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("A", "foo")
    }.getLogicalPlanFor(query)._2

    // Different join and expand variants are OK, but it should maintain the
    // order from a and only need to partially sort c.
    // If this were to disregard sorting, the index on a.foo would be more selective and thus a better choice than a.prop.
    plan.treeCount {
      case _: Sort => true
    } shouldBe 0
    plan should beLike {
      case PartialSort(_, Seq(Ascending("a.prop")), Seq(Ascending("c.prop"))) => ()
    }
  }

  test("Should choose larger index on the same variable, if sorting can be avoided, when sorting on 3 variables.") {
    val query =
      """MATCH (a:A)-->(b)-->(c)
        |WHERE exists(a.prop) AND exists(a.foo)
        |RETURN a, b, c
        |ORDER BY a.prop, b.prop, c.prop""".stripMargin

    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 1.0,
      Exists(prop("a", "prop"))(pos) -> 0.5,
      Exists(prop("a", "foo"))(pos) -> 0.1 // Make it cheapest to start on a.foo.
    ).withDefaultValue(1.0)

    val plan = new given {
      cardinality = selectivitiesCardinality(selectivities, qg => Math.pow(100.0, qg.patternNodes.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("A", "foo")
    }.getLogicalPlanFor(query)._2

    // Different join and expand variants are OK, but it should maintain the
    // order from a and only need to partially sort b, c.
    // If this were to disregard sorting, the index on a.foo would be more selective and thus a better choice than a.prop.
    withClue(plan) {
      plan.treeCount {
        case _: Sort => true
      } shouldBe 0
      plan should beLike {
        case PartialSort(_, Seq(Ascending("a.prop")), Seq(Ascending("b.prop"), Ascending("c.prop"))) => ()
      }
    }
  }

  test("Should choose larger index on the same variable, if sorting can be avoided, when sorting on 2 variables, with projection.") {
    val query =
      """MATCH (a:A)-->(b)-->(c)
        |WHERE exists(a.prop) AND exists(a.foo)
        |RETURN a.prop AS first, c.prop AS second
        |ORDER BY first, second""".stripMargin

    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 1.0,
      Exists(prop("a", "prop"))(pos) -> 0.5,
      Exists(prop("a", "foo"))(pos) -> 0.1
    ).withDefaultValue(1.0)

    val plan = new given {
      // Make it cheapest to start on a.foo.
      cardinality = selectivitiesCardinality(selectivities, qg => Math.pow(100.0, qg.patternNodes.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
      indexOn("A", "foo")
    }.getLogicalPlanFor(query)._2

    // Different join and expand variants are OK, but it should maintain the
    // order from a and only need to partially sort c.
    // If this were to disregard sorting, the index on a.foo would be more selective and thus a better choice than a.prop.
    plan.treeCount {
      case _: Sort => true
    } shouldBe 0
    plan should beLike {
      case PartialSort(_, Seq(Ascending("first")), Seq(Ascending("second"))) => ()
    }
  }

  test("Should plan Partial Sort in between Expands, when sorting on 2 variables with projection.") {
    val query =
      """MATCH (a:A)-->(b)-->(c)
        |WHERE exists(a.prop)
        |RETURN a.prop AS first, b.prop AS second
        |ORDER BY first, second""".stripMargin

    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 1.0,
      Exists(prop("a", "prop"))(pos) -> 0.5
    ).withDefaultValue(1.0)

    val plan = new given {
      cardinality = selectivitiesCardinality(selectivities, qg => Math.pow(100.0, qg.patternNodes.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query)._2

    // Different join and expand variants are OK, but it should maintain the
    // order from a and only need to partially sort b.
    plan.treeCount {
      case _: Sort => true
    } shouldBe 0
    plan.treeCount {
      case PartialSort(_, Seq(Ascending("first")), Seq(Ascending("second"))) => true
    } shouldBe 1
    // The PartialSort should be between the expands, not at the end.
    plan should not be a[PartialSort]
  }

  test("Should plan Partial Sort in between Expands, when sorting on 3 variables with projection.") {
    val query =
      """MATCH (a:A)-->(b)-->(c)-->(d)
        |WHERE exists(a.prop)
        |RETURN a.prop AS first, b.prop AS second, c.prop AS third
        |ORDER BY first, second, third""".stripMargin

    val selectivities = Map[Expression, Double](
      hasLabels("a", "A") -> 1.0,
      Exists(prop("a", "prop"))(pos) -> 0.5
    ).withDefaultValue(1.0)

    val plan = new given {
      cardinality = selectivitiesCardinality(selectivities, qg => Math.pow(100.0, qg.patternNodes.size))
      indexOn("A", "prop").providesOrder(IndexOrderCapability.BOTH)
    }.getLogicalPlanFor(query)._2

    withClue(plan) {
      // Different join and expand variants are OK, but it should maintain the
      // order from a and only need to partially sort b, c.
      plan.treeCount {
        case _: Sort => true
      } shouldBe 0
      plan.treeCount {
        case PartialSort(_, Seq(Ascending("first")), Seq(Ascending("second"), Ascending("third"))) => true
      } shouldBe 1
      // The PartialSort should be between the expands, not at the end.
      plan should not be a[PartialSort]
    }
  }

  test("Should plan sort last when sorting on a property in last node in the expand") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name, b.title
        |ORDER BY b.title""".stripMargin
    val plan = new given().getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Sort(_,Seq(Ascending("b.title"))), _
      ) => ()
    }
  }

  test("Should plan sort last when sorting on the last node in the expand") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name, b.title
        |ORDER BY b""".stripMargin
    val plan = new given().getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Sort(_,Seq(Ascending("b"))), _
      ) => ()
    }
  }

  test("Should plan sort between the expands when ordering by functions of both nodes in first expand and included aliased in return") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name + p.name AS add, b.title
        |ORDER BY add""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Sort(_,Seq(Ascending("add"))), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort between the expands when ordering by functions of both nodes in first expand and included unaliased in return") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name + p.name, b.title
        |ORDER BY u.name + p.name""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Sort(_,Seq(Ascending("u.name + p.name"))), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort between the expands when ordering by functions of both nodes in first expand and not included in the return") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u.name AS uname, p.name AS pname, b.title
        |ORDER BY uname + pname""".stripMargin
    val plan = idpGiven.getLogicalPlanFor(query)._2

    plan should beLike {
      case Projection(
        Selection(_,
          Expand(
            Sort(_,Seq(Ascending("uname + pname"))), _, _, _, _, _, _
          )
        ), _
      ) => ()
    }
  }

  test("Should plan sort last when ordering by functions of node in last expand") {
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.name STARTS WITH 'Joe'
        |RETURN u, b.title
        |ORDER BY u.name + b.title""".stripMargin
    val plan = new given().getLogicalPlanFor(query)._2

    plan should beLike {
      case Sort(_,Seq(Ascending("u.name + b.title"))) => ()
    }
  }

  test("should handle pattern comprehension within map projection followed by ORDER BY") {
    val query =
      """
        |MATCH (n:`Operation`) WITH n RETURN n{.id,
        | Operation_bankAccount_BankAccount: [(n)<-[:`bankAccount`]-(n_bankAccount:`BankAccount`)|n_bankAccount{.id,
        |  BankAccount_user_jhi_user: [(n_bankAccount)-[:`user`]->(n_bankAccount_user:`jhi_user`)|n_bankAccount_user{.user_id,
        |   jhi_user_HAS_AUTHORITY_jhi_authority: [(n_bankAccount_user)-[:`HAS_AUTHORITY`]->(n_bankAccount_user_authorities:`jhi_authority`)|n_bankAccount_user_authorities{.name}]}],
        |  BankAccount_operations_Operation: [(n_bankAccount)-[:`operations`]->(n_bankAccount_operations:`Operation`)|n_bankAccount_operations{.id}]}],
        | Operation_LABELS_Label: [(n)-[:`LABELS`]->(n_labels:`Label`)|n_labels{.id,
        |  Label_OPERATIONS_Operation: [(n_labels)-[:`OPERATIONS`]->(n_labels_operations:`Operation`)|n_labels_operations{.id, .date, .description, .amount}]}]} ORDER by n.id
        |""".stripMargin

    val (_, plan, _, _) = new given().getLogicalPlanFor(query)
    plan shouldBe a[Sort]
  }
}
