/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.ir.v3_5.ProvidedOrder
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs

import scala.collection.mutable

class OrderAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  val nodeList: mutable.MutableList[Node] = mutable.MutableList()

  override def beforeEach(): Unit = {
    super.beforeEach()
    nodeList += createLabeledNode(Map("age" -> 10, "name" -> "A", "foo" -> 6), "A")
    nodeList += createLabeledNode(Map("age" -> 9, "name" -> "B", "foo" -> 5), "A")
    nodeList += createLabeledNode(Map("age" -> 12, "name" -> "C", "foo" -> 4), "A")
    nodeList += createLabeledNode(Map("age" -> 16, "name" -> "D", "foo" -> 3), "A")
    nodeList += createLabeledNode(Map("age" -> 14, "name" -> "E", "foo" -> 2), "A")
    nodeList += createLabeledNode(Map("age" -> 4, "name" -> "F", "foo" -> 1), "A")
  }

  test("ORDER BY previously unprojected column in WITH") {
    val result = executeWith(Configs.All, "MATCH (a:A) WITH a ORDER BY a.age RETURN a.name")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("anon[30]"))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
        ))

    result.columnAs[String]("a.name").toList should equal(List("F", "B", "A", "C", "E", "D"))
  }

  test("ORDER BY previously unprojected column in WITH and return that column") {
    val result = executeWith(Configs.All, "MATCH (a:A) WITH a ORDER BY a.age RETURN a.name, a.age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name, a.age : a.age}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("anon[30]"))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "a.age" -> 4),
      Map("a.name" -> "B", "a.age" -> 9),
      Map("a.name" -> "A", "a.age" -> 10),
      Map("a.name" -> "C", "a.age" -> 12),
      Map("a.name" -> "E", "a.age" -> 14),
      Map("a.name" -> "D", "a.age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in WITH and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a, a.age AS age
      ORDER BY age
      RETURN a.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("age"))
        .containingArgument("age")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{age : a.age}")
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "age" -> 4),
      Map("a.name" -> "B", "age" -> 9),
      Map("a.name" -> "A", "age" -> 10),
      Map("a.name" -> "C", "age" -> 12),
      Map("a.name" -> "E", "age" -> 14),
      Map("a.name" -> "D", "age" -> 16)
    ))
  }

  test("ORDER BY renamed column old name in WITH and project and return that column") {
    // 3.1 and older is buggy here
    val result = executeWith(Configs.All - Configs.Version2_3 - Configs.Version3_1,
      """
      MATCH (a:A)
      WITH a AS b, a.age AS age
      ORDER BY a
      RETURN b.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{age : a.age}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("b"))
        .containingArgument("b")
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : a}")
        ))

    result.toList should equal(List(
      Map("b.name" -> "A", "age" -> 10),
      Map("b.name" -> "B", "age" -> 9),
      Map("b.name" -> "C", "age" -> 12),
      Map("b.name" -> "D", "age" -> 16),
      Map("b.name" -> "E", "age" -> 14),
      Map("b.name" -> "F", "age" -> 4)
    ))
  }

  test("ORDER BY renamed column new name in WITH and project and return that column") {
    // 3.1 and older is buggy here
    val result = executeWith(Configs.All - Configs.Version2_3 - Configs.Version3_1,
      """
      MATCH (a:A)
      WITH a AS b, a.age AS age
      ORDER BY b
      RETURN b.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{age : a.age}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("b"))
        .containingArgument("b")
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : a}")
        ))

    result.toList should equal(List(
      Map("b.name" -> "A", "age" -> 10),
      Map("b.name" -> "B", "age" -> 9),
      Map("b.name" -> "C", "age" -> 12),
      Map("b.name" -> "D", "age" -> 16),
      Map("b.name" -> "E", "age" -> 14),
      Map("b.name" -> "F", "age" -> 4)
    ))
  }

  test("ORDER BY renamed column expression with old name in WITH and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a AS b, a.age AS age
      ORDER BY a.foo, a.age + 5
      RETURN b.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingArgumentRegex("\\{ : b\\.foo,  : age \\+ \\$`  AUTOINT\\d+`\\}".r)
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : a, age : a.age}")
        )
      )

    result.toList should equal(List(
      Map("b.name" -> "F", "age" -> 4),
      Map("b.name" -> "E", "age" -> 14),
      Map("b.name" -> "D", "age" -> 16),
      Map("b.name" -> "C", "age" -> 12),
      Map("b.name" -> "B", "age" -> 9),
      Map("b.name" -> "A", "age" -> 10)
    ))
  }

  test("ORDER BY renamed column expression with new name in WITH and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a AS b, a.age AS age
      ORDER BY b.foo, b.age + 5
      RETURN b.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{age : a.age}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingArgumentRegex("\\{ : b\\.foo,  : b\\.age \\+ \\$`  AUTOINT\\d+`\\}".r)
          .onTopOf(aPlan("Projection")
            .containingArgument("{b : a}")
          )
        ))

    result.toList should equal(List(
      Map("b.name" -> "F", "age" -> 4),
      Map("b.name" -> "E", "age" -> 14),
      Map("b.name" -> "D", "age" -> 16),
      Map("b.name" -> "C", "age" -> 12),
      Map("b.name" -> "B", "age" -> 9),
      Map("b.name" -> "A", "age" -> 10)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN") {
    val result = executeWith(Configs.All, "MATCH (a:A) RETURN a.name ORDER BY a.age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("anon[37]"))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
        ))

    result.toList should equal(List(
      Map("a.name" -> "F"),
      Map("a.name" -> "B"),
      Map("a.name" -> "A"),
      Map("a.name" -> "C"),
      Map("a.name" -> "E"),
      Map("a.name" -> "D")
    ))
  }

  test("ORDER BY previously unprojected column in RETURN and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN a.name, a.age
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{a.age : a.age}")
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "a.age" -> 4),
      Map("a.name" -> "B", "a.age" -> 9),
      Map("a.name" -> "A", "a.age" -> 10),
      Map("a.name" -> "C", "a.age" -> 12),
      Map("a.name" -> "E", "a.age" -> 14),
      Map("a.name" -> "D", "a.age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN a.name, a.age AS age
      ORDER BY age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{age : a.age}")
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "age" -> 4),
      Map("a.name" -> "B", "age" -> 9),
      Map("a.name" -> "A", "age" -> 10),
      Map("a.name" -> "C", "age" -> 12),
      Map("a.name" -> "E", "age" -> 14),
      Map("a.name" -> "D", "age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN *") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN *
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingVariables("a")
        .containingArgument("{ : a.age}")
      )

    result.toList should equal(List(
      Map("a" -> nodeList(5)),
      Map("a" -> nodeList(1)),
      Map("a" -> nodeList(0)),
      Map("a" -> nodeList(2)),
      Map("a" -> nodeList(4)),
      Map("a" -> nodeList(3))
    ))
  }

  test("ORDER BY previously unprojected column in RETURN * and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN *, a.age
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingVariables("a")
        .containingArgument("{a.age : a.age}")
      )

    result.toList should equal(List(
      Map("a" -> nodeList(5), "a.age" -> 4),
      Map("a" -> nodeList(1), "a.age" -> 9),
      Map("a" -> nodeList(0), "a.age" -> 10),
      Map("a" -> nodeList(2), "a.age" -> 12),
      Map("a" -> nodeList(4), "a.age" -> 14),
      Map("a" -> nodeList(3), "a.age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN * and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN *, a.age AS age
      ORDER BY age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingVariables("a")
        .containingArgument("{age : a.age}")
      )

    result.toList should equal(List(
      Map("a" -> nodeList(5), "age" -> 4),
      Map("a" -> nodeList(1), "age" -> 9),
      Map("a" -> nodeList(0), "age" -> 10),
      Map("a" -> nodeList(2), "age" -> 12),
      Map("a" -> nodeList(4), "age" -> 14),
      Map("a" -> nodeList(3), "age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column with expression in WITH") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a
      ORDER BY a.age + 4
      RETURN a.name
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentRegex("\\{ : a.age \\+ \\$`  AUTOINT\\d+`\\}".r)
        ))

    result.toList should equal(List(
      Map("a.name" -> "F"),
      Map("a.name" -> "B"),
      Map("a.name" -> "A"),
      Map("a.name" -> "C"),
      Map("a.name" -> "E"),
      Map("a.name" -> "D")
    ))
  }

  test("ORDER BY previously unprojected DISTINCT column in WITH and project and return it") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH DISTINCT a.age AS age
      ORDER BY age
      RETURN age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("age"))
      .containingArgument("age")
      .onTopOf(aPlan("Distinct")
        .containingVariables("age")
        .containingArgument("age")
      )

    result.toList should equal(List(
      Map("age" -> 4),
      Map("age" -> 9),
      Map("age" -> 10),
      Map("age" -> 12),
      Map("age" -> 14),
      Map("age" -> 16)
    ))
  }

  test("ORDER BY column that isn't referenced in WITH DISTINCT") {
    val result = executeWith(Configs.All, "MATCH (a:A) WITH DISTINCT a.name AS name, a ORDER BY a.age RETURN name")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("anon[55]"))
      .onTopOf(aPlan("Projection")
        .containingArgument("{ : a.age}")
        .onTopOf(aPlan("Distinct")
          .containingVariables("name", "a")
          .containingArgument("name, a")
        )
      )

    result.toList should equal(List(
      Map("name" -> "F"),
      Map("name" -> "B"),
      Map("name" -> "A"),
      Map("name" -> "C"),
      Map("name" -> "E"),
      Map("name" -> "D")
    ))
  }

  test("ORDER BY previously unprojected AGGREGATING column in WITH and project and return it") {
    // sum is not supported in compiled
    val result = executeWith(Configs.All - Configs.Compiled,
      """
      MATCH (a:A)
      WITH a.name AS name, sum(a.age) AS age
      ORDER BY age
      RETURN name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("age"))
      .containingArgument("age")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("name") // the group column
      )

    result.toList should equal(List(
      Map("name" -> "F", "age" -> 4),
      Map("name" -> "B", "age" -> 9),
      Map("name" -> "A", "age" -> 10),
      Map("name" -> "C", "age" -> 12),
      Map("name" -> "E", "age" -> 14),
      Map("name" -> "D", "age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected GROUPING column in WITH and project and return it") {
    // sum is not supported in compiled
    val result = executeWith(Configs.All - Configs.Compiled,
      """
      MATCH (a:A)
      WITH a.name AS name, sum(a.age) AS age
      ORDER BY name
      RETURN name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("name"))
      .containingArgument("name")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("name") // the group column
      )

    result.toList should equal(List(
      Map("name" -> "A", "age" -> 10),
      Map("name" -> "B", "age" -> 9),
      Map("name" -> "C", "age" -> 12),
      Map("name" -> "D", "age" -> 16),
      Map("name" -> "E", "age" -> 14),
      Map("name" -> "F", "age" -> 4)
    ))
  }

  test("ORDER BY column that isn't referenced in WITH GROUP BY") {
    // sum is not supported in compiled
    val result = executeWith(Configs.All - Configs.Compiled, "MATCH (a:A) WITH a.name AS name, a, sum(a.age) AS age ORDER BY a.foo RETURN name, age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("anon[65]"))
      .onTopOf(aPlan("Projection")
        .containingArgument("{ : a.foo}")
        .onTopOf(aPlan("EagerAggregation")
          .containingVariables("age", "name") // the introduced variables
          .containingArgument("name, a") // the group columns
        ))

    result.toList should equal(List(
      Map("name" -> "F", "age" -> 4),
      Map("name" -> "E", "age" -> 14),
      Map("name" -> "D", "age" -> 16),
      Map("name" -> "C", "age" -> 12),
      Map("name" -> "B", "age" -> 9),
      Map("name" -> "A", "age" -> 10)
    ))
  }

  test("Should fail when accessing undefined variable after WITH ORDER BY") {
    failWithError(Configs.All, "MATCH (a) WITH a.name AS n ORDER BY a.foo RETURN a.x",
                            errorType = Seq("SyntaxException"))
  }

}
