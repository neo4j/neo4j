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
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class OrderAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  override def beforeEach(): Unit = {
    super.beforeEach()
    graph.execute(
      """
      CREATE (:A {age: 10, name: 'A', foo: 6})
      CREATE (:A {age: 9, name: 'B', foo: 5})
      CREATE (:A {age: 12, name: 'C', foo: 4})
      CREATE (:A {age: 16, name: 'D', foo: 3})
      CREATE (:A {age: 14, name: 'E', foo: 2})
      CREATE (:A {age: 4, name: 'F', foo: 1})
      """)
  }

  test("ORDER BY previously unprojected column in WITH") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a
      ORDER BY a.age
      RETURN a.name
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
        ))
  }

  test("ORDER BY previously unprojected column in WITH and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a
      ORDER BY a.age
      RETURN a.name, a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name, a.age : a.age}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
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
        .containingArgument("age")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{age : a.age}")
        ))
  }

  test("ORDER BY renamed column old name in WITH and project and return that column") {
    // 3.1 and older is buggy here
    val result = executeWith(Configs.All - Configs.Before3_3AndRule,
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
        .containingArgument("b")
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : a}")
        ))
  }

  test("ORDER BY renamed column new name in WITH and project and return that column") {
    // 3.1 and older is buggy here
    val result = executeWith(Configs.All - Configs.Before3_3AndRule,
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
        .containingArgument("b")
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : a}")
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
  }

  test("ORDER BY previously unprojected column in RETURN") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN a.name
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{ : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
        ))
  }

  // Does not regress but has some awkward Projections we could get rid of
  test("ORDER BY previously unprojected column in RETURN and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN a.name, a.age
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{ : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
        ))
  }

  // Does not regress but has some awkward Projections we could get rid of
  test("ORDER BY previously unprojected column in RETURN and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN a.name, a.age AS age
      ORDER BY age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{ : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{ : a.age}")
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
        .containingArgument("{ : a.age}")
      )
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
        .containingArgument("{ : a.age}")
      )
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
      .containingArgument("age")
      .onTopOf(aPlan("Distinct")
        .containingVariables("age")
        .containingArgument("age")
      )
  }

  test("ORDER BY column that isn't referenced in WITH DISTINCT") {
    val result = executeWith(Configs.All,
      """
        MATCH (a:A)
        WITH DISTINCT a.name AS name, a
        ORDER BY a.age
        RETURN name
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingArgument("{ : a.age}")
        .onTopOf(aPlan("Distinct")
          .containingVariables("name", "a")
          .containingArgument("name, a")
        )
      )
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
      .containingArgument("age")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("name") // the group column
      )
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
      .containingArgument("name")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("name") // the group column
      )
  }

  test("ORDER BY column that isn't referenced in WITH GROUP BY") {
    // sum is not supported in compiled
    val result = executeWith(Configs.All - Configs.Compiled,
      """
      MATCH (a:A)
      WITH a.name AS name, a, sum(a.age) AS age
      ORDER BY a.foo
      RETURN name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingArgument("{ : a.foo}")
        .onTopOf(aPlan("EagerAggregation")
          .containingVariables("age", "name") // the introduced variables
          .containingArgument("name, a") // the group columns
        ))
  }

  test("Should fail when accessing undefinded variable after WITH ORDER BY") {
    failWithError(Configs.AbsolutelyAll, "MATCH (a) WITH a.name AS n ORDER BY a.foo RETURN a.x",
                            errorType = Seq("SyntaxException"))
  }

}
