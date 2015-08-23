/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.semantics.v2_3.InputPosition
import org.neo4j.cypher.internal.semantics.v2_3.notification._

class NotificationAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Warn for cartesian product") {
    val result = executeWithAllPlannersAndRuntimes("explain match (a)-->(b), (c)-->(d) return *")

    result.notifications.toList should equal(List(CartesianProductNotification(InputPosition(0, 1, 1), Set("c", "d"))))
  }

  test("Warn for cartesian product with runtime=compiled") {
    val result = innerExecute("explain cypher runtime=compiled match (a)-->(b), (c)-->(d) return count(*)")

    result.notifications.toList should equal(List(CartesianProductNotification(InputPosition(0, 1, 1), Set("c", "d")),
                                                  RuntimeUnsupportedNotification))
  }

  test("Warn for cartesian product with runtime=interpreted") {
    val result = executeWithAllPlanners("explain cypher runtime=interpreted match (a)-->(b), (c)-->(d) return *")

    result.notifications.toList should equal(List(CartesianProductNotification(InputPosition(0, 1, 1), Set("c", "d"))))
  }

  test("Don't warn for cartesian product when not using explain") {
    val result = executeWithAllPlannersAndRuntimes("match (a)-->(b), (c)-->(d) return *")

    result.notifications shouldBe empty
  }

  test("warn when using length on collection") {
    val result = executeWithAllPlanners("explain return length([1, 2, 3])")

    result.notifications should equal(Set(LengthOnNonPathNotification(InputPosition(14, 1, 15))))
  }

  test("do not warn when using length on a path") {
    val result = executeWithAllPlanners("explain match p=(a)-[*]->(b) return length(p)")

    result.notifications shouldBe empty
  }

  test("do warn when using length on a pattern expression") {
    val result = executeWithAllPlanners("explain match (a) where a.name='Alice' return length((a)-->()-->())")

    result.notifications should equal(Set(LengthOnNonPathNotification(InputPosition(45, 1, 46))))
  }

  test("do warn when using length on a string") {
    val result = executeWithAllPlanners("explain return length('a string')")

    result.notifications should equal(Set(LengthOnNonPathNotification(InputPosition(14, 1, 15))))
  }

  test("do not warn when using size on a collection") {
    val result = executeWithAllPlanners("explain return size([1, 2, 3])")
    result.notifications shouldBe empty
  }

  test("do not warn when using size on a string") {
    val result = executeWithAllPlanners("explain return size('a string')")
    result.notifications shouldBe empty
  }

  test("do not warn for cost unsupported on update query if planner not explicitly requested") {
    val result = innerExecute("EXPLAIN MATCH (n:Movie) SET n.title = 'The Movie'")
    result.notifications shouldBe empty
  }

  test("warn when requesting COST on an update query") {
    val result = innerExecute("EXPLAIN CYPHER planner=COST MATCH (n:Movie) SET n.title = 'The Movie'")
    result.notifications should equal(Set(PlannerUnsupportedNotification))
  }

  test("do not warn when requesting RULE on an update query") {
    val result = innerExecute("EXPLAIN CYPHER planner=RULE MATCH (n:Movie) SET n.title = 'The Movie'")
    result.notifications shouldBe empty
  }

  test("warn when requesting runtime=compiled on an unsupported query") {
    val result = innerExecute("EXPLAIN CYPHER runtime=compiled MATCH (a)-->(b), (c)-->(d) RETURN count(*)")
    result.notifications should contain(RuntimeUnsupportedNotification)
  }

  test("warn once when a single index hint cannot be fulfilled") {
    val result = innerExecute("EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
    result.notifications.toSet should equal(Set(IndexHintUnfulfillableNotification("Person", "name")))
  }

  test("warn for each unfulfillable index hint") {
    val result = innerExecute(
      """EXPLAIN MATCH (n:Person), (m:Party), (k:Animal)
        |USING INDEX n:Person(name)
        |USING INDEX m:Party(city)
        |USING INDEX k:Animal(species)
        |WHERE n.name = 'John' AND m.city = 'Reykjavik' AND k.species = 'Sloth'
        |RETURN n""".stripMargin)

    result.notifications should contain(IndexHintUnfulfillableNotification("Person", "name"))
    result.notifications should contain(IndexHintUnfulfillableNotification("Party", "city"))
    result.notifications should contain(IndexHintUnfulfillableNotification("Animal", "species"))
  }

  test("warn for bare node pattern") {
    val result = innerExecute("EXPLAIN MATCH n-->(m) RETURN n, m")
    result.notifications.toSet should equal(Set(BareNodeSyntaxDeprecatedNotification(InputPosition(6, 1, 7))))
  }

  test("should warn when join hint is used with RULE planner with EXPLAIN") {
    val result = innerExecute( """CYPHER planner=rule EXPLAIN MATCH (a)-->(b) USING JOIN ON b RETURN a, b""")

    result.notifications should equal(Set(JoinHintUnsupportedNotification(Seq("b"))))
  }

  test("should not warn when join hint is used with RULE planner without EXPLAIN") {
    val result = innerExecute( """CYPHER planner=rule MATCH (a)-->(b) USING JOIN ON b RETURN a, b""")

    result.notifications shouldBe empty
  }
}
