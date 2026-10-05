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
package org.neo4j.cypher

import org.neo4j.graphdb.schema.IndexType

/**
 * Tests that ORDER BY and min/max on a suffix column of a composite index seek are satisfied by the
 * index order itself, when all prefix columns are constrained by single exact predicates and hence
 * are constant across the rows of the seek.
 */
class CompositeIndexProvidedOrderAcceptanceTest extends ExecutionEngineFunSuite {

  override def beforeEach(): Unit = {
    super.beforeEach()

    graph.withTx(tx =>
      tx.execute(
        """CREATE (:Label {prop1: 40, prop2: 5})
          |CREATE (:Label {prop1: 40, prop2: 2})
          |CREATE (:Label {prop1: 40, prop2: 3})
          |CREATE (:Label {prop1: 41, prop2: 1})
          |CREATE (:Label {prop1: 42, prop2: 4})
          |CREATE (:Label {prop1: 42, prop2: 0})
          |CREATE (:Label {prop1: 42, prop2: 9})
          |CREATE (:Label {prop1: 42})
          |CREATE (:Label {prop2: 7})
          |
          |CREATE ()-[:REL {prop1: 40, prop2: 5}]->()
          |CREATE ()-[:REL {prop1: 42, prop2: 4}]->()
          |CREATE ()-[:REL {prop1: 42, prop2: 0}]->()
          |CREATE ()-[:REL {prop1: 42, prop2: 9}]->()
          |CREATE ()-[:REL {prop1: 43, prop2: 1}]->()
        """.stripMargin
      )
    )
    graph.createNodeIndex(IndexType.RANGE, "Label", "prop1", "prop2")
    graph.createRelationshipIndex(IndexType.RANGE, "REL", "prop1", "prop2")
  }

  private val expectedProp2ValuesFor42 = Seq(0, 4, 9)

  test("should not sort when ordering by suffix of composite index seek with exact predicate on prefix") {
    Seq("ASC" -> expectedProp2ValuesFor42, "DESC" -> expectedProp2ValuesFor42.reverse).foreach {
      case (direction, expected) =>
        val result = execute(
          s"""MATCH (n:Label)
             |WHERE n.prop1 = 42 AND n.prop2 IS NOT NULL
             |RETURN n.prop2
             |ORDER BY n.prop2 $direction""".stripMargin
        )

        result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
        withClue(direction) {
          result.columnAs[Int]("n.prop2").toList should equal(expected)
        }
    }
  }

  test("should sort when ordering by suffix of composite index seek with range predicate on prefix") {
    val result = execute(
      """MATCH (n:Label)
        |WHERE n.prop1 >= 41 AND n.prop2 IS NOT NULL
        |RETURN n.prop1, n.prop2
        |ORDER BY n.prop2 ASC""".stripMargin
    )

    // The prefix is not constant, so the index order (prop1, prop2) does not satisfy ORDER BY prop2
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort")
  }

  test("should use limit for min on suffix of composite index seek with exact predicate on prefix") {
    val result = execute(
      """MATCH (n:Label)
        |WHERE n.prop1 = 42 AND n.prop2 IS NOT NULL
        |RETURN min(n.prop2)""".stripMargin
    )

    result.executionPlanDescription() should not(includeSomewhere.aPlan("Aggregation"))
    result.executionPlanDescription() should includeSomewhere.aPlan("Limit")
    result.columnAs[Int]("min(n.prop2)").toList should equal(List(0))
  }

  test("should use limit for max on suffix of composite index seek with exact predicate on prefix") {
    val result = execute(
      """MATCH (n:Label)
        |WHERE n.prop1 = 42 AND n.prop2 IS NOT NULL
        |RETURN max(n.prop2)""".stripMargin
    )

    result.executionPlanDescription() should not(includeSomewhere.aPlan("Aggregation"))
    result.executionPlanDescription() should includeSomewhere.aPlan("Limit")
    result.columnAs[Int]("max(n.prop2)").toList should equal(List(9))
  }

  test("should use composite relationship index order when ordering by suffix with exact predicate on prefix") {
    Seq("ASC" -> expectedProp2ValuesFor42, "DESC" -> expectedProp2ValuesFor42.reverse).foreach {
      case (direction, expected) =>
        val result = execute(
          s"""MATCH ()-[r:REL]->()
             |WHERE r.prop1 = 42 AND r.prop2 IS NOT NULL
             |RETURN r.prop2
             |ORDER BY r.prop2 $direction""".stripMargin
        )

        result.executionPlanDescription() should not(includeSomewhere.aPlan("Sort"))
        withClue(direction) {
          result.columnAs[Int]("r.prop2").toList should equal(expected)
        }
    }
  }

  test("should use limit for min on suffix of composite relationship index seek with exact predicate on prefix") {
    val result = execute(
      """MATCH ()-[r:REL]->()
        |WHERE r.prop1 = 42 AND r.prop2 IS NOT NULL
        |RETURN min(r.prop2)""".stripMargin
    )

    result.executionPlanDescription() should not(includeSomewhere.aPlan("Aggregation"))
    result.executionPlanDescription() should includeSomewhere.aPlan("Limit")
    result.columnAs[Int]("min(r.prop2)").toList should equal(List(0))
  }
}
