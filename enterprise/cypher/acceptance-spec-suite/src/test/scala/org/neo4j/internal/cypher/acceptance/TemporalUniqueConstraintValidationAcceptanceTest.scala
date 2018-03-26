/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_4.helpers.ListSupport
import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class TemporalUniqueConstraintValidationAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with ListSupport {

  case class TemporalOperator(operator: String, funcString: String, resultString: String)

  List(
    TemporalOperator("Duration", "duration('P2018Y3M30DT10H10M10S')", "P2018Y3M30DT10H10M10S"),
    TemporalOperator("DateTime", "datetime('2018-03-30T10:10:10+01:00')", "2018-03-30T10:10:10+01:00"),
    TemporalOperator("LocalDateTime", "localdatetime('2018-03-30T10:10:10')", "2018-03-30T10:10:10"),
    TemporalOperator("Date", "date('2018-03-30')", "2018-03-30"),
    TemporalOperator("Time", "time('10:10:10+01:00')", "10:10:10+01:00"),
    TemporalOperator("LocalTime", "localtime('10:10:10')", "10:10:10")
  ).foreach { op =>
    testTemporalOperator(op)
  }

  def testTemporalOperator(op: TemporalOperator) {
    test(s"[${op.operator}] should enforce uniqueness constraint on create node with label and property") {
      // GIVEN
      execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
      execute(s"CREATE ( node:Label1 { key1: ${op.funcString} } )")

      // WHEN
      val exception = intercept[CypherExecutionException] {
        execute(s"CREATE ( node:Label1 { key1: ${op.funcString} } )")
      }
      exception.getMessage should include(s"`key1` = ${op.resultString}")
    }

    test(s"[${op.operator}] should enforce uniqueness constraint on set property") {
      // GIVEN
      execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
      execute(s"CREATE ( node1:Label1 { seq: 1, key1: ${op.funcString} } ), ( node2:Label1 { seq: 2 } )")

      // WHEN
      val exception = intercept[CypherExecutionException] {
        execute(s"MATCH (node2:Label1) WHERE node2.seq = 2 SET node2.key1 = ${op.funcString}")
      }
      exception.getMessage should include(s"`key1` = ${op.resultString}")
    }

    test(s"[${op.operator}] should enforce uniqueness constraint on add label") {
      // GIVEN
      execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
      execute(s"CREATE ( node1:Label1 { seq: 1, key1: ${op.funcString} } ), ( node2 { seq: 2, key1: ${op.funcString} } )")

      // WHEN
      val exception = intercept[CypherExecutionException] {
        execute("MATCH (node2) WHERE node2.seq = 2 SET node2:Label1")
      }
      exception.getMessage should include(s"`key1` = ${op.resultString}")
    }

    test(s"[${op.operator}] should enforce uniqueness constraint on conflicting data in same statement") {
      // GIVEN
      execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")

      // WHEN
      val exception = intercept[CypherExecutionException] {
        execute(s"CREATE ( node1:Label1 { key1: ${op.funcString} } ), ( node2:Label1 { key1: ${op.funcString} } )")
      }
      exception.getMessage should include(s"`key1` = ${op.resultString}")
    }

    test(s"[${op.operator}] should allow remove and add conflicting data in one statement") {
      // GIVEN
      execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
      execute(s"CREATE ( node:Label1 { seq:1, key1: ${op.funcString} } )")

      var seq = 2
      for (resolve <- List("DELETE toRemove", "REMOVE toRemove.key1", "REMOVE toRemove:Label1", "SET toRemove.key1 = datetime('2018-03-30T10:10:10+02:00')")) {
        // WHEN
        execute(s"MATCH (toRemove:Label1 {key1: ${op.funcString}}) $resolve CREATE ( toAdd:Label1 { seq: {seq}, key1: ${op.funcString} } )", "seq" -> seq)

        // THEN
        val result = execute(s"MATCH (n:Label1) WHERE n.key1 =  ${op.funcString} RETURN n.seq AS seq")
        result.columnAs[Int]("seq").toList should equal(List(seq))
        seq += 1
      }
    }
  }

  test("should allow creation of non conflicting data") {
    // GIVEN
    execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
    execute(s"CREATE ( node:Label1 { key1: datetime('2018-03-30T10:10:10+01:00') } )")
    execute("CREATE ( node:Label1 { key1: datetime('2018-03-30T10:10:10.1+01:00') } )")
    execute("CREATE ( node:Label1 { key1: datetime('2018-03-30T10:10:10+02:00') } )")
    execute("CREATE ( node:Label1 { key1: datetime('2018-03-29T10:10:10+01:00') } )")

    execute(s"CREATE ( node:Label1 { key1: localdatetime('2018-03-30T10:10:10') } )")
    execute(s"CREATE ( node:Label1 { key1: date('2018-03-30') } )")
    execute(s"CREATE ( node:Label1 { key1: time('10:10:10+01:00') } )")
    execute(s"CREATE ( node:Label1 { key1: localtime('10:10:10') } )")

    // WHEN
    (0 to 12).foreach { h =>
      // Other timezones
      execute(f"CREATE ( node { key1: datetime('2018-03-30T10:10:10+$h%02d:00') } )")
      execute(f"CREATE ( node:Label2 { key1: datetime('2018-03-30T10:10:10+$h%02d:00') } )")
      execute(f"CREATE ( node:Label1 { key1: datetime('2018-03-30T10:10:10+$h%02d:30') } )")
      execute(f"CREATE ( node:Label1 { key2: datetime('2018-03-30T10:10:10+$h%02d:00') } )")
      execute(f"CREATE ( node:Label1 { key1: datetime('2018-04-30T10:10:10+$h%02d:00') } )")
      execute(f"CREATE ( node:Label1 { key1: datetime('2019-03-30T10:10:10+$h%02d:00') } )")

      // Other hours
      execute(s"CREATE ( node:Label1 { key1: datetime('2018-03-30T${h + 11}:10:10') } )")
      execute(s"CREATE ( node:Label1 { key1: localdatetime('2018-03-30T${h + 11}:10:10') } )")
      execute(s"CREATE ( node:Label1 { key1: time('${h + 11}:10:10+01:00') } )")
      execute(s"CREATE ( node:Label1 { key1: localtime('${h + 11}:10:10') } )")

      // Other days
      execute(s"CREATE ( node:Label1 { key1: datetime('2018-03-${h + 1}T10:10:10') } )")
      execute(s"CREATE ( node:Label1 { key1: localdatetime('2018-03-${h + 1}T10:10:10') } )")
      execute(s"CREATE ( node:Label1 { key1: date('2018-03-${h + 1}') } )")
    }

    // THEN
    val result = execute("MATCH (n) RETURN count(*) AS nodeCount")
    result.columnAs[Int]("nodeCount").toList should equal(List(177))
  }
}
