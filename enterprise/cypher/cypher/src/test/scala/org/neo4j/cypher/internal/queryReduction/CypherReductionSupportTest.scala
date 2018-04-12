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
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.runtime.InternalExecutionResult
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.ArithmeticException

import scala.util.{Failure, Success, Try}

class CypherReductionSupportTest extends CypherFunSuite with CypherReductionSupport {

  private val NL = System.lineSeparator()

  test("a simply query that cannot be reduced") {
    val query = "MATCH (n) RETURN n"
    reduceQuery(query)(_ => NotReproduced) should equal(s"MATCH (n)${NL}RETURN n AS n")
  }

  test("removes unnecessary where") {
    val setup = "CREATE (n {name: \"x\"}) RETURN n"
    val query = "MATCH (n) WHERE true RETURN n.name"
    val reduced = reduceQuery(query, Some(setup)) { (tryResults: Try[InternalExecutionResult]) =>
      tryResults match {
        case Success(result) =>
          val list = result.toList
          if(list.nonEmpty && list.head == Map("n.name" -> "x"))
            Reproduced
          else
            NotReproduced
        case Failure(_) => NotReproduced
      }
    }
    reduced should equal(s"MATCH (n)${NL}RETURN n.name AS `n.name`")
  }

  test("rolls back after each oracle invocation") {
    val query = "CREATE (n) RETURN n"
    reduceQuery(query)(_ => NotReproduced)
    evaluate("MATCH (n) RETURN count(n)").toList should be(List(Map("count(n)" -> 0)))
  }

  test("evaluate rolls back") {
    val query = "CREATE (n) RETURN n"
    evaluate(query)
    evaluate("MATCH (n) RETURN count(n)").toList should be(List(Map("count(n)" -> 0)))
  }

  test("removes unnecessary stuff from faulty query") {
    val setup = "CREATE (n:Label {name: 0}) RETURN n"
    val query = s"MATCH (n:Label)-[:X]->(m:Label),(p) WHERE 100/n.name > 34 AND m.name = n.name WITH n.name AS name RETURN name, $$a ORDER BY name SKIP 1 LIMIT 5"
    val reduced = reduceQuery(query, Some(setup)) { (tryResults: Try[InternalExecutionResult]) =>
      tryResults match {
        case Failure(e:ArithmeticException) =>
          if(e.getMessage == "/ by zero" || e.getMessage == "divide by zero")
            Reproduced
          else
            NotReproduced
        case _ => NotReproduced
      }
    }
    reduced should equal(s"MATCH (n)${NL}  WHERE 100 / n.name > 34${NL}RETURN ")
  }

  test("removes unnecessary stuff from faulty query with enterprise") {
    val setup = "CREATE (n:Label {name: 0}) RETURN n"
    val query = s"MATCH (n:Label)-[:X]->(m:Label),(p) WHERE 100/n.name > 34 AND m.name = n.name WITH n.name AS name RETURN name, $$a ORDER BY name SKIP 1 LIMIT 5"
    val reduced = reduceQuery(query, Some(setup), enterprise = true) { (tryResults: Try[InternalExecutionResult]) =>
      tryResults match {
        case Failure(e:ArithmeticException) =>
          if(e.getMessage == "/ by zero" || e.getMessage == "divide by zero")
            Reproduced
          else
            NotReproduced
        case _ => NotReproduced
      }
    }
    reduced should equal(s"MATCH (n)${NL}  WHERE 100 / n.name > 34${NL}RETURN ")
  }
}
