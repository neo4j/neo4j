/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
          if(e.getMessage == "/ by zero")
            Reproduced
          else
            NotReproduced
        case _ => NotReproduced
      }
    }
    reduced should equal(s"MATCH (n)${NL}  WHERE 100 / n.name > 34${NL}RETURN ")
  }

  test("test") {
    val setup = "UNWIND [ {name:'Chicken'},{name:'Carrot'},{name:'Butter'},{name:'Pineapple'},{name:'Ham'},{name:'Sage'} ] as row CREATE (n:Ingredient) SET n.name=row.name RETURN n.name as name, ID(n) as id UNION " +
      "UNWIND [{startName:'Chicken', endName:'Carrot', affinity:'EXCELLENT'},{startName:'Chicken', endName:'Pineapple', affinity:'GOOD'},{startName:'Pineapple', endName:'Ham', affinity:'OKISH'}," +
      "{startName:'Carrot', endName:'Butter', affinity:'GOOD'},{startName:'Butter', endName:'Sage', affinity:'EXCELLENT'}] as row MATCH (startNode:Ingredient{name:row.startName}), (endNode:Ingredient{name:row.endName}) " +
      "with row, startNode, endNode CREATE (startNode)-[rel:PAIRS_WITH]->(endNode) set rel.affinity=row.affinity RETURN '' as name, 0 as id ;"

    val query = "MATCH (chicken:Ingredient{name:'Chicken'})-[r0:PAIRS_WITH]->(carrot:Ingredient{name:'Carrot'}) WITH r0, chicken, carrot RETURN r0,chicken,[ [ (chicken)-[r_p1:PAIRS_WITH]-(i1:Ingredient) | [ r_p1, i1, [ [ (i1)-[r_p2:PAIRS_WITH]-(i2:Ingredient) | [ r_p2, i2 ] ] ] ] ] ],carrot,[ [ (carrot)-[r_p1:PAIRS_WITH]-(i1:Ingredient) | [ r_p1, i1, [ [ (i1)-[r_p2:PAIRS_WITH]-(i2:Ingredient) | [ r_p2, i2 ] ] ] ] ] ], ID(r0)"

    val reduced = reduceQueryWithCurrentQueryText(query, Some(setup)) { (tryResults: Try[(String, InternalExecutionResult)]) =>
      tryResults match {
        case Success((queryText, result)) =>
          val list = result.toList
          println(queryText)
          val list2 = evaluate("cypher runtime=slotted " + queryText).toList
          if(list != list2)
            Reproduced
          else
            NotReproduced
        case Failure(e) =>
          println(e)
          NotReproduced
      }
    }
    print(reduced)
  }
}
