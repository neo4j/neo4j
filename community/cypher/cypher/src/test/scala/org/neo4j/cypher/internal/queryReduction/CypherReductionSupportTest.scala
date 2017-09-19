/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.queryReduction.ast.ASTNodeHelper._
import org.neo4j.cypher.internal.runtime.InternalExecutionResult
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

import scala.util.{Failure, Success, Try}

class CypherReductionSupportTest extends CypherFunSuite {

  test("a simply query that cannot be reduced") {
    val query = "MATCH (n) RETURN n"
    CypherReductionSupport.reduceQuery(query)(_ => NotReproduced)
  }

  test("removes unnecessary where") {
    val setup = "CREATE (n {name: \"x\"}) RETURN n"
    val query = "MATCH (n) WHERE true RETURN n.name"
    val reduced = CypherReductionSupport.reduceQuery(query, Some(setup)) { (tryResults: Try[InternalExecutionResult]) =>
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
    forallNodes(reduced)(!_.isInstanceOf[Where]) &&
      existsNode(reduced)(_.isInstanceOf[Match]) should be(true)
  }

  test("rolls back after each oracle invocation") {
    val query = "CREATE (n) RETURN n"
    CypherReductionSupport.reduceQuery(query)(_ => NotReproduced)
    CypherReductionSupport.evaluate("MATCH (n) RETURN count(n)").toList should be(List(Map("count(n)" -> 0)))
  }

  test("evaluate rolls back") {
    val query = "CREATE (n) RETURN n"
    CypherReductionSupport.evaluate(query)
    CypherReductionSupport.evaluate("MATCH (n) RETURN count(n)").toList should be(List(Map("count(n)" -> 0)))
  }

}