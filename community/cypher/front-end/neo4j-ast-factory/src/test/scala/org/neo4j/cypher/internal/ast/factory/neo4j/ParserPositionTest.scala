/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

import scala.util.Try

class ParserPositionTest extends CypherFunSuite with TestName  {
  private val exceptionFactory = new OpenCypherExceptionFactory(None)
  private val javaCcAST = (query: String) => Try(JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator()))

  test("MATCH (n) RETURN n.prop") {
    validatePosition(testName, _.isInstanceOf[Property], InputPosition(17, 1, 18))
  }

  test("MATCH (n) SET n.prop = 1") {
    validatePosition(testName, _.isInstanceOf[SetPropertyItem], InputPosition(14, 1, 15))
  }

  test("MATCH (n) REMOVE n.prop") {
    validatePosition(testName, _.isInstanceOf[RemovePropertyItem], InputPosition(17, 1, 18))
  }

  test("LOAD CSV FROM 'url' AS line") {
    validatePosition(testName, _.isInstanceOf[LoadCSV], InputPosition(0, 1, 1))
  }

  test("USE GRAPH(x) RETURN 1 as y ") {
    validatePosition(testName, _.isInstanceOf[UseGraph], InputPosition(0, 1, 1))
  }

  test("CREATE (a)-[:X]->(b)") {
    validatePosition(testName, _.isInstanceOf[EveryPath], InputPosition(7, 1, 8))
  }

  test("SHOW ALL ROLES YIELD role") {
    validatePosition(testName, _.isInstanceOf[Yield], InputPosition(15, 1, 16))
  }

  test("RETURN 3 IN list[0] AS r") {
    validatePosition(testName, _.isInstanceOf[ContainerIndex], InputPosition(17, 1, 18))
  }

  test("RETURN 3 IN [1, 2, 3][0..1] AS r") {
    validatePosition(testName, _.isInstanceOf[ListSlice], InputPosition(21, 1, 22))
  }

  test("MATCH (a) WHERE NOT (a:A)") {
    validatePosition(testName, _.isInstanceOf[HasLabelsOrTypes], InputPosition(21, 1, 22))
  }

  test("MATCH (n) SET n += {name: null}") {
    validatePosition(testName, _.isInstanceOf[SetIncludingPropertiesFromMapItem], InputPosition(14, 1, 15))
  }

  test("MATCH (n) SET n = {name: null}") {
    validatePosition(testName, _.isInstanceOf[SetExactPropertiesFromMapItem], InputPosition(14, 1, 15))
  }

  Seq(
    ("DATABASES YIELD name", 21),
    ("DEFAULT DATABASE YIELD name", 28),
    ("HOME DATABASE YIELD name", 25),
    ("DATABASE $db YIELD name", 24),
    ("DATABASE neo4j YIELD name", 26),
  ).foreach { case (name, variableOffset) =>
    test(s"SHOW $name") {
      validatePosition(testName, _.isInstanceOf[ShowDatabase], InputPosition(0, 1, 1))
      validatePosition(testName, _.isInstanceOf[Variable], InputPosition(variableOffset, 1, variableOffset + 1))
    }
  }

  test("DROP INDEX ON :Person(name)") {
    validatePosition(testName, _.isInstanceOf[PropertyKeyName], InputPosition(22, 1, 23))
  }

  private def validatePosition(query: String, astToVerify: ASTNode => Boolean, pos: InputPosition): Unit = {
    val propAst = javaCcAST(query).treeFind[ASTNode] {
      case ast if astToVerify(ast) => true
    }

    propAst.get.position shouldBe pos
  }
}
