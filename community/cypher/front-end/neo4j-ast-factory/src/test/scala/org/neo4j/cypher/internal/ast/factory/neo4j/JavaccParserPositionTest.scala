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

import org.neo4j.cypher.internal.ast.HasCatalog
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.PeriodicCommitHint
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
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

import scala.util.Try

class JavaccParserPositionTest extends ParserComparisonTestBase with FunSuiteLike with TestName {
  private val exceptionFactory = new OpenCypherExceptionFactory(None)
  private val javaccAST = (query: String) => Try(JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator()))

  test("MATCH (n) RETURN n.prop") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[Property], InputPosition(17, 1, 18))
  }

  test("MATCH (n) SET n.prop = 1") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[SetPropertyItem], InputPosition(14, 1, 15))
  }

  test("MATCH (n) REMOVE n.prop") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[RemovePropertyItem], InputPosition(17, 1, 18))
  }

  test("LOAD CSV FROM 'url' AS line") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[LoadCSV], InputPosition(0, 1, 1))
  }

  test("USE GRAPH(x) RETURN 1 as y ") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[UseGraph], InputPosition(0, 1, 1))
  }

  test("CREATE (a)-[:X]->(b)") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[EveryPath], InputPosition(7, 1, 8))
  }

  test("CATALOG SHOW ALL ROLES YIELD role") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[Yield], InputPosition(23, 1, 24))
  }

  test("RETURN 3 IN list[0] AS r") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[ContainerIndex], InputPosition(17, 1, 18))
  }

  test("RETURN 3 IN [1, 2, 3][0..1] AS r") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[ListSlice], InputPosition(21, 1, 22))
  }

  test("MATCH (a) WHERE NOT (a:A)") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[HasLabelsOrTypes], InputPosition(21, 1, 22))
  }


  test("MATCH (n) WHERE exists { (n) --> () }") {
    val exists = javaccAST(testName).folder.treeFindByClass[ExistsSubClause].get
    exists.position shouldBe InputPosition(16, 1, 17)
    exists.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(25, 1, 26)
  }

  test("MATCH (n) WHERE exists { MATCH (n)-[r]->(m) }") {
    val exists = javaccAST(testName).folder.treeFindByClass[ExistsSubClause].get
    exists.position shouldBe InputPosition(16, 1, 17)
    exists.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(31, 1, 32)
  }

  test("MATCH (n) WHERE exists { MATCH (m) WHERE exists { (n)-[]->(m) } }") {
    val exists :: existsNested :: Nil = javaccAST(testName).folder.findAllByClass[ExistsSubClause].toList
    exists.position shouldBe InputPosition(16, 1, 17)
    exists.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(31, 1, 32)
    existsNested.position shouldBe InputPosition(41, 1, 42)
    existsNested.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(50, 1, 51)
  }

  test("USING PERIODIC COMMIT LOAD CSV FROM 'url' AS line RETURN line") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[SingleQuery], InputPosition(22, 1, 23))
    validatePosition(testName, _.isInstanceOf[PeriodicCommitHint], InputPosition(6, 1, 7))
  }

  test("MATCH (n) SET n += {name: null}") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[SetIncludingPropertiesFromMapItem], InputPosition(14, 1, 15))
  }

  test("MATCH (n) SET n = {name: null}") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[SetExactPropertiesFromMapItem], InputPosition(14, 1, 15))
  }

  test("CATALOG SHOW ALL ROLES") {
    assertSameAST(testName)
    validatePosition(testName, _.isInstanceOf[HasCatalog], InputPosition(8, 1, 9))
  }

  Seq(
    ("DATABASES YIELD name", 21),
    ("DEFAULT DATABASE YIELD name", 28),
    ("HOME DATABASE YIELD name", 25),
    ("DATABASE $db YIELD name", 24),
    ("DATABASE neo4j YIELD name", 26),
  ).foreach { case (name, variableOffset) =>
    test(s"SHOW $name") {
      assertSameAST(testName)
      validatePosition(testName, _.isInstanceOf[ShowDatabase], InputPosition(0, 1, 1))
      validatePosition(testName, _.isInstanceOf[Variable], InputPosition(variableOffset, 1, variableOffset + 1))
    }
  }

  test("DROP INDEX ON :Person(name)") {
    // PropertyKeyName in this AST is not the same in JavaCC and parboiled
    validatePosition(testName, _.isInstanceOf[PropertyKeyName], InputPosition(22, 1, 23))
  }

  private def validatePosition(query: String, astToVerify: (ASTNode) => Boolean, pos: InputPosition): Unit = {
    val propAst = javaccAST(query).folder.treeFind[ASTNode] {
      case ast if astToVerify(ast) => true
    }

    propAst.get.position shouldBe pos
  }
}
