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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PatternPartTest extends CypherFunSuite with JavaccParserAstTestBase[PatternPart]
    with AstConstructionTestSupport {

  implicit val parser: JavaccRule[PatternPart] = JavaccRule.PatternPart

  test("(n)-->(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("(n)-->+(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("(n)-->*(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("(n)-->*(m) ()") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("(n)-->{0,}(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("(n)-->{,2}(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("(n)-->{1,2}(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("(n)-->{1}(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("((n)-->(m))+") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("((n)-->(m))*") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("((n)-->(m))* ()") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("((n)-->(m)){0,}") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("((n)-->(m)){,2}") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("((n)-->(m)){1,2}") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("((n)-->(m)){1}") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("SHORTEST 1 (n)-->+(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("SHORTEST 4 (n)-->*(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("ALL SHORTEST (n)-->*(m) ()") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("ALL SHORTEST (n)-->{0,}(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("ANY SHORTEST (n)-->+(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("ANY 4 (n)-->*(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("ANY (n)-->*(m) ()") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("SHORTEST 5 GROUPS (n)-->{0,}(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("(n)-[r *]->(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("(n)-[r *1..]->(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("(n)-[r *..2]->(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("(n)-[r *2]->(m)") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("((a)-[r]->(b))") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("((a)-[r]->+(b))") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe false
  }

  test("shortestPath((a)-[r]->+(b))") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("allShortestPaths((a)-[r*]->(b))") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }

  test("shortestPath((a)-[r]->(b))") {
    val m = parsing(testName)
    m.actuals.forall(_.isBounded) shouldBe true
  }
}
