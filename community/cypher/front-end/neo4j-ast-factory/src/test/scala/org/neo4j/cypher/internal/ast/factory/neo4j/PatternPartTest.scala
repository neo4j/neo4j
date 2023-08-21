/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PatternPartTest extends CypherFunSuite with ParserSyntaxTreeBase[Cst.Pattern, PatternPart]
    with AstConstructionTestSupport {

  implicit val javaccRule: JavaccRule[PatternPart] = JavaccRule.PatternPart
  implicit val antlrRule: AntlrRule[Cst.Pattern] = AntlrRule.PatternPart

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
