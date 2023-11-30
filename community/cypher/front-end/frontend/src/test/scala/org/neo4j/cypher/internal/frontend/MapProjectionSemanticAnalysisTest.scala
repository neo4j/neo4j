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
package org.neo4j.cypher.internal.frontend

class MapProjectionSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  // Literal Map Entry on a MAP, NODE or RELATIONSHIP should work
  test("WITH {g: 1} AS l RETURN l{p: 2}") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("WITH {g: 1} AS l RETURN l{.*, p: 2}") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n) RETURN n{p: 2}") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n) RETURN n{.*, p: 2}") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r]->() RETURN r{.*, p: 2}") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r]->() RETURN r{p: 2}") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // Literal Map Entry not on a map should not work
  test("WITH [1] AS l RETURN l{p: 2}") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Type mismatch: expected Map, Node or Relationship but was List<Integer>"
    )
  }

  test("WITH [1] AS l RETURN l{.*, p: 2}") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Type mismatch: expected Map, Node or Relationship but was List<Integer>"
    )
  }

  test("WITH 2 AS l RETURN l{.*, p: 2}") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Type mismatch: expected Map, Node or Relationship but was Integer"
    )
  }

  test("WITH \"hello\" AS l RETURN l{.*, p: 2}") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Type mismatch: expected Map, Node or Relationship but was String"
    )
  }

}
