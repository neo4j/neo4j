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

import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression

class PatternExpressionSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  // Pattern comprehensions
  test("MATCH (n) WITH [ p = (n)--(m) | p ] as paths RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n) WITH [ p = (n) | p ] as paths RETURN count(*)") {
    // this currently fails with a parse error
    run()
      // .hasErrorMessages("Path patterns in a pattern comprehension must include at least one relationship.")
      .failsWithMessageContaining("Invalid input")
  }

  test("MATCH (n) RETURN [ (n)--() | n ] as paths") {
    run().assert { result =>
      result.errors shouldBe empty
      val expression =
        result.state
          .statement().folder
          .treeFindByClass[PatternComprehension]
          .get
      expression.dependencies.map(_.name) shouldBe Set("n")
    }
  }

  // Pattern expressions
  test("MATCH (n) WHERE (n)--() RETURN count(*)") {
    run().assert { result =>
      result.errors shouldBe empty
      val expression =
        result.state
          .statement().folder
          .treeFindByClass[PatternExpression]
          .get
      expression.dependencies.map(_.name) shouldBe Set("n")
    }
  }

  // we need the property in the second pattern expression to differentiate it from a parenthesized variable
  test("MATCH (n) RETURN CASE WHEN (n)--() THEN 'Relationship' WHEN (n {prop: 42}) THEN 'Node' END") {
    run()
      // .hasErrorMessages("Path patterns in a pattern expression must include at least one relationship.")
      .hasErrorMessages("Type mismatch: expected Boolean but was Map")
  }
}
