/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticFeature, SemanticState}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SubQueryTest extends CypherFunSuite with AstConstructionHelp {

  private val clean = SemanticState.clean.withFeature(SemanticFeature.SubQueries)

  test("should be disabled by default") {
    val sq = singleQuery(
      with_(i("1") -> v("x")),
      subQuery(
        return_(i("1") -> v("y"))
      ),
      return_(v("y") -> v("y"), v("x") -> v("x"))
    )


    val result = sq.semanticCheck(SemanticState.clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should include ("The `CALL {...}` clause is not available")
  }

  test("should expose returned variables") {
    val sq = singleQuery(
      with_(i("1") -> v("x")),
      subQuery(
        return_(i("1") -> v("y"))
      ),
      return_(v("y") -> v("y"), v("x") -> v("x"))
    )

    val result = sq.semanticCheck(clean)

    result.errors.size shouldEqual 0
  }

  test("should be uncorrelated") {
    val sq = singleQuery(
      with_(i("1") -> v("x")),
      subQuery(
        return_(v("x") -> v("y"))
      ),
      return_(v("y") -> v("y"))
    )

    val result = sq.semanticCheck(clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should include ("Variable `x` not defined")
  }

  test("should fail on variable name collision") {
    val sq = singleQuery(
      with_(i("1") -> v("x")),
      subQuery(
        return_(i("2") -> v("x"))
      ),
      return_(i("1") -> v("y"))
    )

    val result = sq.semanticCheck(clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should include ("Variable `x` already declared")
  }
}
