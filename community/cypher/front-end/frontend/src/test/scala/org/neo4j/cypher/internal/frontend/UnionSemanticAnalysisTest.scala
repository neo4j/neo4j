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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionSemanticAnalysisTest
    extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  test(
    "Union's must have same return ordering: Errors in Cypher 6"
  ) {
    val queries: Seq[(String, InputPosition)] = Seq(
      ("MATCH (a)-[]-(b) RETURN a, b UNION MATCH (c)-[]-(d) RETURN c as b, d as a", InputPosition(0, 1, 1)),
      ("RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one", InputPosition(0, 1, 1)),
      (
        "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
        InputPosition(34, 1, 35)
      ),
      ("MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (c)-[]-(d) RETURN c as b, d as a", InputPosition(0, 1, 1)),
      ("RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one", InputPosition(0, 1, 1)),
      (
        "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
        InputPosition(34, 1, 35)
      ),
      ("RETURN COUNT { MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN b, a }", InputPosition(15, 1, 16))
    )
    queries.foreach { query =>
      runSemanticAnalysisWithCypherVersion(Seq(CypherVersion.Cypher6), query._1).errors.toSet shouldEqual Set(
        SemanticError(
          "All subqueries in a UNION [ALL] must have the same ordering for the return columns.",
          query._2
        )
      )
    }
  }

  test(
    "Union's should have same return ordering: No Error in Cypher 5 (deprecation warning tested elsewhere)."
  ) {
    val queries = Seq(
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (c)-[]-(d) RETURN c as b, d as a",
      "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
      "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (c)-[]-(d) RETURN c as b, d as a",
      "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
      "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
      "RETURN COUNT { MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN b, a }"
    )
    queries.foreach { query =>
      runSemanticAnalysisWithCypherVersion(Seq(CypherVersion.Cypher5), query).errors shouldBe empty
    }
  }
}
