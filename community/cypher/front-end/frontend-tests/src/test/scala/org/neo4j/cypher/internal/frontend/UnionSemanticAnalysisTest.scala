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

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionSemanticAnalysisTest
    extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  test("Union's do not require the same return ordering") {
    for {
      query <- Seq(
        "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (c)-[]-(d) RETURN c as b, d as a",
        "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
        "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
        "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (c)-[]-(d) RETURN c as b, d as a",
        "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
        "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
        "RETURN COUNT { MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN b, a }"
      )
    } run(query).hasNoErrors
  }
}
