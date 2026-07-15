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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42I58
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N39
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N71

class CountExpressionSemanticAnalysisTest extends SubqueryExpressionSemanticAnalysisTest("Count", " > 1") {

  test("RETURN  COUNT { MATCH (a) }") {
    run().hasNoErrors
  }

  test(
    """MATCH (a)
      |WHERE COUNT {
      |  MATCH (a)
      |  RETURN *
      |} > 3
      |RETURN a
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |RETURN COUNT { (a)-->(b) WHERE b.prop = 5  }
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    UNION ALL
         |    MATCH (m)
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    UNION
         |    MATCH (m)
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    run().hasErrors(
      getGql42001_42N71(40, 3, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(40, 3, 5),
      getGql42001_42N71(64, 5, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(64, 5, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    RETURN n.prop
         |    UNION ALL
         |    MATCH (m)
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 72, 5, 5),
      "All sub queries in an UNION must have the same return column names",
      p(72, 5, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    UNION ALL
         |    MATCH (m)
         |    RETURN m
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 54, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      p(54, 4, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    RETURN n
         |    UNION ALL
         |    MATCH (m)
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 67, 5, 5),
      "All sub queries in an UNION must have the same return column names",
      p(67, 5, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    UNION ALL
         |    MATCH (m)
         |    RETURN m.prop
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 54, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      p(54, 4, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    UNION ALL
         |    MATCH (m)
         |    RETURN m
         |    UNION ALL
         |    MATCH (l)
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 54, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      p(54, 4, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    RETURN n
         |    UNION
         |    MATCH (m)
         |    RETURN m
         |    UNION
         |    MATCH (l)
         |    RETURN l
         |} > 5
         |RETURN person.name
     """.stripMargin) {
    run().hasErrors(
      SemanticError(
        getGql42001_42N39(Union.errorParam, 67, 5, 5),
        "All sub queries in an UNION must have the same return column names",
        p(67, 5, 5)
      ),
      SemanticError(
        getGql42001_42N39(Union.errorParam, 104, 8, 5),
        "All sub queries in an UNION must have the same return column names",
        p(104, 8, 5)
      )
    )
  }

  test("should raise an error on cross reference in graph pattern with count expression") {
    run(
      """CREATE (a), (b {prop: COUNT { MATCH (n) WHERE n.prop = a.prop } })
        |RETURN a""".stripMargin,
      disabledVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(
      SemanticError(
        getGql42001_42I58("a", 55, 1, 56),
        "Creating an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed. Only reference variables created in earlier clauses.",
        p(55, 1, 56)
      )
    )
  }
}
