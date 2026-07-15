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

class ExistsExpressionSemanticAnalysisTest extends SubqueryExpressionSemanticAnalysisTest("Exists", "") {

  test("RETURN  EXISTS { MATCH (a) }") {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |WHERE EXISTS {
         |  MATCH (a)
         |  RETURN *
         |}
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |RETURN EXISTS { (a)-->(b) WHERE b.prop = 5  }
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    UNION
         |    MATCH (m)
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    UNION ALL
         |    MATCH (m)
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    RETURN n.prop
         |    UNION ALL
         |    MATCH (m)
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 73, 5, 5),
      "All sub queries in an UNION must have the same return column names",
      p(73, 5, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    UNION ALL
         |    MATCH (m)
         |    RETURN m
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 55, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      p(55, 4, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    RETURN n
         |    UNION
         |    MATCH (m)
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 68, 5, 5),
      "All sub queries in an UNION must have the same return column names",
      p(68, 5, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    UNION
         |    MATCH (m)
         |    RETURN m.prop
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 55, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      p(55, 4, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    UNION
         |    MATCH (m)
         |    RETURN m
         |    UNION
         |    MATCH (l)
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasError(
      getGql42001_42N39(Union.errorParam, 55, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      p(55, 4, 5)
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (n)
         |    RETURN n
         |    UNION
         |    MATCH (m)
         |    RETURN m
         |    UNION
         |    MATCH (l)
         |    RETURN l
         |}
         |RETURN person.name
     """.stripMargin) {
    run().hasErrors(
      SemanticError(
        getGql42001_42N39(Union.errorParam, 68, 5, 5),
        "All sub queries in an UNION must have the same return column names",
        p(68, 5, 5)
      ),
      SemanticError(
        getGql42001_42N39(Union.errorParam, 105, 8, 5),
        "All sub queries in an UNION must have the same return column names",
        p(105, 8, 5)
      )
    )
  }

  test("should raise an error on cross reference in graph pattern with exists expression") {
    run(
      """CREATE (a), (b {prop: EXISTS { MATCH (n) WHERE n.prop = a.prop } })
        |RETURN a""".stripMargin,
      disabledVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(
      SemanticError(
        getGql42001_42I58("a", 56, 1, 57),
        "Creating an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed. Only reference variables created in earlier clauses.",
        p(56, 1, 57)
      )
    )
  }

  test("should raise an error on cross reference in graph pattern without exists expression") {
    run(
      """CREATE (a), (b {prop: a.prop})
        |RETURN a""".stripMargin,
      disabledVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(
      SemanticError(
        getGql42001_42I58("a", 22, 1, 23),
        "Creating an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed. Only reference variables created in earlier clauses.",
        p(22, 1, 23)
      )
    )
  }
}
