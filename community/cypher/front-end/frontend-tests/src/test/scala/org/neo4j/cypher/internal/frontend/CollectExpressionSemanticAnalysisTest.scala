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
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42I58
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N22
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N39
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N71

class CollectExpressionSemanticAnalysisTest extends SubqueryExpressionSemanticAnalysisTest("Collect", " = [5]") {

  test("RETURN COLLECT { MATCH (a) }") {
    run().hasErrors(
      SemanticError(
        getGql42001_42N71(17, 1, 18),
        "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
        p(17, 1, 18)
      ),
      SemanticError(
        getGql42001_42N22(7, 1, 8),
        "A Collect Expression must end with a single return column.",
        p(7, 1, 8)
      )
    )
  }

  test(
    """MATCH (a)
      |WHERE COLLECT {
      |  MATCH (a)
      |  RETURN a.prop
      |}[0] = a
      |RETURN a
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    """MATCH (a)
      |RETURN COLLECT { MATCH (a)-->(b) WHERE b.prop = 5 RETURN b }
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    """MATCH (person:Person)
      |WHERE COLLECT {
      |    MATCH (n)
      |    UNION
      |    MATCH (m)
      |}[1] > 1
      |RETURN person.name
     """.stripMargin
  ) {
    run().hasErrors(
      getGql42001_42N71(42, 3, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(42, 3, 5),
      getGql42001_42N71(66, 5, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(66, 5, 5),
      getGql42001_42N22(28, 2, 7),
      "A Collect Expression must end with a single return column.",
      InputPosition(28, 2, 7)
    )
  }

  test(
    """MATCH (person:Person)
      |WHERE COLLECT {
      |    MATCH (n)
      |    RETURN n.prop
      |    UNION ALL
      |    MATCH (m)
      |} = [1, 2]
      |RETURN person.name
     """.stripMargin
  ) {
    run().hasErrors(
      getGql42001_42N39(Union.errorParam, 74, 5, 5),
      "All sub queries in an UNION must have the same return column names",
      InputPosition(74, 5, 5),
      getGql42001_42N71(88, 6, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(88, 6, 5),
      getGql42001_42N22(28, 2, 7),
      "A Collect Expression must end with a single return column.",
      InputPosition(28, 2, 7)
    )
  }

  test(
    """MATCH (person:Person)
      |WHERE COLLECT {
      |    MATCH (n)
      |    UNION ALL
      |    MATCH (m)
      |    RETURN m
      |} = [1, 2]
      |RETURN person.name
     """.stripMargin
  ) {
    run().hasErrors(
      getGql42001_42N39(Union.errorParam, 56, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      InputPosition(56, 4, 5),
      getGql42001_42N71(42, 3, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(42, 3, 5),
      getGql42001_42N22(28, 2, 7),
      "A Collect Expression must end with a single return column.",
      InputPosition(28, 2, 7)
    )
  }

  test(
    """MATCH (person:Person)
      |WHERE COLLECT {
      |    MATCH (n)
      |    RETURN n
      |    UNION ALL
      |    MATCH (m)
      |} = [1]
      |RETURN person.name
     """.stripMargin
  ) {
    run().hasErrors(
      getGql42001_42N39(Union.errorParam, 69, 5, 5),
      "All sub queries in an UNION must have the same return column names",
      InputPosition(69, 5, 5),
      getGql42001_42N71(83, 6, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(83, 6, 5),
      getGql42001_42N22(28, 2, 7),
      "A Collect Expression must end with a single return column.",
      InputPosition(28, 2, 7)
    )
  }

  test(
    """MATCH (person:Person)
      |WHERE COLLECT {
      |    MATCH (n)
      |    UNION ALL
      |    MATCH (m)
      |    RETURN m.prop
      |} = [1]
      |RETURN person.name
     """.stripMargin
  ) {
    run().hasErrors(
      getGql42001_42N39(Union.errorParam, 56, 4, 5),
      "All sub queries in an UNION must have the same return column names",
      InputPosition(56, 4, 5),
      getGql42001_42N71(42, 3, 5),
      "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(42, 3, 5),
      getGql42001_42N22(28, 2, 7),
      "A Collect Expression must end with a single return column.",
      InputPosition(28, 2, 7)
    )
  }

  test(
    """MATCH (person:Person)
      |WHERE COLLECT {
      |    MATCH (n)
      |    UNION ALL
      |    MATCH (m)
      |    RETURN m
      |    UNION ALL
      |    MATCH (l)
      |} = [1]
      |RETURN person.name
     """.stripMargin
  ) {
    run().hasErrors(
      SemanticError(
        getGql42001_42N39(Union.errorParam, 56, 4, 5),
        "All sub queries in an UNION must have the same return column names",
        InputPosition(56, 4, 5)
      ),
      SemanticError(
        getGql42001_42N71(42, 3, 5),
        "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
        p(42, 3, 5)
      ),
      SemanticError(
        getGql42001_42N71(111, 8, 5),
        "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
        p(111, 8, 5)
      ),
      SemanticError(
        getGql42001_42N22(28, 2, 7),
        "A Collect Expression must end with a single return column.",
        InputPosition(28, 2, 7)
      )
    )
  }

  test(
    """MATCH (person:Person)
      |WHERE COLLECT {
      |    MATCH (n)
      |    RETURN n
      |    UNION
      |    MATCH (m)
      |    RETURN m
      |    UNION
      |    MATCH (l)
      |    RETURN l
      |} = [1, 2, 3]
      |RETURN person.name
     """.stripMargin
  ) {
    run().hasErrors(
      SemanticError(
        getGql42001_42N39(Union.errorParam, 69, 5, 5),
        "All sub queries in an UNION must have the same return column names",
        p(69, 5, 5)
      ),
      SemanticError(
        getGql42001_42N39(Union.errorParam, 106, 8, 5),
        "All sub queries in an UNION must have the same return column names",
        p(106, 8, 5)
      ),
      SemanticError(
        getGql42001_42N22(28, 2, 7),
        "A Collect Expression must end with a single return column.",
        p(28, 2, 7)
      )
    )
  }

  test(
    """RETURN COLLECT {
      |  MATCH (a)
      |  RETURN *
      |}
      |""".stripMargin
  ) {
    run().hasError(
      getGql42001_42N22(7, 1, 8),
      "A Collect Expression must end with a single return column.",
      InputPosition(7, 1, 8)
    )
  }

  test(
    """RETURN COLLECT {
      |  MATCH (a)
      |  RETURN a.prop1, a.prop2
      |}
      |""".stripMargin
  ) {
    run().hasError(
      getGql42001_42N22(7, 1, 8),
      "A Collect Expression must end with a single return column.",
      InputPosition(7, 1, 8)
    )
  }

  test(
    """MATCH (a)
      |WHERE COLLECT {
      |  MATCH (a)
      |  RETURN *
      |}[0] = a
      |RETURN a
      |""".stripMargin
  ) {
    run().hasError(
      getGql42001_42N22(16, 2, 7),
      "A Collect Expression must end with a single return column.",
      InputPosition(16, 2, 7)
    )
  }

  test(
    """MATCH (a)
      |RETURN COLLECT { MATCH (m)-[r]->(p), (a)-[r2]-(c) RETURN * }
      |""".stripMargin
  ) {
    run().hasError(
      getGql42001_42N22(17, 2, 8),
      "A Collect Expression must end with a single return column.",
      InputPosition(17, 2, 8)
    )
  }

  test("should raise an error on cross reference in graph pattern with collect expression") {
    run(
      """CREATE (a), (b {prop: COLLECT { MATCH (n) WHERE n.prop = a.prop RETURN 1 } })
        |RETURN a""".stripMargin,
      disabledVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(
      SemanticError(
        getGql42001_42I58("a", 57, 1, 58),
        "Creating an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed. Only reference variables created in earlier clauses.",
        p(57, 1, 58)
      )
    )
  }

  test("should raise errors on cross reference in graph pattern with invalid collect expression") {
    // Due to the return errors, we are not computing dependencies so we cannot check them for cross-references.
    // This is OK, as long as we are failing with at least one relevant semantic error.
    run(
      """CREATE (a), (b {prop: COLLECT { MATCH (n) WHERE n.prop = a.prop } })
        |RETURN a""".stripMargin,
      disabledVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(
      SemanticError(
        getGql42001_42N22(22, 1, 23),
        "A Collect Expression must end with a single return column.",
        p(22, 1, 23)
      ),
      SemanticError(
        getGql42001_42N71(32, 1, 33),
        "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
        p(32, 1, 33)
      ),
      SemanticError(
        getGql42001_42I58("a", 57, 1, 58),
        "Creating an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed. Only reference variables created in earlier clauses.",
        p(57, 1, 58)
      )
    )
  }
}
