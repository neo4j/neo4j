/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CountExpressionSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  test("""MATCH (a)
         |RETURN COUNT { CREATE (b) } > 1
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "A Count Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (m)
         |WHERE COUNT { OPTIONAL MATCH (a)-[r]->(b) } > 1
         |RETURN m
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
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
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""MATCH (m)
         |WHERE COUNT { MATCH (a:A)-[r]->(b) USING SCAN a:A } > 1
         |RETURN m
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""MATCH (a)
         |RETURN COUNT { SET a.name = 1 } > 1
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "A Count Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (a)
         |RETURN COUNT { MATCH (b) WHERE b.a = a.a DETACH DELETE b } > 1
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "A Count Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (a)
         |RETURN COUNT { MATCH (b) MERGE (b)-[:FOLLOWS]->(:Person) } > 1
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "A Count Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (a)
         |RETURN COUNT { CALL db.labels() } > 1
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""MATCH (a)
         |RETURN COUNT {
         |   MATCH (a)-[:KNOWS]->(b)
         |   RETURN b.name as name
         |   UNION ALL
         |   MATCH (a)-[:LOVES]->(b)
         |   RETURN b.name as name
         |}""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""MATCH (a)
         |RETURN COUNT { MATCH (m)-[r]->(p), (a)-[r2]-(c) }
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""MATCH (a)
         |RETURN COUNT { (a)-->(b) WHERE b.prop = 5  }
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""WITH 5 as aNum
         |MATCH (a)
         |RETURN COUNT {
         |  WITH 6 as aNum
         |  MATCH (a)-->(b) WHERE b.prop = aNum
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `aNum` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(52, 4, 13)
      )
    )
  }

  test("""WITH 5 as aNum
         |MATCH (a)
         |RETURN COUNT {
         |  MATCH (a)-->(b) WHERE b.prop = aNum
         |  WITH 6 as aNum
         |  MATCH (b)-->(c) WHERE c.prop = aNum
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `aNum` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(90, 5, 13)
      )
    )
  }

  test("""MATCH (a)
         |RETURN COUNT {
         |  MATCH (a)-->(b)
         |  WITH b as a
         |  MATCH (b)-->(c)
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(55, 4, 13)
      )
    )
  }

  test(
    """MATCH (a)
      |RETURN COUNT {
      |  MATCH (b)
      |  RETURN b AS a
      |  UNION
      |  MATCH (a)
      |  RETURN a
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(51, 4, 15)
      )
    )
  }

  test(
    """MATCH (a)
      |RETURN COUNT {
      |  MATCH (a)
      |  RETURN a
      |  UNION ALL
      |  MATCH ()-->(a)
      |  RETURN a
      |  UNION ALL
      |  MATCH (b)
      |  RETURN b AS a
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(126, 10, 15)
      )
    )
  }

  test("""MATCH (a)
         |RETURN COUNT {
         |  MATCH (a)-->(b)
         |  WITH b as c
         |  MATCH (c)-->(d)
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test(
    """MATCH (a)
      |RETURN COUNT {
      |  MATCH (a)
      |  RETURN a
      |  UNION
      |  MATCH (a)
      |  RETURN a
      |  UNION
      |  MATCH (a)
      |  RETURN a
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test(
    """MATCH (a)
      |RETURN COUNT {
      |  MATCH (a)
      |  RETURN a
      |  UNION ALL
      |  MATCH (a)
      |  RETURN a
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    RETURN CASE
         |       WHEN true THEN 1
         |       ELSE 2
         |    END
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    UNION ALL
         |    MATCH (m)
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         |    MATCH (n)
         |    UNION
         |    MATCH (m)
         |} > 1
         |RETURN person.name
     """.stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "Query cannot conclude with MATCH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
        InputPosition(40, 3, 5)
      ),
      SemanticError(
        "Query cannot conclude with MATCH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
        InputPosition(64, 5, 5)
      )
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
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "All sub queries in an UNION must have the same return column names",
        InputPosition(72, 5, 5)
      )
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
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "All sub queries in an UNION must have the same return column names",
        InputPosition(54, 4, 5)
      )
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
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "All sub queries in an UNION must have the same return column names",
        InputPosition(67, 5, 5)
      )
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
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "All sub queries in an UNION must have the same return column names",
        InputPosition(54, 4, 5)
      )
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
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "All sub queries in an UNION must have the same return column names",
        InputPosition(54, 4, 5)
      )
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
         |} > 3
         |RETURN person.name
     """.stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "All sub queries in an UNION must have the same return column names",
        InputPosition(67, 5, 5)
      ),
      SemanticError(
        "All sub queries in an UNION must have the same return column names",
        InputPosition(104, 8, 5)
      )
    )
  }
}
