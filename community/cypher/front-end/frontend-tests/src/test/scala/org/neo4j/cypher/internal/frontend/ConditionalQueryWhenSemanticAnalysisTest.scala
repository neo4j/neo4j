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
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ConditionalQueryWhenSemanticAnalysisTest
    extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  def run(query: String): AnalysisAssertions = super.run(query, disabledVersions = Set(CypherVersion.Cypher5))

  test("WHEN predicate type mismatch") {
    for {
      (query, actual, pos) <- Seq(
        ("WHEN 1 THEN RETURN 1 AS x", "Integer", p(5, 1, 6).withInputLength(1)),
        ("WHEN 1.0 THEN RETURN 1 AS x", "Float", p(5, 1, 6).withInputLength(3)),
        ("WHEN {key: 'Value'} THEN RETURN 1 AS x", "Map", p(5, 1, 6)),
        (
          "WHEN point({latitude: toFloat('13.43'), longitude: toFloat('56.21')}) THEN RETURN 1 AS x",
          "Point",
          p(5, 1, 6)
        ),
        ("WHEN 'abc' THEN RETURN 1 AS x", "String", InputPosition.Range(5, 1, 6, 5))
      )
    } yield run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        val msg = s"Type mismatch: expected Boolean but was $actual"
        Seq(SemanticError.typeMismatch(List("BOOLEAN"), actual.toUpperCase, msg, pos))
    }
  }

  test("WHEN predicate invalid entity type") {
    for {
      (query, actual, pos) <- Seq(
        ("WITH 1 AS a CALL (a) { WHEN a THEN RETURN 1 AS x } RETURN x", "Integer", p(28, 1, 29)),
        (
          "WITH 'abc' AS a CALL (a) { WHEN a THEN RETURN 1 AS x } RETURN x",
          "String",
          p(32, 1, 33)
        ),
        ("MATCH (a)-[r]-(m) CALL (a) { WHEN a THEN RETURN 1 AS x } RETURN x", "Node", p(34, 1, 35)),
        ("MATCH (n)-[a]-(m) CALL (a) { WHEN a THEN RETURN 1 AS x } RETURN x", "Relationship", p(34, 1, 35)),
        (
          "MATCH a = ()-[:ACTED_IN]->(movie:Movie) CALL (a) { WHEN a THEN RETURN 1 AS x } RETURN x",
          "Path",
          p(56, 1, 57)
        )
      )
    } yield run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        val msg = s"Type mismatch: expected Boolean but was $actual"
        Seq(SemanticError.invalidEntityType(actual.toUpperCase, "`a`", List("BOOLEAN"), msg, pos))
    }
  }

  test("Predicate expression cannot be an aggregating expression") {
    val query = "WHEN MAX($actor.age) = 1 THEN RETURN 1 AS x"
    val pos = p(5, 1, 6)
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(
          SemanticError.aggregateExpressionsNotAllowedInSimpleExpressions("MAX($actor.age)", "max", pos)
        )
    }
  }

  test("Predicate expression in later branch cannot be an aggregating expression") {
    val query =
      """
        |     WHEN false THEN RETURN 1 AS x
        |     WHEN false THEN RETURN 2 AS x
        |     WHEN false THEN RETURN 3 AS x
        |     WHEN sum(1) > 0 THEN RETURN 4 AS x
        |""".stripMargin
    val pos = p(116, 5, 11)
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(
          SemanticError.aggregateExpressionsNotAllowedInSimpleExpressions("sum(1)", "sum", pos)
        )
    }
  }

  test("Predicate expression in later branch and else cannot be an aggregating expression") {
    val query =
      """
        |     WHEN false THEN RETURN 1 AS x
        |     WHEN false THEN RETURN 2 AS x
        |     WHEN false THEN RETURN 3 AS x
        |     WHEN sum(1) > 0 THEN RETURN 4 AS x
        |     ELSE RETURN 5 AS x
        |""".stripMargin
    val pos = p(116, 5, 11)
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(
          SemanticError.aggregateExpressionsNotAllowedInSimpleExpressions("sum(1)", "sum", pos)
        )
    }
  }

  test("Multiple non boolean predicates") {
    val query =
      """WHEN 1 THEN RETURN 1 AS x
        |WHEN "str" THEN RETURN 2 AS x
        |WHEN 1 + 1 THEN RETURN 3 AS x
        |WHEN COUNT { RETURN 1 AS x } THEN RETURN 4 AS x
        |ELSE RETURN 5 AS x
        |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(
          SemanticError.typeMismatch(
            List("BOOLEAN"),
            "INTEGER",
            "Type mismatch: expected Boolean but was Integer",
            p(5, 1, 6).withInputLength(1)
          ),
          SemanticError.typeMismatch(
            List("BOOLEAN"),
            "STRING",
            "Type mismatch: expected Boolean but was String",
            InputPosition.Range(31, 2, 6, 5)
          ),
          SemanticError.typeMismatch(
            List("BOOLEAN"),
            "INTEGER",
            "Type mismatch: expected Boolean but was Integer",
            p(63, 3, 8)
          ),
          SemanticError.typeMismatch(
            List("BOOLEAN"),
            "INTEGER",
            "Type mismatch: expected Boolean but was Integer",
            p(91, 4, 6)
          )
        )
    }
  }

  test("Error when wrapped in importing with subquery") {
    val query = """CALL {
                  |   WHEN true THEN RETURN 1 AS n
                  |   WHEN false THEN RETURN 1 AS n
                  |   ELSE RETURN 1 AS n
                  |}
                  |FINISH""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.invalidUseOfOldCall(ConditionalQueryWhen.msg, p(10, 2, 4)))
    }
  }

  test("All clauses be either Unit or Returning") {
    val query = """WHEN true THEN RETURN 1 AS n
                  |WHEN false THEN FINISH
                  |ELSE RETURN 1 AS n""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleSubqueryType(ConditionalQueryWhen.name, p(45, 2, 17)))
    }
  }

  test("Wrapped all clauses be either Unit or Returning") {
    val query = """WHEN true THEN { RETURN 1 AS n }
                  |WHEN false THEN { FINISH }
                  |ELSE { RETURN 1 AS n}""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleSubqueryType(ConditionalQueryWhen.name, p(49, 2, 17)))
    }
  }

  test("All clauses be either Unit or Returning - non conformer in else") {
    val query = """WHEN true THEN { RETURN 1 AS n }
                  |ELSE { FINISH }""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleSubqueryType(ConditionalQueryWhen.name, p(38, 2, 6)))
    }
  }

  test("Wrapped different order of returns allowed") {
    val query = """WHEN true THEN { RETURN 1 AS n, 1 AS m }
                  |ELSE { RETURN 1 AS m, 1 AS n }
                  |""".stripMargin
    run(query).hasNoErrors
  }

  test("Wrapped different number of returns not allowed") {
    val query = """WHEN true THEN { RETURN 1 AS n, 1 AS m }
                  |ELSE { RETURN 1 AS m }
                  |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleNumberOfReturnColumns(ConditionalQueryWhen.name, p(41, 2, 1)))
    }
  }

  test("Different number of returns not allowed") {
    val query = """WHEN true THEN RETURN 1 AS n, 1 AS m
                  |ELSE RETURN 1 AS m
                  |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleNumberOfReturnColumns(ConditionalQueryWhen.name, p(37, 2, 1)))
    }
  }

  test("Different number of returns not allowed with return all") {
    val query = """WHEN true THEN RETURN 1 AS n, 1 AS m
                  |ELSE WITH 1 AS m RETURN *
                  |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleNumberOfReturnColumns(ConditionalQueryWhen.name, p(37, 2, 1)))
    }
  }

  test("Same number of returns allowed with return all") {
    val query = """WHEN true THEN RETURN 1 AS n, 1 AS m
                  |ELSE WITH 1 AS m, 1 AS n RETURN *
                  |""".stripMargin
    run(query).hasNoErrors
  }

  test("Only taking into account first level of returns") {
    val query = """WHEN true THEN CALL () { RETURN 1 AS notAProblem } RETURN 1 AS n, 1 AS m
                  |ELSE RETURN 1 AS n, 1 AS m
                  |""".stripMargin
    run(query).hasNoErrors
  }

  test("Names of returned columns must agree") {
    val query = """WHEN true THEN RETURN 1 AS m
                  |ELSE RETURN 1 AS n
                  |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleWhenReturnColumns(ConditionalQueryWhen.name, p(29, 2, 1)))
    }
  }

  test("Wrapped Names of returned columns must agree") {
    val query = """WHEN true THEN { RETURN 1 AS m }
                  |ELSE { RETURN 1 AS n }
                  |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.incompatibleWhenReturnColumns(ConditionalQueryWhen.name, p(33, 2, 1)))
    }
  }

  test("Should fail on non existing variable") {
    val query =
      """WITH 1 AS x
        |RETURN EXISTS {
        |   WHEN x < 0 THEN RETURN 1 + x AS y
        |   WHEN b < 1 THEN RETURN 2 AS y
        |   ELSE RETURN 3 + x AS y
        |} AS res
        |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(SemanticError.variableNotDefined("b", p(73, 4, 9)))
    }
  }

  test("Returning variables of a different data type is allowed") {
    val query = """WHEN true THEN { RETURN 1 AS n }
                  |ELSE { RETURN true AS n }
                  |""".stripMargin
    run(query).hasNoErrors
  }

  test("Should import all variables from outer scope in subquery expression") {
    val query =
      """
        |     WITH 1 AS x, 2 AS b
        |     RETURN EXISTS {
        |        WHEN x < 0 THEN RETURN 1 + x AS y
        |        WHEN b < 1 THEN RETURN 2 AS y
        |        ELSE RETURN 3 + x AS y
        |     } AS res
        |""".stripMargin
    run(query).hasNoErrors
  }
}
