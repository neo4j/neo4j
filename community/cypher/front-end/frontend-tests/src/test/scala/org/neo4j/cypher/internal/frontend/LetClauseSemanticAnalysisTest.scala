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
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.GqlHelper

import scala.util.Failure
import scala.util.Success

class LetClauseSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  /*
   * positive tests
   */
  val validLets = Seq(
    "LET a = 1 RETURN a",
    "LET a = true RETURN a",
    "LET a = null RETURN a",
    "LET a = 1, b = 2 RETURN a, b",
    "LET DISTINCT = 1 RETURN *",
    "LET LET = 1 RETURN *",
    "LET AS = 1 RETURN *",
    "LET `*, a` = 1 RETURN *",
    """UNWIND [1, 2] AS i
      |LET a = 1, b = 2
      |RETURN i, a, b
      |""".stripMargin,
    """FILTER false
      |LET a = 1, b = 2
      |RETURN a, b
      |""".stripMargin,
    """UNWIND [5, 9, 23, 42, 64] AS i
      |LET odd = i % 2 = 1
      |LET even = NOT odd
      |LET square = ceil(sqrt(i))^2 = i
      |LET squareOfEven = ceil(sqrt(i)) % 2 = 0
      |LET evenSquareOfEven = even AND squareOfEven
      |RETURN i, odd, even, square, squareOfEven, evenSquareOfEven
      |""".stripMargin,
    """MATCH (a)
      |LET hasNeighbor = EXISTS { (a)--() }
      |RETURN a, hasNeighbor
      |""".stripMargin
  ) ++ (
    /*
     * Valid use of aggregating function in scalar subquery in this context
     */
    for {
      scalar <- Seq("EXISTS", "COUNT", "COLLECT")
      (agg, args) <- Seq(
        ("collect", "a"),
        ("count", "*"),
        ("count", "a"),
        ("sum", "a"),
        ("min", "a"),
        ("max", "a"),
        ("avg", "a"),
        ("stDev", "a"),
        ("stDevP", "a"),
        ("percentileCont", "a, 0.5"),
        ("percentileDisc", "a, 0.5")
      )
    } yield s"LET x = $scalar { UNWIND [1.1, 2.1, 3.1] AS a RETURN $agg($args) } RETURN x"
  )

  for {
    validLet <- validLets
  } {
    test(validLet) {
      run().assertTryIn {
        case CypherVersion.Cypher5 => {
          case Success(_) => fail(new Exception("LET is not part of Cypher 5 syntax"))
          case Failure(_) => ()
        }
        case _ => {
          case Success(result) => result.errors shouldBe empty
          case Failure(t)      => fail(t)
        }
      }
    }
  }

  /*
   * negative tests
   */
  private def hasErrors(expected: SemanticError*): Unit = {
    run().assertTryIn {
      case CypherVersion.Cypher5 => {
        case Success(_) => fail(new Exception("LET is not part of Cypher 5 syntax"))
        case Failure(_) => ()
      }
      case _ => {
        case Success(result) => result.errors should contain theSameElementsAs expected
        case Failure(t)      => fail(t)
      }
    }
  }

  /*
   * negative tests: Variable not defined
   */
  for {
    (proj, offset) <- Seq(
      ("x = a", 4),
      ("x = 1, y = a", 11),
      ("a = 1, x = a", 11),
      ("x = a, a = 1", 4),
      ("x = 1, a = a", 11),
      ("x = 1, y = ceil(sqrt(a)) % 2 = 0, z = 'abc'", 21)
    )
  } {
    test(s"LET $proj RETURN x") {
      val position = p(4 + offset, 1, 5 + offset)
      hasErrors(SemanticError(
        GqlHelper.getGql42001_42N62("a", position.offset, position.line, position.column),
        "Variable `a` not defined",
        position
      ))
    }
  }

  /*
   * negative tests: Variable already declared
   */
  for {
    (cypher, offset) <- Seq(
      ("UNWIND [1, 2] AS a LET a = 1 RETURN a", 23),
      ("MATCH (a) LET a = 1 RETURN a", 14),
      ("LET a = 1 LET a = 1 RETURN a", 14),
      ("LET a = 1 LET a = 2 RETURN a", 14),
      ("WITH 1 AS a LET a = 2 RETURN a", 16),
      ("WITH count(*) AS a LET a = 2 RETURN a", 23),
      ("UNWIND [1, 2] AS a WITH * WHERE true LET a = 2 RETURN a", 41),
      ("UNWIND [1, 2] AS a FILTER true LET a = 2 RETURN a", 35),
      ("CALL { MATCH (a) RETURN a } LET a = 1 RETURN a", 32),
      ("CALL { MATCH (n) RETURN n AS a } LET a = 1 RETURN a", 37),
      ("CREATE (a:A) LET a = 1 RETURN a", 17),
      ("MERGE (a:A) LET a = 1 RETURN a", 16)
    )
  } {
    test(cypher) {
      val position = p(offset, 1, offset + 1)
      hasErrors(SemanticError(
        GqlHelper.getGql42001_42N59("a", position.offset, position.line, position.column),
        "Variable `a` already declared",
        position
      ))
    }
  }

  /*
   * negative tests: Invalid use of aggregating function in this context
   */
  for {
    (agg, args) <- Seq(
      ("collect", "a"),
      ("count", "*"),
      ("count", "a"),
      ("sum", "a"),
      ("min", "a"),
      ("max", "a"),
      ("avg", "a"),
      ("stDev", "a"),
      ("stDevP", "a"),
      ("percentileCont", "a, 0.5"),
      ("percentileDisc", "a, 0.5")
    )
  } {
    test(s"UNWIND [1.1, 2.1, 3.1] AS a LET x = $agg($args) RETURN x") {
      val position = p(36, 1, 37)
      hasErrors(SemanticError(
        GqlHelper.getGql42001_42I24(s"$agg($args)", position.offset, position.line, position.column),
        s"Invalid use of aggregating function $agg(...) in this context",
        position
      ))
    }
  }
}
