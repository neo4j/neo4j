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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher6
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.InputPosition

class SubqueryCallParserTest extends AstParsingTestBase {

  test("CALL { RETURN 1 }") {
    parsesTo[SubqueryCall](importingWithSubqueryCall(return_(literalInt(1).unaliased)))
  }

  test("CALL { CALL { RETURN 1 as a } }") {
    parsesTo[SubqueryCall](importingWithSubqueryCall(importingWithSubqueryCall(return_(literalInt(1).as("a")))))
  }

  test("CALL { RETURN 1 AS a UNION RETURN 2 AS a }") {
    parsesIn[SubqueryCall] {
      case Cypher6 => _.toAst(
          importingWithSubqueryCall(
            union(
              singleQuery(return_(literalInt(1).as("a"))),
              singleQuery(return_(literalInt(2).as("a")))
            )
          )
        )
      case _ => _.toAst(importingWithSubqueryCall(
          union(
            singleQuery(return_(literalInt(1).as("a"))),
            singleQuery(return_(literalInt(2).as("a"))),
            differentReturnOrderAllowed = true
          )
        ))
    }
  }

  test("CALL { }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '}'")
      case _ => _.withMessage(
          """Invalid input '}': expected 'FOREACH', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNWIND', 'USE' or 'WITH' (line 1, column 8 (offset: 7))
            |"CALL { }"
            |        ^""".stripMargin
        )
    }
  }

  test("CALL { CREATE (n:N) }") {
    parsesTo[SubqueryCall](importingWithSubqueryCall(create(nodePat(Some("n"), Some(labelLeaf("N"))))))
  }

  // Subquery call with importing variable scope

  test("CALL () { CREATE (n:N) }") {
    parsesTo[SubqueryCall](scopeClauseSubqueryCall(false, Seq.empty, create(nodePat(Some("n"), Some(labelLeaf("N"))))))
  }

  test("CALL (*) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](scopeClauseSubqueryCall(true, Seq.empty, create(nodePat(Some("n"), Some(labelLeaf("N"))))))
  }

  test("CALL (*, a) { CREATE (n:N) }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage("Invalid input ',': expected \")\" (line 1, column 8 (offset: 7))")
      case _ => _.withMessage(
          """Invalid input ',': expected ')' (line 1, column 8 (offset: 7))
            |"CALL (*, a) { CREATE (n:N) }"
            |        ^""".stripMargin
        )
    }
  }

  test("CALL (a, *) { CREATE (n:N) }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage("Invalid input '*': expected an identifier (line 1, column 10 (offset: 9))")
      case _ => _.withMessage(
          """Invalid input '*': expected an identifier (line 1, column 10 (offset: 9))
            |"CALL (a, *) { CREATE (n:N) }"
            |          ^""".stripMargin
        )
    }
  }

  test("CALL (a) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](scopeClauseSubqueryCall(
      false,
      Seq(varFor("a")),
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("CALL (a, b) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](scopeClauseSubqueryCall(
      false,
      Seq(varFor("a"), varFor("b")),
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("CALL (a, b, c) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](scopeClauseSubqueryCall(
      false,
      Seq(varFor("a"), varFor("b"), varFor("c")),
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("CALL() { CALL() { RETURN 1 as a } }") {
    parsesTo[SubqueryCall](scopeClauseSubqueryCall(
      false,
      Seq.empty,
      scopeClauseSubqueryCall(false, Seq.empty, return_(literalInt(1).as("a")))
    ))
  }

  // OPTIONAL SUBQUERY CALL

  test("OPTIONAL CALL { RETURN 1 }") {
    parses[SubqueryCall].toAstPositioned(ImportingWithSubqueryCall(
      SingleQuery(Seq(return_(literalInt(1).unaliased)))(pos),
      None,
      optional = true
    )(InputPosition(0, 1, 1)))
  }

  test("OPTIONAL CALL { OPTIONAL CALL { RETURN 1 as a } }") {
    parsesTo[SubqueryCall](
      optionalImportingWithSubqueryCall(optionalImportingWithSubqueryCall(return_(literalInt(1).as("a"))))
    )
  }

  test("OPTIONAL CALL { RETURN 1 AS a UNION RETURN 2 AS a }") {
    parsesIn[SubqueryCall] {
      case Cypher6 => _.toAst(optionalImportingWithSubqueryCall(
          unionDistinct(
            false,
            singleQuery(return_(literalInt(1).as("a"))),
            singleQuery(return_(literalInt(2).as("a")))
          )
        ))
      case _ => _.toAst(optionalImportingWithSubqueryCall(
          unionDistinct(
            true,
            singleQuery(return_(literalInt(1).as("a"))),
            singleQuery(return_(literalInt(2).as("a")))
          )
        ))
    }
  }

  test("OPTIONAL CALL { }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '}'")
      case _ => _.withMessage(
          """Invalid input '}': expected 'FOREACH', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNWIND', 'USE' or 'WITH' (line 1, column 17 (offset: 16))
            |"OPTIONAL CALL { }"
            |                 ^""".stripMargin
        )
    }
  }

  test("OPTIONAL CALL { CREATE (n:N) }") {
    parsesTo[SubqueryCall](optionalImportingWithSubqueryCall(create(nodePat(Some("n"), Some(labelLeaf("N"))))))
  }

  // Subquery call with importing variable scope

  test("OPTIONAL CALL () { RETURN 1 }") {
    parses[SubqueryCall].toAstPositioned(ScopeClauseSubqueryCall(
      SingleQuery(Seq(return_(literalInt(1).unaliased)))(pos),
      isImportingAll = false,
      Seq.empty,
      None,
      optional = true
    )(InputPosition(0, 1, 1)))
  }

  test("OPTIONAL CALL () { CREATE (n:N) }") {
    parsesTo[SubqueryCall](optionalScopeClauseSubqueryCall(
      false,
      Seq.empty,
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("OPTIONAL CALL (*) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](optionalScopeClauseSubqueryCall(
      true,
      Seq.empty,
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("OPTIONAL CALL (*, a) { CREATE (n:N) }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage("Invalid input ',': expected \")\" (line 1, column 17 (offset: 16))")
      case _ => _.withMessage(
          """Invalid input ',': expected ')' (line 1, column 17 (offset: 16))
            |"OPTIONAL CALL (*, a) { CREATE (n:N) }"
            |                 ^""".stripMargin
        )
    }
  }

  test("OPTIONAL CALL (a, *) { CREATE (n:N) }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage("Invalid input '*': expected an identifier (line 1, column 19 (offset: 18))")
      case _ => _.withMessage(
          """Invalid input '*': expected an identifier (line 1, column 19 (offset: 18))
            |"OPTIONAL CALL (a, *) { CREATE (n:N) }"
            |                   ^""".stripMargin
        )
    }
  }

  test("OPTIONAL CALL (a) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](optionalScopeClauseSubqueryCall(
      false,
      Seq(varFor("a")),
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("OPTIONAL CALL (a, b) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](optionalScopeClauseSubqueryCall(
      false,
      Seq(varFor("a"), varFor("b")),
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("OPTIONAL CALL (a, b, c) { CREATE (n:N) }") {
    parsesTo[SubqueryCall](optionalScopeClauseSubqueryCall(
      false,
      Seq(varFor("a"), varFor("b"), varFor("c")),
      create(nodePat(Some("n"), Some(labelLeaf("N"))))
    ))
  }

  test("OPTIONAL CALL() { OPTIONAL CALL() { RETURN 1 as a } }") {
    parsesTo[SubqueryCall](optionalScopeClauseSubqueryCall(
      false,
      Seq.empty,
      optionalScopeClauseSubqueryCall(false, Seq.empty, return_(literalInt(1).as("a")))
    ))
  }
}
