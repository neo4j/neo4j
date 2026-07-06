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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupportWithPosConversion
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.SemanticDirection

/* Tests for combining listing and terminating commands with regular Cypher, administration commands and schema commands */
class CombineCommandsAndRegularCypherParserTest extends CombineCommandsParserTestBase
    with AstConstructionTestSupportWithPosConversion {

  Seq(
    ("MATCH (n) RETURN n", Seq(match_(nodePat(Some("n"))), return_(variableReturnItem("n")))),
    ("WITH 1 AS x", Seq(with_(aliasedReturnItem(literalInt(1), "x")))),
    ("UNWIND [1, 2, 3] AS id", Seq(unwind(listOfInt(1, 2, 3), varFor("id")))),
    ("FINISH", Seq(finish()))
  ).foreach { case (otherClause, clauseSeq: Seq[ast.Clause]) =>
    test(s"SHOW TRANSACTIONS tx $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            showTx(ExpressionNames(varFor("tx")), None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW TRANSACTIONS") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses = clauseSeq :+ showTx(NoNames, None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"TERMINATE TRANSACTIONS tx $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            terminateTx(ExpressionNames(varFor("tx")), None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause TERMINATE TRANSACTIONS tx") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            clauseSeq :+ terminateTx(ExpressionNames(varFor("tx")), None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"SHOW SETTINGS setting $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            showSetting(ExpressionNames(varFor("setting")), None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW SETTINGS") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            clauseSeq :+ showSetting(NoNames, None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"SHOW FUNCTIONS $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            showFunction(ast.AllFunctions, None, None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW FUNCTIONS") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            clauseSeq :+ showFunction(ast.AllFunctions, None, None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"SHOW PROCEDURES $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses = showProcedure(None, None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW PROCEDURES") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses = clauseSeq :+ showProcedure(None, None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"SHOW CONSTRAINTS $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            showConstraint(ast.AllConstraints, None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW CONSTRAINTS") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            clauseSeq :+ showConstraint(ast.AllConstraints, None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"SHOW INDEXES $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses = showIndex(ast.AllIndexes, None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW INDEXES") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses = clauseSeq :+ showIndex(ast.AllIndexes, None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"SHOW CURRENT GRAPH TYPE $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            showCurrentGraphType(asGraph = false, None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW CURRENT GRAPH TYPE") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            clauseSeq :+ showCurrentGraphType(asGraph = false, None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"SHOW HOME DATABASE $otherClause") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            showDatabase(ast.HomeDatabaseScope()(pos), None, yieldAll = false, List.empty, None)(pos) +: clauseSeq
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

    test(s"$otherClause SHOW HOME DATABASE") {
      parsesIn[ast.Statements] {
        case Cypher5 => _.withMessageStart("Invalid input")
        case _ =>
          val expectedClauses =
            clauseSeq :+ showDatabase(ast.HomeDatabaseScope()(pos), None, yieldAll = false, List.empty, None)(pos)
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
      }
    }

  }

  for {
    prefix <- Seq("USE neo4j", "")
    (command, clause) <- Seq(
      ("SHOW INDEXES", showIndex(ast.AllIndexes, _, _, _, _)),
      ("SHOW CONSTRAINTS", showConstraint(ast.AllConstraints, _, _, _, _)),
      ("SHOW FUNCTIONS", showFunction(ast.AllFunctions, None, _, _, _, _)),
      ("SHOW PROCEDURES", showProcedure(None, _, _, _, _)),
      ("SHOW SETTINGS set", showSetting(ExpressionNames(varFor("set")), _, _, _, _)),
      ("SHOW TRANSACTIONS tx", showTx(ExpressionNames(varFor("tx")), _, _, _, _)),
      ("TERMINATE TRANSACTION tx", terminateTx(ExpressionNames(varFor("tx")), _, _, _, _)),
      ("SHOW CURRENT GRAPH TYPE", showCurrentGraphType(false, _, _, _, _)),
      ("SHOW DATABASE foo", showDatabase(ast.SingleNamedDatabaseScope(ast.NamespacedName("foo")(pos))(pos), _, _, _, _))
    )
  } {
    val maybeUseClause = if (prefix.nonEmpty) Some(use(List("neo4j"), resolveStrictly = true)) else None

    test(s"$prefix $command WITH * MATCH (n) RETURN n") {
      parsesIn[ast.Statements] {
        // Can't parse WITH after SHOW/TERMINATE
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("INDEX") || command.contains("CONSTRAINT")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("PROCEDURE") || command.contains("FUNCTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("DATABASE")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(None, false, List.empty, None),
            withAll(),
            match_(nodePat(Some("n"))),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD * WITH * MATCH (n) RETURN n") {
      parsesIn[ast.Statements] {
        // Can't parse WITH after SHOW/TERMINATE
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else _.withSyntaxErrorContaining("Invalid input 'WITH': expected 'ORDER BY'")
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(None, true, List.empty, Some(withFromYield(returnAllItems))),
            withAll(),
            match_(nodePat(Some("n"))),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix UNWIND range(1,10) as b $command YIELD * RETURN *") {
      parsesIn[ast.Statements] {
        // Can't parse SHOW/TERMINATE after UNWIND
        case Cypher5 =>
          if (command.contains("TERMINATE")) _.withSyntaxErrorContaining(
            "Invalid input 'TERMINATE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'SHOW': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            unwind(function("range", literalInt(1), literalInt(10)), varFor("b")),
            clause(
              None,
              true,
              List.empty,
              Some(withFromYield(returnAllItems))
            ),
            returnAll
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH name, type RETURN *") {
      parsesIn[ast.Statements] {
        // Can't parse WITH after SHOW/TERMINATE
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("INDEX") || command.contains("CONSTRAINT")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("PROCEDURE") || command.contains("FUNCTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("DATABASE")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(None, false, List.empty, None),
            with_(returnItem(varFor("name"), "name"), returnItem(varFor("type"), "type")),
            returnAll
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix WITH 'n' as n $command YIELD name RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TERMINATE")) _.withSyntaxErrorContaining(
            "Invalid input 'TERMINATE': expected ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'SHOW': expected ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            with_(aliasedReturnItem(literalString("n"), "n")),
            clause(
              None,
              false,
              List(commandResultItem("name")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("name"))))
            ),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("INDEX") || command.contains("CONSTRAINT")) _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("PROCEDURE") || command.contains("FUNCTION")) _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("DATABASE")) _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(None, false, List.empty, None),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH 1 as c RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("INDEX") || command.contains("CONSTRAINT")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("PROCEDURE") || command.contains("FUNCTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("DATABASE")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(None, false, List.empty, None),
            with_(aliasedReturnItem(literalInt(1), "c")),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH 1 as c") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("INDEX") || command.contains("CONSTRAINT")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("PROCEDURE") || command.contains("FUNCTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("DATABASE")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(None, false, List.empty, None),
            with_(aliasedReturnItem(literalInt(1), "c"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD a WITH a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              None,
              false,
              List(commandResultItem("a")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
            ),
            with_(variableReturnItem("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command UNWIND as as a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("INDEX") || command.contains("CONSTRAINT")) _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("PROCEDURE") || command.contains("FUNCTION")) _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("DATABASE")) _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(None, false, List.empty, None),
            unwind(varFor("as"), varFor("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD as UNWIND as as a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("GRAPH")) showCurrentGraphTypeCypher5Error
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              None,
              false,
              List(commandResultItem("as")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("as"))))
            ),
            unwind(varFor("as"), varFor("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }
  }

  for {
    prefix <- Seq("USE neo4j", "")
    (command, clause) <- Seq(
      ("SHOW SETTINGS", showSetting _),
      ("SHOW TRANSACTIONS", showTx _),
      ("TERMINATE TRANSACTION", terminateTx _)
    )
  } {
    val maybeUseClause = if (prefix.nonEmpty) Some(use(List("neo4j"), resolveStrictly = true)) else None

    test(s"$prefix $command WITH * MATCH (n) RETURN n") {
      parsesIn[ast.Statements] {
        // Parses `WITH * MATCH (n)` as an expression
        case Cypher5 =>
          if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected an expression, 'WHERE', 'YIELD' or <EOF>""".stripMargin
          )
        case _ if command.contains("TERMINATE") =>
          // Since TERMINATE requires an expression it'll think the `WITH * MATCH (n)` is one (compared to SHOW where the expression is optional)
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              ExpressionNames(multiply(varFor("WITH"), function("MATCH", varFor("n")))),
              None,
              false,
              List.empty,
              None
            ),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(NoNames, None, false, List.empty, None),
            withAll(),
            match_(nodePat(Some("n"))),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD * WITH * MATCH (n) RETURN n") {
      parsesIn[ast.Statements] {
        // parses `YIELD * WITH * MATCH (n)` as an expression
        case Cypher5 =>
          if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'RETURN': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              ExpressionNames(multiply(multiply(varFor("YIELD"), varFor("WITH")), function("MATCH", varFor("n")))),
              None,
              false,
              List.empty,
              None
            ),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(NoNames, None, true, List.empty, Some(withFromYield(returnAllItems))),
            withAll(),
            match_(nodePat(Some("n"))),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix UNWIND range(1,10) as b $command YIELD * RETURN *") {
      parsesIn[ast.Statements] {
        // Can't parse SHOW/TERMINATE after UNWIND
        case Cypher5 =>
          if (command.contains("TERMINATE")) _.withSyntaxErrorContaining(
            "Invalid input 'TERMINATE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'SHOW': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining("Invalid input '': expected an expression")
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            unwind(function("range", literalInt(1), literalInt(10)), varFor("b")),
            clause(NoNames, None, true, List.empty, Some(withFromYield(returnAllItems))),
            returnAll
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH name, type RETURN *") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'name': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'name': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input 'name': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(NoNames, None, false, List.empty, None),
            with_(returnItem(varFor("name"), "name"), returnItem(varFor("type"), "type")),
            returnAll
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix WITH 'n' as n $command YIELD name RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TERMINATE")) _.withSyntaxErrorContaining(
            "Invalid input 'TERMINATE': expected ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'SHOW': expected ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input 'name': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            with_(aliasedReturnItem(literalString("n"), "n")),
            clause(
              NoNames,
              None,
              false,
              List(commandResultItem("name")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("name"))))
            ),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'name': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'name': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input 'name': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(NoNames, None, false, List.empty, None),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH 1 as c RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input '1': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input '1': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input '1': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(NoNames, None, false, List.empty, None),
            with_(aliasedReturnItem(literalInt(1), "c")),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH 1 as c") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input '1': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input '1': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input '1': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(NoNames, None, false, List.empty, None),
            with_(aliasedReturnItem(literalInt(1), "c"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD a WITH a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TERMINATE")) _.withSyntaxErrorContaining(
            "Invalid input 'a': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input 'a': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              NoNames,
              None,
              false,
              List(commandResultItem("a")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
            ),
            with_(variableReturnItem("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command UNWIND as as a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'as': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'as': expected an expression, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input 'as': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(NoNames, None, false, List.empty, None),
            unwind(varFor("as"), varFor("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD as UNWIND as as a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          if (command.contains("TERMINATE")) _.withSyntaxErrorContaining(
            "Invalid input 'as': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
          else if (command.contains("TRANSACTION")) _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"
          )
          else _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF>"
          )
        case _ if command.contains("TERMINATE") =>
          _.withSyntaxErrorContaining(
            "Invalid input 'as': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              NoNames,
              None,
              false,
              List(commandResultItem("as")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("as"))))
            ),
            unwind(varFor("as"), varFor("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }
  }

  for {
    prefix <- Seq("USE neo4j", "")
    (command, clause) <- Seq(
      ("SHOW DATABASES", showDatabase _)
    )
  } {
    val maybeUseClause = if (prefix.nonEmpty) Some(use(List("neo4j"), resolveStrictly = true)) else None

    test(s"$prefix $command WITH * MATCH (n) RETURN n") {
      parsesIn[ast.Statements] {
        // Parses `WITH` as database name
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            """Invalid input '*': expected a database name, 'WHERE', 'YIELD' or <EOF>""".stripMargin
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(ast.AllDatabasesScope()(pos), None, false, List.empty, None),
            withAll(),
            match_(nodePat(Some("n"))),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD * WITH * MATCH (n) RETURN n") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(ast.AllDatabasesScope()(pos), None, true, List.empty, Some(withFromYield(returnAllItems))),
            withAll(),
            match_(nodePat(Some("n"))),
            return_(variableReturnItem("n"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix UNWIND range(1,10) as b $command YIELD * RETURN *") {
      parsesIn[ast.Statements] {
        // Can't parse SHOW/TERMINATE after UNWIND
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'SHOW': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            unwind(function("range", literalInt(1), literalInt(10)), varFor("b")),
            clause(ast.AllDatabasesScope()(pos), None, true, List.empty, Some(withFromYield(returnAllItems))),
            returnAll
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH name, type RETURN *") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'name': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(ast.AllDatabasesScope()(pos), None, false, List.empty, None),
            with_(returnItem(varFor("name"), "name"), returnItem(varFor("type"), "type")),
            returnAll
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix WITH 'n' as n $command YIELD name RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'SHOW': expected ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            with_(aliasedReturnItem(literalString("n"), "n")),
            clause(
              ast.AllDatabasesScope()(pos),
              None,
              false,
              List(commandResultItem("name")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("name"))))
            ),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'name': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(ast.AllDatabasesScope()(pos), None, false, List.empty, None),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH 1 as c RETURN name as numIndexes") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input '1': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(ast.AllDatabasesScope()(pos), None, false, List.empty, None),
            with_(aliasedReturnItem(literalInt(1), "c")),
            return_(aliasedReturnItem(varFor("name"), "numIndexes"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command WITH 1 as c") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input '1': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(ast.AllDatabasesScope()(pos), None, false, List.empty, None),
            with_(aliasedReturnItem(literalInt(1), "c"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD a WITH a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              ast.AllDatabasesScope()(pos),
              None,
              false,
              List(commandResultItem("a")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
            ),
            with_(variableReturnItem("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command UNWIND as as a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'as': expected a database name, 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(ast.AllDatabasesScope()(pos), None, false, List.empty, None),
            unwind(varFor("as"), varFor("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }

    test(s"$prefix $command YIELD as UNWIND as as a RETURN a") {
      parsesIn[ast.Statements] {
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF>"
          )
        case _ =>
          val expected: List[ast.Clause] = maybeUseClause.toList ++ List(
            clause(
              ast.AllDatabasesScope()(pos),
              None,
              false,
              List(commandResultItem("as")),
              Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("as"))))
            ),
            unwind(varFor("as"), varFor("a")),
            return_(variableReturnItem("a"))
          )
          _.toAstPositioned(ast.Statements(Seq(singleQuery(expected: _*))))
      }
    }
  }

  Seq(
    "SHOW USERS",
    "CREATE ROLE name",
    "CREATE INDEX FOR (n:L) ON (n.p)",
    "DROP CONSTRAINT name"
  ).foreach(otherClause => {
    test(s"SHOW TRANSACTIONS $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW TRANSACTIONS") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"TERMINATE TRANSACTIONS $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause TERMINATE TRANSACTIONS") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"SHOW SETTINGS $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW SETTINGS") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"SHOW FUNCTIONS $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW FUNCTIONS") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"SHOW PROCEDURES $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW PROCEDURES") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"SHOW CONSTRAINTS $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW CONSTRAINTS") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"SHOW INDEXES $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW INDEXES") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"SHOW CURRENT GRAPH TYPE $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW CURRENT GRAPH TYPE") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"SHOW DEFAULT DATABASE $otherClause") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

    test(s"$otherClause SHOW DEFAULT DATABASE") {
      failsParsing[ast.Statements].withMessageStart("Invalid input")
    }

  })

  test(
    "SHOW TRANSACTIONS YIELD a1, b1 AS c1, d1 AS d1, e1 AS f1, g1 AS e1 ORDER BY a1, b1, d1, e1 WHERE a1 AND b1 AND d1 AND e1 RETURN a " +
      "UNION " +
      "TERMINATE TRANSACTIONS 'id' YIELD a2, b2 AS c2, d2 AS d2, e2 AS f2, g2 AS e2 ORDER BY a2, b2, d2, e2 WHERE a2 AND b2 AND d2 AND e2 RETURN a " +
      "UNION " +
      "SHOW SETTINGS YIELD a3, b3 AS c3, d3 AS d3, e3 AS f3, g3 AS e3 ORDER BY a3, b3, d3, e3 WHERE a3 AND b3 AND d3 AND e3 RETURN a " +
      "UNION " +
      "SHOW FUNCTIONS YIELD a4, b4 AS c4, d4 AS d4, e4 AS f4, g4 AS e4 ORDER BY a4, b4, d4, e4 WHERE a4 AND b4 AND d4 AND e4 RETURN a " +
      "UNION " +
      "SHOW PROCEDURES YIELD a5, b5 AS c5, d5 AS d5, e5 AS f5, g5 AS e5 ORDER BY a5, b5, d5, e5 WHERE a5 AND b5 AND d5 AND e5 RETURN a " +
      "UNION " +
      "SHOW INDEXES YIELD a6, b6 AS c6, d6 AS d6, e6 AS f6, g6 AS e6 ORDER BY a6, b6, d6, e6 WHERE a6 AND b6 AND d6 AND e6 RETURN a " +
      "UNION " +
      "SHOW CONSTRAINTS YIELD a7, b7 AS c7, d7 AS d7, e7 AS f7, g7 AS e7 ORDER BY a7, b7, d7, e7 WHERE a7 AND b7 AND d7 AND e7 RETURN a " +
      "UNION " +
      "SHOW CURRENT GRAPH TYPE AS GRAPH YIELD a8, b8 AS c8, d8 AS d8, e8 AS f8, g8 AS e8 ORDER BY a8, b8, d8, e8 WHERE a8 AND b8 AND d8 AND e8 RETURN a"
  ) {
    val showTxClause = showTx(
      NoNames,
      None,
      yieldAll = false,
      List(
        commandResultItem("a1"),
        commandResultItem("b1", Some("c1")),
        commandResultItem("d1", Some("d1")),
        commandResultItem("e1", Some("f1")),
        commandResultItem("g1", Some("e1"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a1", "c1", "d1", "f1", "e1")),
        Some(orderBy(
          sortItem(varFor("a1")),
          sortItem(varFor("c1")),
          sortItem(varFor("d1")),
          sortItem(varFor("e1"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a1"),
                varFor("c1")
              ),
              varFor("d1")
            ),
            varFor("e1")
          )
        ))
      ))
    )
    val terminateTxClause = terminateTx(
      ExpressionNames(literalString("id")),
      None,
      yieldAll = false,
      List(
        commandResultItem("a2"),
        commandResultItem("b2", Some("c2")),
        commandResultItem("d2", Some("d2")),
        commandResultItem("e2", Some("f2")),
        commandResultItem("g2", Some("e2"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a2", "c2", "d2", "f2", "e2")),
        Some(orderBy(
          sortItem(varFor("a2")),
          sortItem(varFor("c2")),
          sortItem(varFor("d2")),
          sortItem(varFor("e2"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a2"),
                varFor("c2")
              ),
              varFor("d2")
            ),
            varFor("e2")
          )
        ))
      ))
    )
    val showSettingsClause = showSetting(
      NoNames,
      None,
      yieldAll = false,
      List(
        commandResultItem("a3"),
        commandResultItem("b3", Some("c3")),
        commandResultItem("d3", Some("d3")),
        commandResultItem("e3", Some("f3")),
        commandResultItem("g3", Some("e3"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a3", "c3", "d3", "f3", "e3")),
        Some(orderBy(
          sortItem(varFor("a3")),
          sortItem(varFor("c3")),
          sortItem(varFor("d3")),
          sortItem(varFor("e3"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a3"),
                varFor("c3")
              ),
              varFor("d3")
            ),
            varFor("e3")
          )
        ))
      ))
    )
    val showFunctionsClause = showFunction(
      ast.AllFunctions,
      None,
      None,
      yieldAll = false,
      List(
        commandResultItem("a4"),
        commandResultItem("b4", Some("c4")),
        commandResultItem("d4", Some("d4")),
        commandResultItem("e4", Some("f4")),
        commandResultItem("g4", Some("e4"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a4", "c4", "d4", "f4", "e4")),
        Some(orderBy(
          sortItem(varFor("a4")),
          sortItem(varFor("c4")),
          sortItem(varFor("d4")),
          sortItem(varFor("e4"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a4"),
                varFor("c4")
              ),
              varFor("d4")
            ),
            varFor("e4")
          )
        ))
      ))
    )
    val showProceduresClause = showProcedure(
      None,
      None,
      yieldAll = false,
      List(
        commandResultItem("a5"),
        commandResultItem("b5", Some("c5")),
        commandResultItem("d5", Some("d5")),
        commandResultItem("e5", Some("f5")),
        commandResultItem("g5", Some("e5"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a5", "c5", "d5", "f5", "e5")),
        Some(orderBy(
          sortItem(varFor("a5")),
          sortItem(varFor("c5")),
          sortItem(varFor("d5")),
          sortItem(varFor("e5"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a5"),
                varFor("c5")
              ),
              varFor("d5")
            ),
            varFor("e5")
          )
        ))
      ))
    )
    val showIndexesClause = showIndex(
      ast.AllIndexes,
      None,
      yieldAll = false,
      List(
        commandResultItem("a6"),
        commandResultItem("b6", Some("c6")),
        commandResultItem("d6", Some("d6")),
        commandResultItem("e6", Some("f6")),
        commandResultItem("g6", Some("e6"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a6", "c6", "d6", "f6", "e6")),
        Some(orderBy(
          sortItem(varFor("a6")),
          sortItem(varFor("c6")),
          sortItem(varFor("d6")),
          sortItem(varFor("e6"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a6"),
                varFor("c6")
              ),
              varFor("d6")
            ),
            varFor("e6")
          )
        ))
      ))
    )
    val showConstraintsClause = showConstraint(
      ast.AllConstraints,
      None,
      yieldAll = false,
      List(
        commandResultItem("a7"),
        commandResultItem("b7", Some("c7")),
        commandResultItem("d7", Some("d7")),
        commandResultItem("e7", Some("f7")),
        commandResultItem("g7", Some("e7"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a7", "c7", "d7", "f7", "e7")),
        Some(orderBy(
          sortItem(varFor("a7")),
          sortItem(varFor("c7")),
          sortItem(varFor("d7")),
          sortItem(varFor("e7"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a7"),
                varFor("c7")
              ),
              varFor("d7")
            ),
            varFor("e7")
          )
        ))
      ))
    )
    val showCurrentGraphTypeClause = showCurrentGraphType(
      asGraph = true,
      None,
      yieldAll = false,
      List(
        commandResultItem("a8"),
        commandResultItem("b8", Some("c8")),
        commandResultItem("d8", Some("d8")),
        commandResultItem("e8", Some("f8")),
        commandResultItem("g8", Some("e8"))
      ),
      Some(withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a8", "c8", "d8", "f8", "e8")),
        Some(orderBy(
          sortItem(varFor("a8")),
          sortItem(varFor("c8")),
          sortItem(varFor("d8")),
          sortItem(varFor("e8"))
        )),
        where = Some(where(
          and(
            and(
              and(
                varFor("a8"),
                varFor("c8")
              ),
              varFor("d8")
            ),
            varFor("e8")
          )
        ))
      ))
    )

    parsesIn[ast.Statements] {
      case Cypher5 => _.withMessageStart("Invalid input 'UNION'")
      case _ => _.toAstPositioned(ast.Statements(Seq(unionDistinct(
          singleQuery(showTxClause, return_(variableReturnItem("a"))),
          singleQuery(terminateTxClause, return_(variableReturnItem("a"))),
          singleQuery(showSettingsClause, return_(variableReturnItem("a"))),
          singleQuery(showFunctionsClause, return_(variableReturnItem("a"))),
          singleQuery(showProceduresClause, return_(variableReturnItem("a"))),
          singleQuery(showIndexesClause, return_(variableReturnItem("a"))),
          singleQuery(showConstraintsClause, return_(variableReturnItem("a"))),
          singleQuery(showCurrentGraphTypeClause, return_(variableReturnItem("a")))
        ))))
    }
  }

  test("SHOW TRANSACTIONS MATCH (n)") {
    parsesIn[ast.Statements] {
      // Cypher 5 parses 'MATCH(n)' as a function call
      case Cypher5 =>
        _.toAstPositioned(ast.Statements(Seq(singleQuery(
          ast.ShowTransactionsClause(
            ExpressionNames(function("MATCH", varFor("n"))),
            None,
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = true
          )(pos)
        ))))
      case _ =>
        _.toAstPositioned(ast.Statements(Seq(singleQuery(
          ast.ShowTransactionsClause(
            NoNames,
            None,
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = false
          )(pos),
          match_(nodePat(Some("n")))
        ))))
    }
  }

  test("SHOW TRANSACTIONS MATCH (n) RETURN *") {
    parsesIn[ast.Statements] {
      // Cypher 5 parses 'MATCH(n)' as a function call, and then fails on the RETURN
      case Cypher5 => _.withMessageStart("Invalid input 'RETURN': expected ")
      case _ =>
        _.toAstPositioned(ast.Statements(Seq(singleQuery(
          ast.ShowTransactionsClause(
            NoNames,
            None,
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = false
          )(pos),
          match_(nodePat(Some("n"))),
          returnAll
        ))))
    }
  }

  test("SHOW TRANSACTIONS MATCH (n) YIELD x, y RETURN *") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ast.ShowTransactionsClause(
          ExpressionNames(function("MATCH", varFor("n"))),
          None,
          List(commandResultItem("x", Some("x")), commandResultItem("y", Some("y"))),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("x", "y")))),
          fromCypher5
        )(pos),
        returnAll
      )
    )
  }

  test("SHOW TRANSACTIONS MATCH (n) YIELD x, y") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ast.ShowTransactionsClause(
          ExpressionNames(function("MATCH", varFor("n"))),
          None,
          List(commandResultItem("x", Some("x")), commandResultItem("y", Some("y"))),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("x", "y")))),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS MATCH (n)-[]->(m)") {
    parsesIn[ast.Statements] {
      // Cypher 5 parses 'MATCH(n)-[]-' as a function call minus an empty list minus something which is where it fails
      case Cypher5 => _.withMessageStart("Invalid input '>': expected an expression (")
      case _ =>
        _.toAstPositioned(ast.Statements(Seq(singleQuery(
          showTx(NoNames, None, yieldAll = false, List.empty, None)(pos),
          match_(relationshipChain(
            nodePat(Some("n")),
            relPat(direction = SemanticDirection.OUTGOING),
            nodePat(Some("m"))
          ))
        ))))
    }
  }

  test("MATCH (n) TERMINATE TRANSACTION") {
    failsParsing[ast.Statements].in {
      // not allowed to combine MATCH and TERMINATE TRANSACTION
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'TERMINATE': expected a graph pattern, ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'USING', 'WHERE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
            |"MATCH (n) TERMINATE TRANSACTION"
            |           ^""".stripMargin
        )
      // missing id for terminate transaction
      case _ => _.withSyntaxError(
          """Invalid input '': expected a string or an expression (line 1, column 32 (offset: 31))
            |"MATCH (n) TERMINATE TRANSACTION"
            |                                ^""".stripMargin
        )
    }
  }
}
