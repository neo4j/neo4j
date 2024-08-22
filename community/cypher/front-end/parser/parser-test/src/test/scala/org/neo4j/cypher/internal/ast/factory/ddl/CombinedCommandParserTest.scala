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
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny

/* Tests for combining listing and terminating commands */
class CombinedCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  private type CommandClauseWithNames =
    (
      Either[List[String], Expression],
      Option[(ast.Where, InputPosition)],
      Boolean,
      List[ast.CommandResultItem]
    ) => InputPosition => ast.CommandClause

  private type CommandClauseNoNames =
    (
      Option[(ast.Where, InputPosition)],
      Boolean,
      List[ast.CommandResultItem]
    ) => InputPosition => ast.CommandClause

  private def showTx(
    ids: Either[List[String], Expression],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.ShowTransactionsClause(ids, where.map(_._1), yieldItems, yieldAll)

  private def terminateTx(
    ids: Either[List[String], Expression],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.TerminateTransactionsClause(ids, yieldItems, yieldAll, where.map(_._2))

  private def showSetting(
    ids: Either[List[String], Expression],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.ShowSettingsClause(ids, where.map(_._1), yieldItems, yieldAll)

  private def showFunction(
    functionType: ast.ShowFunctionType,
    executable: Option[ast.ExecutableBy],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.ShowFunctionsClause(functionType, executable, where.map(_._1), yieldItems, yieldAll)

  private def showProcedure(
    executable: Option[ast.ExecutableBy],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.ShowProceduresClause(executable, where.map(_._1), yieldItems, yieldAll)

  private def showConstraint(
    constraintType: ast.ShowConstraintType,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.ShowConstraintsClause(constraintType, where.map(_._1), yieldItems, yieldAll, returnCypher5Values = false)

  private def showIndex(
    indexType: ast.ShowIndexType,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.ShowIndexesClause(indexType, where.map(_._1), yieldItems, yieldAll)

  private def getWherePosition(startIndex: Int = 0) = {
    val startOfWhereClause = testName.indexOf("WHERE", startIndex)
    InputPosition(startOfWhereClause, 1, startOfWhereClause + 1)
  }

  private val commandCombinationsAllowingStringExpressions = Seq(
    ("SHOW TRANSACTION", showTx: CommandClauseWithNames, "SHOW TRANSACTION", showTx: CommandClauseWithNames),
    ("SHOW TRANSACTION", showTx: CommandClauseWithNames, "TERMINATE TRANSACTION", terminateTx: CommandClauseWithNames),
    ("TERMINATE TRANSACTION", terminateTx: CommandClauseWithNames, "SHOW TRANSACTION", showTx: CommandClauseWithNames),
    (
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames,
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames
    ),
    ("SHOW SETTING", showSetting: CommandClauseWithNames, "SHOW SETTING", showSetting: CommandClauseWithNames),
    ("SHOW TRANSACTION", showTx: CommandClauseWithNames, "SHOW SETTING", showSetting: CommandClauseWithNames),
    ("SHOW SETTING", showSetting: CommandClauseWithNames, "SHOW TRANSACTION", showTx: CommandClauseWithNames),
    ("TERMINATE TRANSACTION", terminateTx: CommandClauseWithNames, "SHOW SETTING", showSetting: CommandClauseWithNames),
    ("SHOW SETTING", showSetting: CommandClauseWithNames, "TERMINATE TRANSACTION", terminateTx: CommandClauseWithNames)
  )

  private val commandCombinationsWithoutExpressions: Seq[(String, CommandClauseNoNames, String, CommandClauseNoNames)] =
    Seq(
      // show functions only combinations
      (
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _),
        "SHOW FUNCTIONS EXECUTABLE",
        showFunction(ast.AllFunctions, Some(ast.CurrentUser), _, _, _)
      ),
      (
        "SHOW ALL FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _),
        "SHOW ALL FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _)
      ),
      (
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _),
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _)
      ),
      (
        "SHOW USER DEFINED FUNCTIONS",
        showFunction(ast.UserDefinedFunctions, None, _, _, _),
        "SHOW USER DEFINED FUNCTIONS EXECUTABLE BY user",
        showFunction(ast.UserDefinedFunctions, Some(ast.User("user")), _, _, _)
      ),
      (
        "SHOW FUNCTIONS EXECUTABLE BY user",
        showFunction(ast.AllFunctions, Some(ast.User("user")), _, _, _),
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _)
      ),
      (
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _),
        "SHOW USER DEFINED FUNCTIONS",
        showFunction(ast.UserDefinedFunctions, None, _, _, _)
      ),
      (
        "SHOW USER DEFINED FUNCTIONS EXECUTABLE",
        showFunction(ast.UserDefinedFunctions, Some(ast.CurrentUser), _, _, _),
        "SHOW ALL FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _)
      )
    ) ++ Seq(
      // show procedures only combinations
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _)
      ),
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW PROCEDURES EXECUTABLE",
        showProcedure(Some(ast.CurrentUser), _, _, _)
      ),
      (
        "SHOW PROCEDURES EXECUTABLE BY CURRENT USER",
        showProcedure(Some(ast.CurrentUser), _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _)
      ),
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW PROCEDURES EXECUTABLE BY user",
        showProcedure(Some(ast.User("user")), _, _, _)
      ),
      (
        "SHOW PROCEDURES EXECUTABLE",
        showProcedure(Some(ast.CurrentUser), _, _, _),
        "SHOW PROCEDURES EXECUTABLE BY SHOW",
        showProcedure(Some(ast.User("SHOW")), _, _, _)
      )
    ) ++ Seq(
      // show constraints only combinations
      (
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _)
      ),
      (
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "SHOW NODE KEY CONSTRAINTS",
        showConstraint(ast.NodeKeyConstraints, _, _, _)
      ),
      (
        "SHOW RELATIONSHIP KEY CONSTRAINTS",
        showConstraint(ast.RelKeyConstraints, _, _, _),
        "SHOW KEY CONSTRAINTS",
        showConstraint(ast.KeyConstraints, _, _, _)
      ),
      (
        "SHOW NODE UNIQUENESS CONSTRAINTS",
        showConstraint(ast.NodeUniqueConstraints.cypher6, _, _, _),
        "SHOW UNIQUE CONSTRAINTS",
        showConstraint(ast.UniqueConstraints.cypher6, _, _, _)
      ),
      (
        "SHOW REL UNIQUE CONSTRAINTS",
        showConstraint(ast.RelUniqueConstraints.cypher6, _, _, _),
        "SHOW EXISTENCE CONSTRAINTS",
        showConstraint(ast.ExistsConstraints.cypher6, _, _, _)
      ),
      (
        "SHOW NODE EXIST CONSTRAINTS",
        showConstraint(ast.NodeExistsConstraints.cypher6, _, _, _),
        "SHOW REL EXIST CONSTRAINTS",
        showConstraint(ast.RelExistsConstraints.cypher6, _, _, _)
      ),
      (
        "SHOW PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.PropTypeConstraints, _, _, _),
        "SHOW NODE PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.NodePropTypeConstraints, _, _, _)
      ),
      (
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "SHOW RELATIONSHIP PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.RelPropTypeConstraints, _, _, _)
      )
    ) ++ Seq(
      // show indexes only combinations
      (
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _)
      ),
      (
        "SHOW ALL INDEXES",
        showIndex(ast.AllIndexes, _, _, _),
        "SHOW RANGE INDEXES",
        showIndex(ast.RangeIndexes, _, _, _)
      ),
      (
        "SHOW FULLTEXT INDEXES",
        showIndex(ast.FulltextIndexes, _, _, _),
        "SHOW TEXT INDEXES",
        showIndex(ast.TextIndexes, _, _, _)
      ),
      (
        "SHOW POINT INDEXES",
        showIndex(ast.PointIndexes, _, _, _),
        "SHOW LOOKUP INDEXES",
        showIndex(ast.LookupIndexes, _, _, _)
      ),
      (
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _),
        "SHOW VECTOR INDEXES",
        showIndex(ast.VectorIndexes, _, _, _)
      )
    ) ++ Seq(
      // mixed show and terminate commands
      // excluding mixes of only those accepting string expressions,
      // as that is handled by `commandCombinationsAllowingStringExpressions`

      // show transaction combined with remaining commands
      (
        "SHOW TRANSACTIONS",
        showTx(Left(List.empty), _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _)
      ),
      (
        "SHOW ALL FUNCTIONS EXECUTABLE BY SHOW",
        showFunction(ast.AllFunctions, Some(ast.User("SHOW")), _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(Right(literalString("db1-transaction-123")), _, _, _)
      ),
      (
        "SHOW TRANSACTIONS",
        showTx(Left(List.empty), _, _, _),
        "SHOW PROCEDURES EXECUTABLE BY SHOW",
        showProcedure(Some(ast.User("SHOW")), _, _, _)
      ),
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(Right(literalString("db1-transaction-123")), _, _, _)
      ),
      (
        "SHOW TRANSACTIONS",
        showTx(Left(List.empty), _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _)
      ),
      (
        "SHOW PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.PropTypeConstraints, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(Right(literalString("db1-transaction-123")), _, _, _)
      ),
      (
        "SHOW TRANSACTIONS",
        showTx(Left(List.empty), _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _)
      ),
      (
        "SHOW POINT INDEXES",
        showIndex(ast.PointIndexes, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(Right(literalString("db1-transaction-123")), _, _, _)
      ),
      // terminate transaction combined with remaining commands
      (
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(Right(literalString("db1-transaction-123")), _, _, _),
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _)
      ),
      (
        "SHOW FUNCTIONS EXECUTABLE BY TERMINATE",
        showFunction(ast.AllFunctions, Some(ast.User("TERMINATE")), _, _, _),
        "TERMINATE TRANSACTIONS",
        terminateTx(Left(List.empty), _, _, _)
      ),
      (
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(Right(literalString("db1-transaction-123")), _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _)
      ),
      (
        "SHOW PROCEDURES EXECUTABLE BY TERMINATE",
        showProcedure(Some(ast.User("TERMINATE")), _, _, _),
        "TERMINATE TRANSACTIONS",
        terminateTx(Left(List.empty), _, _, _)
      ),
      (
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(Right(literalString("db1-transaction-123")), _, _, _),
        "SHOW NODE EXISTENCE CONSTRAINTS",
        showConstraint(ast.NodeExistsConstraints.cypher6, _, _, _)
      ),
      (
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "TERMINATE TRANSACTIONS",
        terminateTx(Left(List.empty), _, _, _)
      ),
      (
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(Right(literalString("db1-transaction-123")), _, _, _),
        "SHOW RANGE INDEXES",
        showIndex(ast.RangeIndexes, _, _, _)
      ),
      (
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _),
        "TERMINATE TRANSACTIONS",
        terminateTx(Left(List.empty), _, _, _)
      ),
      // show settings combined with remaining commands
      (
        "SHOW SETTINGS",
        showSetting(Left(List.empty), _, _, _),
        "SHOW USER DEFINED FUNCTIONS EXECUTABLE",
        showFunction(ast.UserDefinedFunctions, Some(ast.CurrentUser), _, _, _)
      ),
      (
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(Right(parameter("setting", CTAny)), _, _, _)
      ),
      (
        "SHOW SETTINGS",
        showSetting(Left(List.empty), _, _, _),
        "SHOW PROCEDURES EXECUTABLE",
        showProcedure(Some(ast.CurrentUser), _, _, _)
      ),
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(Right(parameter("setting", CTAny)), _, _, _)
      ),
      (
        "SHOW SETTINGS",
        showSetting(Left(List.empty), _, _, _),
        "SHOW UNIQUENESS CONSTRAINTS",
        showConstraint(ast.UniqueConstraints.cypher6, _, _, _)
      ),
      (
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(Right(parameter("setting", CTAny)), _, _, _)
      ),
      (
        "SHOW SETTINGS",
        showSetting(Left(List.empty), _, _, _),
        "SHOW TEXT INDEXES",
        showIndex(ast.TextIndexes, _, _, _)
      ),
      (
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(Right(parameter("setting", CTAny)), _, _, _)
      ),
      // show functions combined with remaining commands
      (
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _)
      ),
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _)
      ),
      (
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _),
        "SHOW KEY CONSTRAINTS",
        showConstraint(ast.KeyConstraints, _, _, _)
      ),
      (
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _)
      ),
      (
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _)
      ),
      (
        "SHOW LOOKUP INDEXES",
        showIndex(ast.LookupIndexes, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _)
      ),
      // show procedures combined with remaining commands
      (
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _)
      ),
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _)
      ),
      (
        "SHOW ALL INDEXES",
        showIndex(ast.AllIndexes, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _)
      ),
      (
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _)
      ),
      // show constraints combined with remaining commands
      (
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _)
      ),
      (
        "SHOW FULLTEXT INDEXES",
        showIndex(ast.FulltextIndexes, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _)
      )
    )

  private val commandCombinationsAll: Seq[(String, CommandClauseNoNames, String, CommandClauseNoNames)] =
    commandCombinationsAllowingStringExpressions.map { case (firstCommand, firstClause, secondCommand, secondClause) =>
      (firstCommand, firstClause(Left(List.empty), _, _, _), secondCommand, secondClause(Left(List.empty), _, _, _))
    } ++ commandCombinationsWithoutExpressions

  private def updateForCypher5(clause: ast.Clause): ast.Clause = clause match {
    case scc: ast.ShowConstraintsClause =>
      scc.constraintType match {
        case _: ast.UniqueConstraints =>
          scc.copy(constraintType = ast.UniqueConstraints.cypher5, returnCypher5Values = true)(scc.position)
        case _: ast.NodeUniqueConstraints =>
          scc.copy(constraintType = ast.NodeUniqueConstraints.cypher5, returnCypher5Values = true)(scc.position)
        case _: ast.RelUniqueConstraints =>
          scc.copy(constraintType = ast.RelUniqueConstraints.cypher5, returnCypher5Values = true)(scc.position)
        case _: ast.ExistsConstraints =>
          scc.copy(constraintType = ast.ExistsConstraints.cypher5, returnCypher5Values = true)(scc.position)
        case _: ast.NodeExistsConstraints =>
          scc.copy(constraintType = ast.NodeExistsConstraints.cypher5, returnCypher5Values = true)(scc.position)
        case _: ast.RelExistsConstraints =>
          scc.copy(constraintType = ast.RelExistsConstraints.cypher5, returnCypher5Values = true)(scc.position)
        case _ =>
          scc.copy(returnCypher5Values = true)(scc.position)
      }
    case other => other
  }

  private def assertAst(expectedClauses: ast.Clause*): Unit = {
    parsesIn[ast.Statements] {
      case Cypher5 | Cypher5JavaCc =>
        _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses.map(updateForCypher5): _*))))
      case _ =>
        _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
    }
  }

  private def assertAstDontComparePos(expectedClauses: ast.Clause*): Unit = {
    parsesIn[ast.Statements] {
      case Cypher5 | Cypher5JavaCc =>
        _.toAst(ast.Statements(Seq(singleQuery(expectedClauses.map(updateForCypher5): _*))))
      case _ =>
        _.toAst(ast.Statements(Seq(singleQuery(expectedClauses: _*))))
    }
  }

  commandCombinationsAll.foreach {
    case (firstCommand, firstClause, secondCommand, secondClause) =>
      test(s"$firstCommand $secondCommand") {
        assertAst(
          firstClause(None, false, List.empty)(defaultPos),
          secondClause(None, false, List.empty)(pos)
        )
      }

      test(s"USE db $firstCommand $secondCommand") {
        assertAstDontComparePos(
          use(List("db")),
          firstClause(None, false, List.empty)(pos),
          secondClause(None, false, List.empty)(pos)
        )
      }

      test(s"$firstCommand WHERE transactionId = '123' $secondCommand") {
        assertAst(
          firstClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty
          )(defaultPos),
          secondClause(None, false, List.empty)(pos)
        )
      }

      test(s"$firstCommand $secondCommand WHERE transactionId = '123'") {
        assertAst(
          firstClause(None, false, List.empty)(defaultPos),
          secondClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty
          )(pos)
        )
      }

      test(s"$firstCommand WHERE transactionId = '123' $secondCommand WHERE transactionId = '123'") {
        val where1Pos = getWherePosition()
        val where2Pos = getWherePosition(where1Pos.offset + 1)
        assertAst(
          firstClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), where1Pos)),
            false,
            List.empty
          )(defaultPos),
          secondClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), where2Pos)),
            false,
            List.empty
          )(pos)
        )
      }

      test(s"$firstCommand YIELD transactionId AS txId $secondCommand") {
        assertAst(
          firstClause(None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(None, false, List.empty)(pos)
        )
      }

      test(s"$firstCommand $secondCommand YIELD transactionId AS txId") {
        assertAst(
          firstClause(None, false, List.empty)(defaultPos),
          secondClause(None, false, List(commandResultItem("transactionId", Some("txId"))))(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId")))
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand YIELD username"
      ) {
        assertAst(
          firstClause(None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(
            None,
            false,
            List(commandResultItem("username"))
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username")))
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand YIELD username RETURN txId, username"
      ) {
        assertAst(
          firstClause(None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(None, false, List(commandResultItem("username")))(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD * $secondCommand YIELD username RETURN txId, username"
      ) {
        assertAst(
          firstClause(None, true, List.empty)(defaultPos),
          withFromYield(returnAllItems),
          secondClause(None, false, List(commandResultItem("username")))(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand YIELD * RETURN txId, username"
      ) {
        assertAst(
          firstClause(None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(None, true, List.empty)(pos),
          withFromYield(returnAllItems),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD * $secondCommand YIELD * RETURN txId, username"
      ) {
        assertAst(
          firstClause(None, true, List.empty)(defaultPos),
          withFromYield(returnAllItems),
          secondClause(None, true, List.empty)(pos),
          withFromYield(returnAllItems),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId RETURN txId $secondCommand YIELD username RETURN txId, username"
      ) {
        assertAst(
          firstClause(None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          returnClause(returnItems(variableReturnItem("txId"))),
          secondClause(
            None,
            false,
            List(commandResultItem("username"))
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"""$firstCommand
           |YIELD transactionId AS txId, currentQuery, username AS user
           |$secondCommand
           |YIELD username, message
           |RETURN *""".stripMargin
      ) {
        assertAst(
          firstClause(
            None,
            false,
            List(
              commandResultItem("transactionId", Some("txId")),
              commandResultItem("currentQuery"),
              commandResultItem("username", Some("user"))
            )
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "currentQuery", "user"))),
          secondClause(
            None,
            false,
            List(
              commandResultItem("username"),
              commandResultItem("message")
            )
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username", "message"))),
          returnAll
        )
      }

      // more commands per query

      commandCombinationsAll.foreach { case (thirdCommand, thirdClause, fourthCommand, fourthClause) =>
        test(
          s"$firstCommand $secondCommand $thirdCommand $fourthCommand"
        ) {
          assertAst(
            firstClause(None, false, List.empty)(defaultPos),
            secondClause(None, false, List.empty)(pos),
            thirdClause(None, false, List.empty)(pos),
            fourthClause(None, false, List.empty)(pos)
          )
        }

        test(
          s"""$firstCommand
             |YIELD *
             |$secondCommand
             |YIELD *
             |$thirdCommand
             |YIELD *
             |$fourthCommand
             |YIELD *""".stripMargin
        ) {
          assertAst(
            firstClause(None, true, List.empty)(defaultPos),
            withFromYield(returnAllItems),
            secondClause(None, true, List.empty)(pos),
            withFromYield(returnAllItems),
            thirdClause(None, true, List.empty)(pos),
            withFromYield(returnAllItems),
            fourthClause(None, true, List.empty)(pos),
            withFromYield(returnAllItems)
          )
        }

        test(
          s"""$firstCommand
             |YIELD transactionId AS txId
             |$secondCommand
             |YIELD transactionId AS txId, username
             |$thirdCommand
             |YIELD transactionId AS txId
             |$fourthCommand
             |YIELD transactionId AS txId, message AS status
             |RETURN *""".stripMargin
        ) {
          assertAst(
            firstClause(
              None,
              false,
              List(commandResultItem("transactionId", Some("txId")))
            )(defaultPos),
            withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
            secondClause(
              None,
              false,
              List(
                commandResultItem("transactionId", Some("txId")),
                commandResultItem("username")
              )
            )(pos),
            withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "username"))),
            thirdClause(
              None,
              false,
              List(commandResultItem("transactionId", Some("txId")))
            )(pos),
            withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
            fourthClause(
              None,
              false,
              List(
                commandResultItem("transactionId", Some("txId")),
                commandResultItem("message", Some("status"))
              )
            )(pos),
            withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "status"))),
            returnAll
          )
        }

        test(
          s"$firstCommand WHERE message = 'Transaction terminated.' " +
            s"$secondCommand WHERE message = 'Transaction terminated.' " +
            s"$thirdCommand WHERE message = 'Transaction terminated.' " +
            s"$fourthCommand WHERE message = 'Transaction terminated.'"
        ) {
          // Can't have multiline query as I need the where positions
          val where1Pos = getWherePosition()
          val where2Pos = getWherePosition(where1Pos.offset + 1)
          val where3Pos = getWherePosition(where2Pos.offset + 1)
          val where4Pos = getWherePosition(where3Pos.offset + 1)
          assertAst(
            firstClause(
              Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where1Pos)),
              false,
              List.empty
            )(defaultPos),
            secondClause(
              Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where2Pos)),
              false,
              List.empty
            )(pos),
            thirdClause(
              Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where3Pos)),
              false,
              List.empty
            )(pos),
            fourthClause(
              Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where4Pos)),
              false,
              List.empty
            )(pos)
          )
        }
      }
  }

  commandCombinationsAllowingStringExpressions.foreach {
    case (firstCommand, firstClause, secondCommand, secondClause) =>
      test(s"$firstCommand ${secondCommand}S 'db1-transaction-123'") {
        assertAst(singleQuery(
          firstClause(Left(List.empty), None, false, List.empty)(defaultPos),
          secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
        ))
      }

      test(s"$firstCommand 'db1-transaction-123' $secondCommand") {
        assertAst(singleQuery(
          firstClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(defaultPos),
          secondClause(Left(List.empty), None, false, List.empty)(pos)
        ))
      }

      test(s"$firstCommand 'db1-transaction-123' $secondCommand 'db1-transaction-123'") {
        assertAst(singleQuery(
          firstClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(defaultPos),
          secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
        ))
      }

      test(
        s"$firstCommand 'db1-transaction-123', 'db1-transaction-123' $secondCommand 'db1-transaction-123', 'db1-transaction-123'"
      ) {
        assertAst(singleQuery(
          firstClause(Left(List("db1-transaction-123", "db1-transaction-123")), None, false, List.empty)(defaultPos),
          secondClause(Left(List("db1-transaction-123", "db1-transaction-123")), None, false, List.empty)(pos)
        ))
      }

      test(s"$firstCommand $$txId $secondCommand $$txId") {
        assertAst(singleQuery(
          firstClause(Right(parameter("txId", CTAny)), None, false, List.empty)(defaultPos),
          secondClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos)
        ))
      }

      test(s"$firstCommand WHERE transactionId = '123' $secondCommand 'db1-transaction-123'") {
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty
          )(defaultPos),
          secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
        ))
      }

      test(s"$firstCommand $secondCommand 'db1-transaction-123' WHERE transactionId = '123'") {
        assertAst(singleQuery(
          firstClause(Left(List.empty), None, false, List.empty)(defaultPos),
          secondClause(
            Right(literalString("db1-transaction-123")),
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty
          )(pos)
        ))
      }

      test(
        s"$firstCommand WHERE transactionId = '123' $secondCommand 'db1-transaction-123' WHERE transactionId = '123'"
      ) {
        val where1Pos = getWherePosition()
        val where2Pos = getWherePosition(where1Pos.offset + 1)
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            Some((where(equals(varFor("transactionId"), literalString("123"))), where1Pos)),
            false,
            List.empty
          )(defaultPos),
          secondClause(
            Right(literalString("db1-transaction-123")),
            Some((where(equals(varFor("transactionId"), literalString("123"))), where2Pos)),
            false,
            List.empty
          )(pos)
        ))
      }

      test(s"$firstCommand YIELD transactionId AS txId $secondCommand 'db1-transaction-123'") {
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
        ))
      }

      test(s"$firstCommand $secondCommand 'db1-transaction-123' YIELD transactionId AS txId") {
        assertAst(singleQuery(
          firstClause(Left(List.empty), None, false, List.empty)(defaultPos),
          secondClause(
            Right(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId")))
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId")))
        ))
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand 'db1-transaction-123' YIELD username"
      ) {
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(
            Right(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("username"))
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username")))
        ))
      }

      test(
        s"$firstCommand YIELD transactionId AS txId RETURN txId $secondCommand 'db1-transaction-123' YIELD username RETURN txId, username"
      ) {
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          returnClause(returnItems(variableReturnItem("txId"))),
          secondClause(
            Right(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("username"))
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        ))
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand 'db1-transaction-123' YIELD username RETURN txId, username"
      ) {
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(
            Right(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("username"))
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        ))
      }

      test(
        s"""$firstCommand 'db1-transaction-123'
           |YIELD transactionId AS txId, currentQuery, username AS user
           |$secondCommand 'db1-transaction-123'
           |YIELD username, message
           |RETURN *""".stripMargin
      ) {
        assertAst(singleQuery(
          firstClause(
            Right(literalString("db1-transaction-123")),
            None,
            false,
            List(
              commandResultItem("transactionId", Some("txId")),
              commandResultItem("currentQuery"),
              commandResultItem("username", Some("user"))
            )
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "currentQuery", "user"))),
          secondClause(
            Right(literalString("db1-transaction-123")),
            None,
            false,
            List(
              commandResultItem("username"),
              commandResultItem("message")
            )
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username", "message"))),
          returnAll
        ))
      }

      // more commands per query

      commandCombinationsAllowingStringExpressions.foreach {
        case (thirdCommand, thirdClause, fourthCommand, fourthClause) =>
          test(
            s"""$firstCommand 'db1-transaction-123'
               |${secondCommand}S 'db1-transaction-123'
               |$thirdCommand 'db1-transaction-123'
               |${fourthCommand}S 'db1-transaction-123'""".stripMargin
          ) {
            assertAst(singleQuery(
              firstClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(defaultPos),
              secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos),
              thirdClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos),
              fourthClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
            ))
          }

          test(
            s"${firstCommand}S $$txId $secondCommand $$txId ${thirdCommand}S $$txId $fourthCommand $$txId"
          ) {
            assertAst(singleQuery(
              firstClause(Right(parameter("txId", CTAny)), None, false, List.empty)(defaultPos),
              secondClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos),
              thirdClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos),
              fourthClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos)
            ))
          }

          test(
            s"""${firstCommand}S 'db1-transaction-123'
               |YIELD *
               |$secondCommand 'db1-transaction-123'
               |YIELD *
               |${thirdCommand}S 'db1-transaction-123'
               |YIELD *
               |$fourthCommand 'db1-transaction-123'
               |YIELD *""".stripMargin
          ) {
            assertAst(singleQuery(
              firstClause(Right(literalString("db1-transaction-123")), None, true, List.empty)(defaultPos),
              withFromYield(returnAllItems),
              secondClause(Right(literalString("db1-transaction-123")), None, true, List.empty)(pos),
              withFromYield(returnAllItems),
              thirdClause(Right(literalString("db1-transaction-123")), None, true, List.empty)(pos),
              withFromYield(returnAllItems),
              fourthClause(Right(literalString("db1-transaction-123")), None, true, List.empty)(pos),
              withFromYield(returnAllItems)
            ))
          }

          test(
            s"""${firstCommand}S 'db1-transaction-123'
               |YIELD transactionId AS txId
               |$secondCommand 'db1-transaction-123'
               |YIELD transactionId AS txId, username
               |${thirdCommand}S 'db1-transaction-123'
               |YIELD transactionId AS txId
               |$fourthCommand 'db1-transaction-123'
               |YIELD transactionId AS txId, message AS status
               |RETURN *""".stripMargin
          ) {
            assertAst(singleQuery(
              firstClause(
                Right(literalString("db1-transaction-123")),
                None,
                false,
                List(commandResultItem("transactionId", Some("txId")))
              )(defaultPos),
              withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
              secondClause(
                Right(literalString("db1-transaction-123")),
                None,
                false,
                List(
                  commandResultItem("transactionId", Some("txId")),
                  commandResultItem("username")
                )
              )(pos),
              withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "username"))),
              thirdClause(
                Right(literalString("db1-transaction-123")),
                None,
                false,
                List(commandResultItem("transactionId", Some("txId")))
              )(pos),
              withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
              fourthClause(
                Right(literalString("db1-transaction-123")),
                None,
                false,
                List(
                  commandResultItem("transactionId", Some("txId")),
                  commandResultItem("message", Some("status"))
                )
              )(pos),
              withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "status"))),
              returnAll
            ))
          }

          test(
            s"${firstCommand}S 'db1-transaction-123' WHERE message = 'Transaction terminated.' " +
              s"$secondCommand 'db1-transaction-123' WHERE message = 'Transaction terminated.' " +
              s"${thirdCommand}S 'db1-transaction-123' WHERE message = 'Transaction terminated.' " +
              s"$fourthCommand 'db1-transaction-123' WHERE message = 'Transaction terminated.'"
          ) {
            // Can't have multiline query as I need the where positions
            val where1Pos = getWherePosition()
            val where2Pos = getWherePosition(where1Pos.offset + 1)
            val where3Pos = getWherePosition(where2Pos.offset + 1)
            val where4Pos = getWherePosition(where3Pos.offset + 1)
            assertAst(singleQuery(
              firstClause(
                Right(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where1Pos)),
                false,
                List.empty
              )(defaultPos),
              secondClause(
                Right(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where2Pos)),
                false,
                List.empty
              )(pos),
              thirdClause(
                Right(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where3Pos)),
                false,
                List.empty
              )(pos),
              fourthClause(
                Right(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where4Pos)),
                false,
                List.empty
              )(pos)
            ))
          }
      }

      // general expression and not just string/param

      test(s"${firstCommand}S YIELD transactionId AS txId $secondCommand txId") {
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(Right(varFor("txId")), None, false, List.empty)(pos)
        ))
      }

      test(s"${firstCommand}S foo YIELD transactionId AS show $secondCommand show") {
        assertAst(singleQuery(
          firstClause(
            Right(varFor("foo")),
            None,
            false,
            List(commandResultItem("transactionId", Some("show")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("show"))),
          secondClause(Right(varFor("show")), None, false, List.empty)(pos)
        ))
      }

      test(
        s"${firstCommand}S ['db1-transaction-123', 'db2-transaction-456'] YIELD transactionId AS show $secondCommand show"
      ) {
        assertAst(singleQuery(
          firstClause(
            Right(listOfString("db1-transaction-123", "db2-transaction-456")),
            None,
            false,
            List(commandResultItem("transactionId", Some("show")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("show"))),
          secondClause(Right(varFor("show")), None, false, List.empty)(pos)
        ))
      }

      test(s"${firstCommand}S YIELD transactionId AS txId $secondCommand txId + '123'") {
        assertAst(singleQuery(
          firstClause(
            Left(List.empty),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
          secondClause(Right(add(varFor("txId"), literalString("123"))), None, false, List.empty)(pos)
        ))
      }

      test(s"${firstCommand}S yield YIELD transactionId AS show $secondCommand show") {
        assertAst(singleQuery(
          firstClause(
            Right(varFor("yield")),
            None,
            false,
            List(commandResultItem("transactionId", Some("show")))
          )(defaultPos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("show"))),
          secondClause(Right(varFor("show")), None, false, List.empty)(pos)
        ))
      }
  }

  test(
    """USE test
      |SHOW TRANSACTIONS ""
      |YIELD *
      |SHOW TRANSACTIONS "", "", ""
      |YIELD *
      |SHOW TRANSACTIONS `콺`
      |YIELD `碌`, `脃`, `麪`
      |  ORDER BY NULL ASCENDING
      |  SKIP 1
      |  WHERE 0o1
      |RETURN *, ("") IS NOT NULL AS `ጤ`
      |  ORDER BY -0x1 DESCENDING, `怭` DESCENDING
      |  SKIP 1.9121409685506285E89
      |  LIMIT NULL""".stripMargin
  ) {
    // From astGenerator, it wasn't a parsing problem
    // but now I have already added the test to check that so it can stay :shrug:
    assertAst(
      singleQuery(
        use(List("test")),
        ast.ShowTransactionsClause(Right(literalString("")), None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems),
        ast.ShowTransactionsClause(Left(List("", "", "")), None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems),
        ast.ShowTransactionsClause(
          Right(varFor("콺")),
          None,
          List(commandResultItem("碌"), commandResultItem("脃"), commandResultItem("麪")),
          yieldAll = false
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("碌", "脃", "麪")),
          Some(orderBy(sortItem(nullLiteral))),
          Some(skip(1)),
          where = Some(where(SignedOctalIntegerLiteral("0o1")(pos)))
        ),
        returnClause(
          returnItems(
            ast.AliasedReturnItem(isNotNull(literalString("")), varFor("ጤ"))(pos)
          ).withExisting(true),
          Some(orderBy(
            ast.DescSortItem(SignedHexIntegerLiteral("-0x1")(pos))(pos),
            ast.DescSortItem(varFor("怭"))(pos)
          )),
          Some(ast.Limit(nullLiteral)(pos)),
          skip = Some(ast.Skip(literalFloat(1.9121409685506285E89))(pos))
        )
      ),
      comparePosition = false
    )
  }

  test(
    "SHOW TRANSACTIONS YIELD a1, b1 AS c1, d1 AS d1, e1 AS f1, g1 AS e1 ORDER BY a1, b1, d1, e1 WHERE a1 AND b1 AND d1 AND e1 " +
      "TERMINATE TRANSACTIONS YIELD a2, b2 AS c2, d2 AS d2, e2 AS f2, g2 AS e2 ORDER BY a2, b2, d2, e2 WHERE a2 AND b2 AND d2 AND e2 " +
      "SHOW SETTINGS YIELD a3, b3 AS c3, d3 AS d3, e3 AS f3, g3 AS e3 ORDER BY a3, b3, d3, e3 WHERE a3 AND b3 AND d3 AND e3 " +
      "SHOW FUNCTIONS YIELD a4, b4 AS c4, d4 AS d4, e4 AS f4, g4 AS e4 ORDER BY a4, b4, d4, e4 WHERE a4 AND b4 AND d4 AND e4 " +
      "SHOW PROCEDURES YIELD a5, b5 AS c5, d5 AS d5, e5 AS f5, g5 AS e5 ORDER BY a5, b5, d5, e5 WHERE a5 AND b5 AND d5 AND e5 " +
      "SHOW INDEXES YIELD a6, b6 AS c6, d6 AS d6, e6 AS f6, g6 AS e6 ORDER BY a6, b6, d6, e6 WHERE a6 AND b6 AND d6 AND e6 " +
      "SHOW CONSTRAINTS YIELD a7, b7 AS c7, d7 AS d7, e7 AS f7, g7 AS e7 ORDER BY a7, b7, d7, e7 WHERE a7 AND b7 AND d7 AND e7 " +
      "RETURN *"
  ) {
    assertAst(
      showTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(
          commandResultItem("a1"),
          commandResultItem("b1", Some("c1")),
          commandResultItem("d1", Some("d1")),
          commandResultItem("e1", Some("f1")),
          commandResultItem("g1", Some("e1"))
        )
      ),
      withFromYield(
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
      ),
      terminateTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(
          commandResultItem("a2"),
          commandResultItem("b2", Some("c2")),
          commandResultItem("d2", Some("d2")),
          commandResultItem("e2", Some("f2")),
          commandResultItem("g2", Some("e2"))
        )
      ),
      withFromYield(
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
      ),
      showSetting(
        Left(List.empty),
        None,
        yieldAll = false,
        List(
          commandResultItem("a3"),
          commandResultItem("b3", Some("c3")),
          commandResultItem("d3", Some("d3")),
          commandResultItem("e3", Some("f3")),
          commandResultItem("g3", Some("e3"))
        )
      ),
      withFromYield(
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
      ),
      showFunction(
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
        )
      ),
      withFromYield(
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
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(
          commandResultItem("a5"),
          commandResultItem("b5", Some("c5")),
          commandResultItem("d5", Some("d5")),
          commandResultItem("e5", Some("f5")),
          commandResultItem("g5", Some("e5"))
        )
      ),
      withFromYield(
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
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(
          commandResultItem("a6"),
          commandResultItem("b6", Some("c6")),
          commandResultItem("d6", Some("d6")),
          commandResultItem("e6", Some("f6")),
          commandResultItem("g6", Some("e6"))
        )
      ),
      withFromYield(
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
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(
          commandResultItem("a7"),
          commandResultItem("b7", Some("c7")),
          commandResultItem("d7", Some("d7")),
          commandResultItem("e7", Some("f7")),
          commandResultItem("g7", Some("e7"))
        )
      ),
      withFromYield(
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
      ),
      returnAll
    )
  }

  test(
    "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "RETURN *"
  ) {
    assertAst(
      showTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      terminateTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showSetting(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      terminateTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showSetting(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      terminateTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showSetting(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      terminateTx(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showSetting(
        Left(List.empty),
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a"))
      ),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))),
      returnAll
    )
  }

  private val manyCommands = (for (_ <- 1 to 300) yield "SHOW TRANSACTIONS YIELD a").mkString(" ")

  test(manyCommands) {
    val clauses = (for (_ <- 1 to 300) yield List(
      showTx(Left(List.empty), None, yieldAll = false, List(commandResultItem("a")))(pos),
      withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a")))
    )).flatten
    assertAst(singleQuery(clauses: _*))
  }

  // show indexes/constraints brief/verbose when combined with other commands

  test("SHOW CONSTRAINTS BRIEF SHOW CONSTRAINTS") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW CONSTRAINTS VERBOSE SHOW CONSTRAINTS") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW CONSTRAINTS SHOW CONSTRAINTS BRIEF") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW CONSTRAINTS SHOW CONSTRAINTS VERBOSE") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW CONSTRAINTS BRIEF SHOW CONSTRAINTS VERBOSE") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW CONSTRAINTS BRIEF SHOW PROCEDURES") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW CONSTRAINTS VERBOSE SHOW FUNCTIONS") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW FUNCTIONS SHOW CONSTRAINTS BRIEF") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW PROCEDURES SHOW CONSTRAINTS VERBOSE") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW INDEXES BRIEF SHOW INDEXES") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW INDEXES VERBOSE SHOW INDEXES") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW INDEXES SHOW INDEXES BRIEF") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW INDEXES SHOW INDEXES VERBOSE") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW BTREE INDEXES VERBOSE SHOW BTREE INDEXES BRIEF") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW INDEXES BRIEF SHOW PROCEDURES") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW INDEXES VERBOSE SHOW FUNCTIONS") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  test("SHOW FUNCTIONS SHOW INDEXES BRIEF") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'BRIEF': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW PROCEDURES SHOW INDEXES VERBOSE") {
    failsParsing[ast.Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
              |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            "Invalid input 'VERBOSE': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  // combined with other commands

  Seq(
    "MATCH (n) RETURN n",
    "WITH 1 AS x",
    "UNWIND [1, 2, 3] AS id",
    "SHOW USERS"
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

  })

  test("SHOW TRANSACTIONS MATCH (n)") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(function("MATCH", varFor("n"))), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("MATCH (n) TERMINATE TRANSACTION") {
    failsParsing[ast.Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input")
      case _ => _.withSyntaxError(
          """Invalid input 'TERMINATE': expected a graph pattern, 'FOREACH', ',', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'USING', 'WHERE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
            |"MATCH (n) TERMINATE TRANSACTION"
            |           ^""".stripMargin
        )
    }
  }

}
