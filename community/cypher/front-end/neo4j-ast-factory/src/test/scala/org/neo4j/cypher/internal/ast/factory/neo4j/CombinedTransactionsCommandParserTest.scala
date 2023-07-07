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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny

/* Tests for combining listing and terminating transactions */
class CombinedTransactionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  private type TransactionClause =
    (
      Either[List[String], Expression],
      Option[(ast.Where, InputPosition)],
      Boolean,
      List[ast.CommandResultItem]
    ) => InputPosition => ast.CommandClause

  private def show(
    ids: Either[List[String], Expression],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.ShowTransactionsClause(ids, where.map(_._1), yieldItems, yieldAll)

  private def terminate(
    ids: Either[List[String], Expression],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem]
  ): InputPosition => ast.CommandClause =
    ast.TerminateTransactionsClause(ids, yieldItems, yieldAll, where.map(_._2))

  private def getWherePosition(startIndex: Int = 0) = {
    val startOfWhereClause = testName.indexOf("WHERE", startIndex)
    InputPosition(startOfWhereClause, 1, startOfWhereClause + 1)
  }

  Seq(
    ("SHOW", show: TransactionClause, "SHOW", show: TransactionClause),
    ("SHOW", show: TransactionClause, "TERMINATE", terminate: TransactionClause),
    ("TERMINATE", terminate: TransactionClause, "SHOW", show: TransactionClause),
    ("TERMINATE", terminate: TransactionClause, "TERMINATE", terminate: TransactionClause)
  ).foreach { case (firstCommand, firstClause, secondCommand, secondClause) =>
    test(s"$firstCommand TRANSACTIONS $secondCommand TRANSACTION") {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List.empty)(defaultPos),
        secondClause(Left(List.empty), None, false, List.empty)(pos)
      ))
    }

    test(s"USE db $firstCommand TRANSACTIONS $secondCommand TRANSACTION") {
      assertAst(
        singleQuery(
          use(varFor("db")),
          firstClause(Left(List.empty), None, false, List.empty)(pos),
          secondClause(Left(List.empty), None, false, List.empty)(pos)
        ),
        comparePosition = false
      )
    }

    test(s"$firstCommand TRANSACTIONS $secondCommand TRANSACTION 'db1-transaction-123'") {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List.empty)(defaultPos),
        secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
      ))
    }

    test(s"$firstCommand TRANSACTIONS 'db1-transaction-123' $secondCommand TRANSACTION") {
      assertAst(singleQuery(
        firstClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(defaultPos),
        secondClause(Left(List.empty), None, false, List.empty)(pos)
      ))
    }

    test(s"$firstCommand TRANSACTIONS 'db1-transaction-123' $secondCommand TRANSACTION 'db1-transaction-123'") {
      assertAst(singleQuery(
        firstClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(defaultPos),
        secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
      ))
    }

    test(
      s"$firstCommand TRANSACTIONS 'db1-transaction-123', 'db1-transaction-123' $secondCommand TRANSACTION 'db1-transaction-123', 'db1-transaction-123'"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List("db1-transaction-123", "db1-transaction-123")), None, false, List.empty)(defaultPos),
        secondClause(Left(List("db1-transaction-123", "db1-transaction-123")), None, false, List.empty)(pos)
      ))
    }

    test(s"$firstCommand TRANSACTIONS $$txId $secondCommand TRANSACTION $$txId") {
      assertAst(singleQuery(
        firstClause(Right(parameter("txId", CTAny)), None, false, List.empty)(defaultPos),
        secondClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos)
      ))
    }

    test(s"$firstCommand TRANSACTIONS WHERE transactionId = '123' $secondCommand TRANSACTION 'db1-transaction-123'") {
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

    test(s"$firstCommand TRANSACTIONS $secondCommand TRANSACTION 'db1-transaction-123' WHERE transactionId = '123'") {
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
      s"$firstCommand TRANSACTIONS WHERE transactionId = '123' $secondCommand TRANSACTION 'db1-transaction-123' WHERE transactionId = '123'"
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

    test(s"$firstCommand TRANSACTIONS YIELD transactionId AS txId $secondCommand TRANSACTION 'db1-transaction-123'") {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
        secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
      ))
    }

    test(
      s"$firstCommand TRANSACTIONS YIELD transactionId AS txId $secondCommand TRANSACTION 'db1-transaction-123' YIELD username"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
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
      s"$firstCommand TRANSACTIONS YIELD transactionId AS txId RETURN txId $secondCommand TRANSACTION 'db1-transaction-123' YIELD username RETURN txId, username"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
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
      s"$firstCommand TRANSACTIONS YIELD transactionId AS txId $secondCommand TRANSACTION 'db1-transaction-123' YIELD username RETURN txId, username"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
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
      s"$firstCommand TRANSACTIONS YIELD transactionId AS txId $secondCommand TRANSACTION YIELD username RETURN txId, username"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
        secondClause(Left(List.empty), None, false, List(commandResultItem("username")))(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))),
        returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
      ))
    }

    test(
      s"$firstCommand TRANSACTIONS YIELD * $secondCommand TRANSACTION YIELD username RETURN txId, username"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, true, List.empty)(defaultPos),
        withFromYield(returnAllItems),
        secondClause(Left(List.empty), None, false, List(commandResultItem("username")))(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))),
        returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
      ))
    }

    test(
      s"$firstCommand TRANSACTIONS YIELD transactionId AS txId $secondCommand TRANSACTION YIELD * RETURN txId, username"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
        secondClause(Left(List.empty), None, true, List.empty)(pos),
        withFromYield(returnAllItems),
        returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
      ))
    }

    test(
      s"$firstCommand TRANSACTIONS YIELD * $secondCommand TRANSACTION YIELD * RETURN txId, username"
    ) {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, true, List.empty)(defaultPos),
        withFromYield(returnAllItems),
        secondClause(Left(List.empty), None, true, List.empty)(pos),
        withFromYield(returnAllItems),
        returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
      ))
    }

    test(
      s"""$firstCommand TRANSACTION 'db1-transaction-123'
         |YIELD transactionId AS txId, currentQuery, username AS user
         |$secondCommand TRANSACTION 'db1-transaction-123'
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

    Seq(
      ("SHOW", show: TransactionClause, "SHOW", show: TransactionClause),
      ("SHOW", show: TransactionClause, "TERMINATE", terminate: TransactionClause),
      ("TERMINATE", terminate: TransactionClause, "SHOW", show: TransactionClause),
      ("TERMINATE", terminate: TransactionClause, "TERMINATE", terminate: TransactionClause)
    ).foreach { case (thirdCommand, thirdClause, fourthCommand, fourthClause) =>
      test(
        s"$firstCommand TRANSACTIONS $secondCommand TRANSACTION $thirdCommand TRANSACTIONS $fourthCommand TRANSACTION"
      ) {
        assertAst(singleQuery(
          firstClause(Left(List.empty), None, false, List.empty)(defaultPos),
          secondClause(Left(List.empty), None, false, List.empty)(pos),
          thirdClause(Left(List.empty), None, false, List.empty)(pos),
          fourthClause(Left(List.empty), None, false, List.empty)(pos)
        ))
      }

      test(
        s"""$firstCommand TRANSACTIONS 'db1-transaction-123'
           |$secondCommand TRANSACTION 'db1-transaction-123'
           |$thirdCommand TRANSACTIONS 'db1-transaction-123'
           |$fourthCommand TRANSACTION 'db1-transaction-123'""".stripMargin
      ) {
        assertAst(singleQuery(
          firstClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(defaultPos),
          secondClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos),
          thirdClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos),
          fourthClause(Right(literalString("db1-transaction-123")), None, false, List.empty)(pos)
        ))
      }

      test(
        s"$firstCommand TRANSACTIONS $$txId $secondCommand TRANSACTION $$txId $thirdCommand TRANSACTIONS $$txId $fourthCommand TRANSACTION $$txId"
      ) {
        assertAst(singleQuery(
          firstClause(Right(parameter("txId", CTAny)), None, false, List.empty)(defaultPos),
          secondClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos),
          thirdClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos),
          fourthClause(Right(parameter("txId", CTAny)), None, false, List.empty)(pos)
        ))
      }

      test(
        s"""$firstCommand TRANSACTIONS 'db1-transaction-123'
           |YIELD *
           |$secondCommand TRANSACTION 'db1-transaction-123'
           |YIELD *
           |$thirdCommand TRANSACTIONS 'db1-transaction-123'
           |YIELD *
           |$fourthCommand TRANSACTION 'db1-transaction-123'
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
        s"""$firstCommand TRANSACTIONS 'db1-transaction-123'
           |YIELD transactionId AS txId
           |$secondCommand TRANSACTION 'db1-transaction-123'
           |YIELD transactionId AS txId, username
           |$thirdCommand TRANSACTIONS 'db1-transaction-123'
           |YIELD transactionId AS txId
           |$fourthCommand TRANSACTION 'db1-transaction-123'
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
        s"$firstCommand TRANSACTIONS 'db1-transaction-123' WHERE message = 'Transaction terminated.' " +
          s"$secondCommand TRANSACTION 'db1-transaction-123' WHERE message = 'Transaction terminated.' " +
          s"$thirdCommand TRANSACTIONS 'db1-transaction-123' WHERE message = 'Transaction terminated.' " +
          s"$fourthCommand TRANSACTION 'db1-transaction-123' WHERE message = 'Transaction terminated.'"
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

    test(s"$firstCommand TRANSACTIONS YIELD transactionId AS txId $secondCommand TRANSACTION txId") {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
        secondClause(Right(varFor("txId")), None, false, List.empty)(pos)
      ))
    }

    test(s"$firstCommand TRANSACTIONS foo YIELD transactionId AS show $secondCommand TRANSACTION show") {
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
      s"$firstCommand TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'] YIELD transactionId AS show $secondCommand TRANSACTION show"
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

    test(s"$firstCommand TRANSACTIONS YIELD transactionId AS txId $secondCommand TRANSACTION txId + '123'") {
      assertAst(singleQuery(
        firstClause(Left(List.empty), None, false, List(commandResultItem("transactionId", Some("txId"))))(defaultPos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))),
        secondClause(Right(add(varFor("txId"), literalString("123"))), None, false, List.empty)(pos)
      ))
    }

    test(s"$firstCommand TRANSACTIONS yield YIELD transactionId AS show $secondCommand TRANSACTION show") {
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
    """USE -4.918690900648941E76
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
        use(literalFloat(-4.918690900648941E76)),
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

  // combined with other commands

  private val otherShowCommands = Seq(
    "SHOW PROCEDURES",
    "SHOW FUNCTIONS",
    "SHOW INDEXES",
    "SHOW CONSTRAINTS"
  )

  (otherShowCommands ++ Seq(
    "MATCH (n) RETURN n",
    "WITH 1 AS x",
    "UNWIND [1, 2, 3] AS id",
    "SHOW USERS"
  )).foreach(otherClause => {

    test(s"SHOW TRANSACTIONS $otherClause") {
      failsToParse
    }

    test(s"$otherClause SHOW TRANSACTIONS") {
      failsToParse
    }

    test(s"TERMINATE TRANSACTIONS $otherClause") {
      failsToParse
    }

    test(s"$otherClause TERMINATE TRANSACTIONS") {
      failsToParse
    }

  })

  otherShowCommands.foreach(firstClause => {
    otherShowCommands.foreach(secondClause => {
      test(s"$firstClause $secondClause") {
        failsToParse
      }
    })
  })

  test("SHOW TRANSACTIONS MATCH (n)") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(function("MATCH", varFor("n"))), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("MATCH (n) TERMINATE TRANSACTION") {
    failsToParse
  }

}
