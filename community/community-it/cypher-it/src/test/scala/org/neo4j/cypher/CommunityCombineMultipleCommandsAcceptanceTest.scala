/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.InvalidSyntaxStatus
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.Reparsesable_42I67
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlException
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.cypher.util.DontRunOnSpdBuild
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.exceptions.NotSystemDatabaseException
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.test.DoubleLatch

// Not needed to run with SPD, already have an enterprise version of this test: CombineMultipleCommandsAcceptanceTest
class CommunityCombineMultipleCommandsAcceptanceTest extends CommunityCombineCommandsAcceptanceTestBase
    with DontRunOnSpdBuild {
  // Tests for combining listing and terminating commands

  test("Should show and terminate transaction with id from show") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId
           |TERMINATE TRANSACTION transactionId
           |YIELD message, transactionId AS txId, username
           |RETURN *""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "message" -> "Transaction terminated.",
        "txId" -> unwindTransactionId,
        "transactionId" -> unwindTransactionId,
        "username" -> username
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should terminate and show transaction with id from terminate") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""TERMINATE TRANSACTION '$unwindTransactionId'
           |YIELD message, transactionId AS txId, username
           |SHOW TRANSACTION txId
           |YIELD transactionId, username AS user
           |RETURN *""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "message" -> "Transaction terminated.",
        "txId" -> unwindTransactionId,
        "transactionId" -> unwindTransactionId,
        "username" -> username,
        "user" -> username
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should terminate and show transaction - different users") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    try {
      val user1TxId = getTransactionIdExecutingQuery(user1Query)
      val user2TxId = getTransactionIdExecutingQuery(user2Query)

      // WHEN
      val result = execute(
        s"""TERMINATE TRANSACTION '$user1TxId'
           |YIELD message, transactionId AS txId, username
           |SHOW TRANSACTION '$user2TxId'
           |YIELD transactionId, username AS user
           |RETURN *""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "message" -> "Transaction terminated.",
        "txId" -> user1TxId,
        "transactionId" -> user2TxId,
        "username" -> username,
        "user" -> username2
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show, terminate and show transaction with id from previous clauses") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId
           |TERMINATE TRANSACTION transactionId
           |YIELD message, transactionId AS txId, username
           |SHOW TRANSACTION txId
           |YIELD username AS user
           |RETURN *""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "message" -> "Transaction terminated.",
        "txId" -> unwindTransactionId,
        "transactionId" -> unwindTransactionId,
        "username" -> username,
        "user" -> username
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show and terminate transaction with specific return") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId
           |TERMINATE TRANSACTION transactionId
           |YIELD message AS m, transactionId AS txId, username
           |RETURN m AS username, txId, transactionId""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "username" -> "Transaction terminated.",
        "txId" -> unwindTransactionId,
        "transactionId" -> unwindTransactionId
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show transactions and show settings") {
    // GIVEN
    val expectedSetting = allSettings(graph).head
    val (unwindQuery, latch) = setupUserWithOneTransaction(Map("setting" -> expectedSetting("name")))

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, parameters
           |SHOW SETTING parameters.setting
           |YIELD name, value
           |RETURN txId, name, value""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "txId" -> unwindTransactionId,
        "name" -> expectedSetting("name"),
        "value" -> expectedSetting("value")
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show settings and terminate transactions") {
    // GIVEN
    val expectedSetting = allSettings(graph).head
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW SETTING '${expectedSetting("name")}'
           |YIELD name, value
           |TERMINATE TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, message
           |RETURN *""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "name" -> expectedSetting("name"),
        "value" -> expectedSetting("value"),
        "txId" -> unwindTransactionId,
        "message" -> "Transaction terminated."
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show transactions and show functions") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId
           |SHOW USER DEFINED FUNCTIONS
           |YIELD name
           |RETURN txId, name""".stripMargin
      ).toList

      // THEN
      val expected = userDefinedFunctionsNames.map(fName =>
        Map(
          "txId" -> unwindTransactionId,
          "name" -> fName
        )
      )
      result should be(expected)
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show functions and terminate transactions") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW BUILT IN FUNCTIONS
           |YIELD name
           |TERMINATE TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, message
           |RETURN *""".stripMargin
      ).toList

      // THEN
      val expected = builtInFunctionsNames.map(fName =>
        Map(
          "name" -> fName,
          "txId" -> unwindTransactionId,
          "message" -> "Transaction terminated."
        )
      )
      result should be(expected)
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show functions and show settings") {
    // GIVEN
    val expectedSetting = allSettings(graph).head

    val result = execute(
      s"""SHOW FUNCTIONS
         |YIELD name AS function
         |SHOW SETTING '${expectedSetting("name")}'
         |YIELD name AS setting, value
         |RETURN *""".stripMargin
    ).toList

    // THEN
    val expected = allFunctionsNames.map(fName =>
      Map(
        "function" -> fName,
        "setting" -> expectedSetting("name"),
        "value" -> expectedSetting("value")
      )
    )
    result should be(expected)
  }

  test("Should show transactions and show procedures") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId
           |SHOW PROCEDURES
           |YIELD name
           |RETURN txId, name""".stripMargin
      ).toList

      // THEN
      val expected = allProceduresNames.map(pName =>
        Map(
          "txId" -> unwindTransactionId,
          "name" -> pName
        )
      )
      result should be(expected)
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show procedures and terminate transactions") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW PROCEDURES
           |YIELD name
           |TERMINATE TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, message
           |RETURN *""".stripMargin
      ).toList

      // THEN
      val expected = allProceduresNames.map(pName =>
        Map(
          "name" -> pName,
          "txId" -> unwindTransactionId,
          "message" -> "Transaction terminated."
        )
      )
      result should be(expected)
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show procedures and show settings") {
    // GIVEN
    val expectedSetting = allSettings(graph).head

    val result = execute(
      s"""SHOW PROCEDURES
         |YIELD name AS procedure
         |SHOW SETTING '${expectedSetting("name")}'
         |YIELD name AS setting, value
         |RETURN *""".stripMargin
    ).toList

    // THEN
    val expected = allProceduresNames.map(pName =>
      Map(
        "procedure" -> pName,
        "setting" -> expectedSetting("name"),
        "value" -> expectedSetting("value")
      )
    )
    result should be(expected)
  }

  test("Should show functions and show procedures") {
    // GIVEN
    val expectedProcedure = allProceduresNames.head

    val result = execute(
      s"""SHOW FUNCTIONS
         |YIELD name AS function
         |SHOW PROCEDURES
         |YIELD name AS procedure
         |WHERE procedure = '$expectedProcedure'
         |RETURN *""".stripMargin
    ).toList

    // THEN
    val expected = allFunctionsNames.map(fName =>
      Map(
        "function" -> fName,
        "procedure" -> expectedProcedure
      )
    )
    result should be(expected)
  }

  test("Should show transactions and show constraints") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()
    graph.createNodeUniquenessConstraintWithName("my_constraint", "L", "p")

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId
           |SHOW CONSTRAINTS
           |YIELD name
           |RETURN txId, name""".stripMargin
      ).toList

      // THEN
      result should be(List(Map("txId" -> unwindTransactionId, "name" -> "my_constraint")))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show constraints and terminate transactions") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()
    graph.createNodeUniquenessConstraintWithName("my_constraint", "L", "p")

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW CONSTRAINTS
           |YIELD name
           |TERMINATE TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, message
           |RETURN *""".stripMargin
      ).toList

      // THEN
      result should be(List(
        Map("name" -> "my_constraint", "txId" -> unwindTransactionId, "message" -> "Transaction terminated.")
      ))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show constraints and show settings") {
    // GIVEN
    graph.createNodeUniquenessConstraintWithName("my_constraint", "L", "p")
    val expectedSetting = allSettings(graph).head

    val result = execute(
      s"""SHOW CONSTRAINT
         |YIELD name AS constraint
         |SHOW SETTING '${expectedSetting("name")}'
         |YIELD name AS setting, value
         |RETURN *""".stripMargin
    ).toList

    // THEN
    result should be(List(
      Map("constraint" -> "my_constraint", "setting" -> expectedSetting("name"), "value" -> expectedSetting("value"))
    ))
  }

  test("Should show functions and show constraints") {
    // GIVEN
    graph.createNodeUniquenessConstraintWithName("my_constraint1", "L", "p1")
    graph.createNodeUniquenessConstraintWithName("my_constraint2", "L", "p2")

    val result = execute(
      s"""SHOW FUNCTIONS
         |YIELD name AS function
         |SHOW UNIQUENESS CONSTRAINTS
         |YIELD name AS constraint
         |WHERE constraint = 'my_constraint2'
         |RETURN *""".stripMargin
    ).toList

    // THEN
    val expected = allFunctionsNames.map(fName =>
      Map(
        "function" -> fName,
        "constraint" -> "my_constraint2"
      )
    )
    result should be(expected)
  }

  test("Should show constraints and show procedures") {
    // GIVEN
    graph.createNodeUniquenessConstraintWithName("my_constraint", "L", "p")
    val expectedProcedure = allProceduresNames.head

    val result = execute(
      s"""SHOW CONSTRAINTS
         |YIELD name AS constraint
         |SHOW PROCEDURES
         |YIELD name AS procedure
         |WHERE procedure = '$expectedProcedure'
         |RETURN *""".stripMargin
    ).toList

    // THEN
    result should be(List(
      Map(
        "constraint" -> "my_constraint",
        "procedure" -> expectedProcedure
      )
    ))
  }

  test("Should show transactions and show indexes") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()
    graph.createNodeIndexWithName("my_index", "L", "p")

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId
           |SHOW RANGE INDEXES
           |YIELD name
           |RETURN txId, name""".stripMargin
      ).toList

      // THEN
      result should be(List(Map("txId" -> unwindTransactionId, "name" -> "my_index")))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show indexes and terminate transactions") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()
    graph.createNodeIndexWithName("my_index", "L", "p")

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW RANGE INDEXES
           |YIELD name
           |TERMINATE TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, message
           |RETURN *""".stripMargin
      ).toList

      // THEN
      result should be(List(
        Map("name" -> "my_index", "txId" -> unwindTransactionId, "message" -> "Transaction terminated.")
      ))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show indexes and show settings") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "L", "p")
    val expectedSetting = allSettings(graph).head

    val result = execute(
      s"""SHOW INDEX
         |YIELD name AS index
         |WHERE index STARTS WITH 'my_'
         |SHOW SETTING '${expectedSetting("name")}'
         |YIELD name AS setting, value
         |RETURN *""".stripMargin
    ).toList

    // THEN
    result should be(List(
      Map("index" -> "my_index", "setting" -> expectedSetting("name"), "value" -> expectedSetting("value"))
    ))
  }

  test("Should show functions and show indexes") {
    // GIVEN
    graph.createNodeIndexWithName("my_index1", "L", "p1")
    graph.createNodeIndexWithName("my_index2", "L", "p2")

    val result = execute(
      s"""SHOW FUNCTIONS
         |YIELD name AS function
         |SHOW INDEXES
         |YIELD name AS index
         |WHERE index = 'my_index2'
         |RETURN *""".stripMargin
    ).toList

    // THEN
    val expected = allFunctionsNames.map(fName =>
      Map(
        "function" -> fName,
        "index" -> "my_index2"
      )
    )
    result should be(expected)
  }

  test("Should show indexes and show procedures") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "L", "p")
    val expectedProcedure = allProceduresNames.head

    val result = execute(
      s"""SHOW RANGE INDEXES
         |YIELD name AS index
         |SHOW PROCEDURES
         |YIELD name AS procedure
         |WHERE procedure = '$expectedProcedure'
         |RETURN *""".stripMargin
    ).toList

    // THEN
    result should be(List(
      Map(
        "index" -> "my_index",
        "procedure" -> expectedProcedure
      )
    ))
  }

  test("Should show constraints and show indexes") {
    // GIVEN
    graph.createNodeUniquenessConstraintWithName("my_constraint", "L", "p")
    graph.createRelationshipIndexWithName("my_index", "L", "p")

    val result = execute(
      s"""SHOW CONSTRAINTS
         |YIELD name AS constraint
         |SHOW INDEXES
         |YIELD name AS index, type
         |WHERE type <> 'LOOKUP'
         |RETURN constraint, index
         |ORDER BY index""".stripMargin
    ).toList

    // THEN
    result should be(List(
      Map(
        "constraint" -> "my_constraint",
        "index" -> "my_constraint"
      ),
      Map(
        "constraint" -> "my_constraint",
        "index" -> "my_index"
      )
    ))
  }

  test("Should show databases and procedures on system database") {
    // GIVEN
    val query =
      """SHOW PROCEDURES
        |  YIELD name AS proc
        |SHOW DATABASES
        |  YIELD name AS db, aliases
        |RETURN db, proc IN aliases AS procedureAlias
        |ORDER BY db, procedureAlias
        |""".stripMargin

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    val result = execute(query)

    // THEN
    result.toList should be(allProceduresNames.flatMap(_ =>
      List(
        Map[String, Any]("db" -> "neo4j", "procedureAlias" -> false),
        Map[String, Any]("db" -> "system", "procedureAlias" -> false)
      )
    ).sortBy(m => (m("db").asInstanceOf[String], m("procedureAlias").asInstanceOf[Boolean])))

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    val exception = the[NotSystemDatabaseException] thrownBy {
      execute(query)
    }

    // THEN
    exception should be(gqlException(
      "This is an administration command and it should be executed against the system database: SHOW DATABASES",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N28,
        "error: system configuration or operation exception - not supported by this database. This Cypher command must be executed against the database `system`."
      )
    ))
  }

  test("Should fail to show databases and show indexes due to requiring different databases") {
    // GIVEN
    val query =
      """SHOW HOME DATABASE
        |  YIELD name AS db
        |SHOW INDEXES
        |  YIELD name AS index
        |RETURN db, index
        |""".stripMargin

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    val exceptionSystemDb = the[InvalidSemanticsException] thrownBy {
      execute(query)
    }

    // THEN
    exceptionSystemDb should be(gqlException(
      s"The following commands are not allowed on a system database: SHOW INDEXES.",
      InvalidSyntaxStatus.withCause(gqlStatus(
        GqlStatusInfoCodes.STATUS_42N17,
        "error: syntax error or access rule violation - unsupported request. " +
          "'SHOW INDEXES' is not allowed on the system database."
      ).withCause(
        gqlStatus(
          GqlStatusInfoCodes.STATUS_42NA9,
          "error: syntax error or access rule violation - system database rules. " +
            "The system database supports a restricted set of Cypher clauses. " +
            "The supported clauses include procedure calls (if the procedure is allowed), a subset of show and terminate commands, and combinations of the two. " +
            "'YIELD' and 'RETURN' are also permitted when combined with procedure calls, show, or terminate commands."
        )
      ))
    ))

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    val exceptionDefaultDb = the[NotSystemDatabaseException] thrownBy {
      execute(query)
    }

    // THEN
    exceptionDefaultDb should be(gqlException(
      "This is an administration command and it should be executed against the system database: SHOW HOME DATABASE",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N28,
        "error: system configuration or operation exception - not supported by this database. This Cypher command must be executed against the database `system`."
      )
    ))
  }

  test("Should combine all show and terminate commands - user database") {
    // GIVEN
    val expectedSetting = allSettings(graph).head
    val expectedProcedure = allProceduresNames.head
    graph.createNodeUniquenessConstraintWithName("my_constraint", "L", "p")
    graph.createRelationshipIndexWithName("my_index", "L", "p")
    val (unwindQuery, latch) = setupUserWithOneTransaction(Map("setting" -> expectedSetting("name")))

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, parameters
           |SHOW PROCEDURES
           |YIELD name AS procedure
           |WHERE procedure = '$expectedProcedure'
           |SHOW RANGE INDEXES
           |YIELD name AS index, entityType, owningConstraint
           |WHERE owningConstraint IS NULL
           |SHOW SETTING parameters.setting
           |YIELD name AS setting, value
           |TERMINATE TRANSACTION txId
           |YIELD message
           |SHOW USER DEFINED FUNCTIONS EXECUTABLE
           |YIELD name AS function
           |WHERE function CONTAINS 'return'
           |SHOW CONSTRAINTS
           |YIELD name AS constraint, type
           |RETURN txId, procedure, setting, value, message, function, constraint, type AS constraintType, index, entityType""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "txId" -> unwindTransactionId,
        "procedure" -> expectedProcedure,
        "index" -> "my_index",
        "entityType" -> "RELATIONSHIP",
        "setting" -> expectedSetting("name"),
        "value" -> expectedSetting("value"),
        "message" -> "Transaction terminated.",
        "function" -> "test.return.latest",
        "constraint" -> "my_constraint",
        "constraintType" -> "NODE_PROPERTY_UNIQUENESS"
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should combine all show and terminate commands - system database") {
    // GIVEN
    val expectedSetting = allSettings(graph).head
    val expectedProcedure = allProceduresNames.head
    val (unwindQuery, latch) = setupUserWithOneTransaction(Map("setting" -> expectedSetting("name")))

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      val result = execute(
        s"""SHOW TRANSACTION '$unwindTransactionId'
           |YIELD transactionId AS txId, parameters, database
           |SHOW PROCEDURES
           |YIELD name AS procedure
           |WHERE procedure = '$expectedProcedure'
           |SHOW SETTING parameters.setting
           |YIELD name AS setting, value
           |TERMINATE TRANSACTION txId
           |YIELD message
           |SHOW USER DEFINED FUNCTIONS EXECUTABLE
           |YIELD name AS function
           |WHERE function CONTAINS 'return'
           |SHOW DATABASES
           |YIELD name AS db, default
           |WHERE db = database
           |RETURN txId, procedure, setting, value, message, function, default""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "txId" -> unwindTransactionId,
        "procedure" -> expectedProcedure,
        "setting" -> expectedSetting("name"),
        "value" -> expectedSetting("value"),
        "message" -> "Transaction terminated.",
        "function" -> "test.return.latest",
        "default" -> true
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should fail to combine commands other than show and terminate transaction in Cypher 5") {
    // THEN
    List(
      (s"SHOW TRANSACTIONS YIELD database AS db1 WHERE db1 <> '$SYSTEM_DATABASE_NAME'", true),
      ("TERMINATE TRANSACTION 'foo-transaction-123' YIELD message AS message1", true),
      ("SHOW PROCEDURES YIELD name AS procedure1", false),
      ("SHOW FUNCTIONS YIELD name AS function1", false),
      ("SHOW SETTINGS YIELD name AS setting1", false),
      ("SHOW INDEXES YIELD name AS index1", false),
      ("SHOW CONSTRAINTS YIELD name AS constraint1", false),
      ("SHOW DATABASES YIELD name AS database1", false)
    ).foreach { case (firstCommand, firstAllowedComposed) =>
      List(
        (s"SHOW TRANSACTIONS YIELD database AS db2 WHERE db2 <> '$SYSTEM_DATABASE_NAME'", true),
        ("TERMINATE TRANSACTION 'foo-transaction-123' YIELD message AS message2", true),
        ("SHOW PROCEDURES YIELD name AS procedure2", false),
        ("SHOW FUNCTIONS YIELD name AS function2", false),
        ("SHOW SETTINGS YIELD name AS setting2", false),
        ("SHOW INDEXES YIELD name AS index2", false),
        ("SHOW CONSTRAINTS YIELD name AS constraint2", false),
        ("SHOW DATABASES YIELD name AS database2", false)
      ).foreach { case (secondCommand, secondAllowedComposed) =>
        val query = s"CYPHER 5 $firstCommand $secondCommand RETURN *"
        withClue(query) {
          if (firstAllowedComposed && secondAllowedComposed) {
            // Only show and terminate transactions are allowed regardless of Cypher version
            // Should see the current transaction in show and `Transaction not found.` for terminate
            execute(query).toList should not be empty
          } else {
            // Commands including anything else should fail
            // WHEN
            val exception = the[SyntaxException] thrownBy {
              execute(query)
            }

            // THEN
            exception should be(gqlException(
              "Invalid input '",
              InvalidSyntaxStatus.withCause(gqlStatus(
                GqlStatusInfoCodes.STATUS_42I06,
                "error: syntax error or access rule violation - invalid input. Invalid input '",
                fuzzyStatusDescr = true
              ).withCause(Reparsesable_42I67)),
              fuzzyMsg = true
            ))
          }
        }
      }
    }
  }

  test("Should fail to combine commands with show current graph type in community") {
    Seq(
      "SHOW TRANSACTIONS YIELD transactionId",
      "SHOW PROCEDURES YIELD name",
      "SHOW INDEXES YIELD name",
      "SHOW SETTINGS YIELD name",
      "TERMINATE TRANSACTION 'txId' YIELD message",
      "SHOW FUNCTIONS YIELD name",
      "SHOW CONSTRAINTS YIELD name",
      "SHOW HOME DATABASE YIELD name"
    ).foreach(command =>
      withClue(command + ": ") {
        // WHEN
        val exceptionAfter = the[RuntimeUnsupportedException] thrownBy {
          execute(s"CYPHER 25 SHOW CURRENT GRAPH TYPE YIELD specification $command RETURN *")
        }

        // THEN
        exceptionAfter should be(gqlException(
          "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
          )
        ))
        val causeAfter = exceptionAfter.getCause
        causeAfter should not be null
        causeAfter shouldBe a[CantCompileQueryException]
        causeAfter.asInstanceOf[CantCompileQueryException] should be(gqlException(
          "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
          )
        ))

        // WHEN
        val exceptionBefore = the[RuntimeUnsupportedException] thrownBy {
          execute(s"CYPHER 25 $command SHOW CURRENT GRAPH TYPE YIELD specification RETURN *")
        }

        // THEN
        exceptionBefore should be(gqlException(
          "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
          )
        ))
        val causeBefore = exceptionBefore.getCause
        causeBefore should not be null
        causeBefore shouldBe a[CantCompileQueryException]
        causeBefore.asInstanceOf[CantCompileQueryException] should be(gqlException(
          "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
          )
        ))
      }
    )
  }

}
