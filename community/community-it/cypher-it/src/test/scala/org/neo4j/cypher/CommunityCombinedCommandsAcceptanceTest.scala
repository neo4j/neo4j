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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.CommunityShowFuncProcAcceptanceTest.readAll
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.test.DoubleLatch

import java.lang.Boolean.TRUE
import java.nio.file.NoSuchFileException

class CommunityCombinedCommandsAcceptanceTest extends TransactionCommandAcceptanceTestSupport
    with ShowSettingsAcceptanceTestSupport {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseInternalSettings.composable_commands -> TRUE
  )

  override protected def onNewGraphDatabase(): Unit = {
    super.onNewGraphDatabase()
    val globalProcedures: GlobalProcedures = graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    globalProcedures.registerFunction(classOf[TestShowFunction])
    globalProcedures.registerAggregationFunction(classOf[TestShowFunction])
  }

  private val funcResourceUrl = getClass.getResource("/builtInFunctions.json")
  if (funcResourceUrl == null) throw new NoSuchFileException(s"File not found: builtInFunctions.json")

  private val builtInFunctionsNames =
    readAll(funcResourceUrl)
      .filterNot(m => m.getOrElse("enterpriseOnly", false).asInstanceOf[Boolean])
      .map(m => m("name").asInstanceOf[String])

  private val userDefinedFunctionsNames = List("test.function", "test.functionWithInput", "test.return.latest")

  private val allFunctionsNames = (builtInFunctionsNames ++ userDefinedFunctionsNames).sorted

  private val procResourceUrl = getClass.getResource("/procedures.json")
  if (procResourceUrl == null) throw new NoSuchFileException(s"File not found: procedures.json")

  private val allProceduresNames =
    readAll(procResourceUrl)
      .filterNot(m => m("enterpriseOnly").asInstanceOf[Boolean])
      .map(m => m("name").asInstanceOf[String])

  // Tests

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

  test("Should combine all show and terminate commands") {
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
        "constraintType" -> "UNIQUENESS"
      )))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

}
