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
import org.neo4j.cypher.internal.ast.CommaSeparatedNames
import org.neo4j.cypher.internal.ast.CommandClauseNames
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.util.Random

/* Tests for combining listing and terminating commands */
class CombineMultipleCommandsParserTest extends CombineCommandsParserTestBase
    with AstConstructionTestSupportWithPosConversion {

  private type CommandClauseWithNames =
    (
      CommandClauseNames,
      Option[(ast.Where, InputPosition)],
      Boolean,
      List[ast.CommandResultItem],
      Option[ast.With]
    ) => InputPosition => ast.CommandClause

  private type CommandClauseNoNames =
    (
      Option[(ast.Where, InputPosition)],
      Boolean,
      List[ast.CommandResultItem],
      Option[ast.With]
    ) => InputPosition => ast.CommandClause

  private case class CommandCombinationsWithNames(
    firstCommand: String,
    firstClause: CommandClauseWithNames,
    secondCommand: String,
    secondClause: CommandClauseWithNames,
    supportedInCypher5: Boolean = false
  )

  private case class CommandCombinationsNoNames(
    firstCommand: String,
    firstClause: CommandClauseNoNames,
    secondCommand: String,
    secondClause: CommandClauseNoNames,
    supportedInCypher5: Boolean = false
  )

  private def getWherePosition(startIndex: Int = 0) = {
    val startOfWhereClause = testName.indexOf("WHERE", startIndex)
    InputPosition(startOfWhereClause, 1, startOfWhereClause + 1)
  }

  private val commandCombinationsAllowingStringExpressions = Seq(
    CommandCombinationsWithNames(
      "SHOW TRANSACTION",
      showTx: CommandClauseWithNames,
      "SHOW TRANSACTION",
      showTx: CommandClauseWithNames,
      supportedInCypher5 = true
    ),
    CommandCombinationsWithNames(
      "SHOW TRANSACTION",
      showTx: CommandClauseWithNames,
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames,
      supportedInCypher5 = true
    ),
    CommandCombinationsWithNames(
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames,
      "SHOW TRANSACTION",
      showTx: CommandClauseWithNames,
      supportedInCypher5 = true
    ),
    CommandCombinationsWithNames(
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames,
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames,
      supportedInCypher5 = true
    ),
    CommandCombinationsWithNames(
      "SHOW SETTING",
      showSetting: CommandClauseWithNames,
      "SHOW SETTING",
      showSetting: CommandClauseWithNames
    ),
    CommandCombinationsWithNames(
      "SHOW TRANSACTION",
      showTx: CommandClauseWithNames,
      "SHOW SETTING",
      showSetting: CommandClauseWithNames
    ),
    CommandCombinationsWithNames(
      "SHOW SETTING",
      showSetting: CommandClauseWithNames,
      "SHOW TRANSACTION",
      showTx: CommandClauseWithNames
    ),
    CommandCombinationsWithNames(
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames,
      "SHOW SETTING",
      showSetting: CommandClauseWithNames
    ),
    CommandCombinationsWithNames(
      "SHOW SETTING",
      showSetting: CommandClauseWithNames,
      "TERMINATE TRANSACTION",
      terminateTx: CommandClauseWithNames
    )
  )

  private val commandCombinationsWithoutExpressions: Seq[CommandCombinationsNoNames] =
    Seq(
      // show functions only combinations
      CommandCombinationsNoNames(
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _),
        "SHOW FUNCTIONS EXECUTABLE",
        showFunction(ast.AllFunctions, Some(ast.CurrentUser), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW ALL FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _),
        "SHOW ALL FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _, _),
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW USER DEFINED FUNCTIONS",
        showFunction(ast.UserDefinedFunctions, None, _, _, _, _),
        "SHOW USER DEFINED FUNCTIONS EXECUTABLE BY user",
        showFunction(ast.UserDefinedFunctions, Some(ast.User("user")(pos)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW FUNCTIONS EXECUTABLE BY user",
        showFunction(ast.AllFunctions, Some(ast.User("user")(pos)), _, _, _, _),
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _, _),
        "SHOW USER DEFINED FUNCTIONS",
        showFunction(ast.UserDefinedFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW USER DEFINED FUNCTIONS EXECUTABLE",
        showFunction(ast.UserDefinedFunctions, Some(ast.CurrentUser), _, _, _, _),
        "SHOW ALL FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      )
    ) ++ Seq(
      // show procedures only combinations
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW PROCEDURES EXECUTABLE",
        showProcedure(Some(ast.CurrentUser), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES EXECUTABLE BY CURRENT USER",
        showProcedure(Some(ast.CurrentUser), _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW PROCEDURES EXECUTABLE BY user",
        showProcedure(Some(ast.User("user")(pos)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES EXECUTABLE",
        showProcedure(Some(ast.CurrentUser), _, _, _, _),
        "SHOW PROCEDURES EXECUTABLE BY SHOW",
        showProcedure(Some(ast.User("SHOW")(pos)), _, _, _, _)
      )
    ) ++ Seq(
      // show constraints only combinations
      CommandCombinationsNoNames(
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW NODE KEY CONSTRAINTS",
        showConstraint(ast.NodeKeyConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW RELATIONSHIP KEY CONSTRAINTS",
        showConstraint(ast.RelKeyConstraints, _, _, _, _),
        "SHOW KEY CONSTRAINTS",
        showConstraint(ast.KeyConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW NODE UNIQUENESS CONSTRAINTS",
        showConstraint(ast.NodeUniqueConstraints.cypher25, _, _, _, _),
        "SHOW UNIQUE CONSTRAINTS",
        showConstraint(ast.UniqueConstraints.cypher25, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW REL UNIQUE CONSTRAINTS",
        showConstraint(ast.RelUniqueConstraints.cypher25, _, _, _, _),
        "SHOW PROPERTY EXISTENCE CONSTRAINTS",
        showConstraint(ast.PropExistsConstraints.cypher25, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW NODE PROPERTY EXIST CONSTRAINTS",
        showConstraint(ast.NodePropExistsConstraints.cypher25, _, _, _, _),
        "SHOW REL EXIST CONSTRAINTS",
        showConstraint(ast.RelAllExistsConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.PropTypeConstraints, _, _, _, _),
        "SHOW NODE PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.NodePropTypeConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW RELATIONSHIP PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.RelPropTypeConstraints, _, _, _, _)
      )
    ) ++ Seq(
      // show indexes only combinations
      CommandCombinationsNoNames(
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW ALL INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _),
        "SHOW RANGE INDEXES",
        showIndex(ast.RangeIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW FULLTEXT INDEXES",
        showIndex(ast.FulltextIndexes, _, _, _, _),
        "SHOW TEXT INDEXES",
        showIndex(ast.TextIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW POINT INDEXES",
        showIndex(ast.PointIndexes, _, _, _, _),
        "SHOW LOOKUP INDEXES",
        showIndex(ast.LookupIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _),
        "SHOW VECTOR INDEXES",
        showIndex(ast.VectorIndexes, _, _, _, _)
      )
    ) ++ Seq(
      // show current graph type only combinations
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _)
      )
    ) ++ Seq(
      // show databases only combinations
      CommandCombinationsNoNames(
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _),
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW HOME DATABASE",
        showDatabase(ast.HomeDatabaseScope()(pos), _, _, _, _),
        "SHOW DEFAULT DATABASE",
        showDatabase(ast.DefaultDatabaseScope()(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _),
        "SHOW DATABASE foo",
        showDatabase(ast.SingleNamedDatabaseScope(ast.NamespacedName("foo")(pos))(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW DATABASES bar.baz",
        showDatabase(ast.SingleNamedDatabaseScope(ast.NamespacedName("bar.baz")(pos))(pos), _, _, _, _),
        "SHOW DATABASE foo",
        showDatabase(ast.SingleNamedDatabaseScope(ast.NamespacedName("foo")(pos))(pos), _, _, _, _)
      )
    ) ++ Seq(
      // mixed show and terminate commands
      // excluding mixes of only those accepting string expressions,
      // as that is handled by `commandCombinationsAllowingStringExpressions`

      // show transaction combined with remaining commands
      CommandCombinationsNoNames(
        "SHOW TRANSACTIONS",
        showTx(NoNames, _, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW SETTINGS",
        showSetting(NoNames, _, _, _, _),
        "SHOW TRANSACTIONS",
        showTx(NoNames, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW ALL FUNCTIONS EXECUTABLE BY SHOW",
        showFunction(ast.AllFunctions, Some(ast.User("SHOW")(pos)), _, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW TRANSACTIONS",
        showTx(NoNames, _, _, _, _),
        "SHOW PROCEDURES EXECUTABLE BY SHOW",
        showProcedure(Some(ast.User("SHOW")(pos)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW TRANSACTIONS",
        showTx(NoNames, _, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROPERTY TYPE CONSTRAINTS",
        showConstraint(ast.PropTypeConstraints, _, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW TRANSACTIONS",
        showTx(NoNames, _, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW POINT INDEXES",
        showIndex(ast.PointIndexes, _, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW TRANSACTIONS",
        showTx(NoNames, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW TRANSACTIONS",
        showTx(NoNames, _, _, _, _),
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW HOME DATABASE",
        showDatabase(ast.HomeDatabaseScope()(pos), _, _, _, _),
        "SHOW TRANSACTIONS 'db1-transaction-123'",
        showTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      // terminate transaction combined with remaining commands
      CommandCombinationsNoNames(
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _),
        "SHOW BUILT IN FUNCTIONS",
        showFunction(ast.BuiltInFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW FUNCTIONS EXECUTABLE BY TERMINATE",
        showFunction(ast.AllFunctions, Some(ast.User("TERMINATE")(pos)), _, _, _, _),
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES EXECUTABLE BY TERMINATE",
        showProcedure(Some(ast.User("TERMINATE")(pos)), _, _, _, _),
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _),
        "SHOW NODE EXISTENCE CONSTRAINTS",
        showConstraint(ast.NodeAllExistsConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _),
        "SHOW RANGE INDEXES",
        showIndex(ast.RangeIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _),
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _),
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _),
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _),
        "SHOW DEFAULT DATABASE",
        showDatabase(ast.DefaultDatabaseScope()(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW DATABASE foo",
        showDatabase(ast.SingleNamedDatabaseScope(ast.NamespacedName("foo")(pos))(pos), _, _, _, _),
        "TERMINATE TRANSACTIONS 'db1-transaction-123'",
        terminateTx(ExpressionNames(literalString("db1-transaction-123")), _, _, _, _)
      ),
      // show settings combined with remaining commands
      CommandCombinationsNoNames(
        "SHOW SETTINGS",
        showSetting(NoNames, _, _, _, _),
        "SHOW USER DEFINED FUNCTIONS EXECUTABLE",
        showFunction(ast.UserDefinedFunctions, Some(ast.CurrentUser), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(ExpressionNames(parameter("setting", CTAny)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW SETTINGS",
        showSetting(NoNames, _, _, _, _),
        "SHOW PROCEDURES EXECUTABLE",
        showProcedure(Some(ast.CurrentUser), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(ExpressionNames(parameter("setting", CTAny)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW SETTINGS",
        showSetting(NoNames, _, _, _, _),
        "SHOW UNIQUENESS CONSTRAINTS",
        showConstraint(ast.UniqueConstraints.cypher25, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(ExpressionNames(parameter("setting", CTAny)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW SETTINGS",
        showSetting(NoNames, _, _, _, _),
        "SHOW TEXT INDEXES",
        showIndex(ast.TextIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(ExpressionNames(parameter("setting", CTAny)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW SETTINGS",
        showSetting(NoNames, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(ExpressionNames(parameter("setting", CTAny)), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW SETTINGS",
        showSetting(NoNames, _, _, _, _),
        "SHOW DATABASE foo",
        showDatabase(ast.SingleNamedDatabaseScope(ast.NamespacedName("foo")(pos))(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _),
        "SHOW SETTINGS $setting",
        showSetting(ExpressionNames(parameter("setting", CTAny)), _, _, _, _)
      ),
      // show functions combined with remaining commands
      CommandCombinationsNoNames(
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _, _),
        "SHOW KEY CONSTRAINTS",
        showConstraint(ast.KeyConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW LOOKUP INDEXES",
        showIndex(ast.LookupIndexes, _, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _, _),
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
        showFunction(ast.BuiltInFunctions, Some(ast.CurrentUser), _, _, _, _),
        "SHOW HOME DATABASE",
        showDatabase(ast.HomeDatabaseScope()(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW DEFAULT DATABASE",
        showDatabase(ast.DefaultDatabaseScope()(pos), _, _, _, _),
        "SHOW FUNCTIONS",
        showFunction(ast.AllFunctions, None, _, _, _, _)
      ),
      // show procedures combined with remaining commands
      CommandCombinationsNoNames(
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW ALL INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW DEFAULT DATABASE",
        showDatabase(ast.DefaultDatabaseScope()(pos), _, _, _, _),
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW PROCEDURES",
        showProcedure(None, _, _, _, _),
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _)
      ),
      // show constraints combined with remaining commands
      CommandCombinationsNoNames(
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW FULLTEXT INDEXES",
        showIndex(ast.FulltextIndexes, _, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW ALL CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _),
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW HOME DATABASE",
        showDatabase(ast.HomeDatabaseScope()(pos), _, _, _, _),
        "SHOW CONSTRAINTS",
        showConstraint(ast.AllConstraints, _, _, _, _)
      ),
      // show indexes combined with remaining commands
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW FULLTEXT INDEXES",
        showIndex(ast.FulltextIndexes, _, _, _, _),
        "SHOW CURRENT GRAPH TYPE AS GRAPH",
        showCurrentGraphType(true, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _),
        "SHOW INDEXES",
        showIndex(ast.AllIndexes, _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW FULLTEXT INDEXES",
        showIndex(ast.FulltextIndexes, _, _, _, _),
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _)
      ),
      // show current graph type combined with remaining commands
      CommandCombinationsNoNames(
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _),
        "SHOW DATABASES",
        showDatabase(ast.AllDatabasesScope()(pos), _, _, _, _)
      ),
      CommandCombinationsNoNames(
        "SHOW HOME DATABASE",
        showDatabase(ast.HomeDatabaseScope()(pos), _, _, _, _),
        "SHOW CURRENT GRAPH TYPE",
        showCurrentGraphType(false, _, _, _, _)
      )
    )

  private val commandCombinationsAll: Seq[CommandCombinationsNoNames] =
    commandCombinationsAllowingStringExpressions.map {
      case CommandCombinationsWithNames(firstCommand, firstClause, secondCommand, secondClause, supportedInCypher5) =>
        CommandCombinationsNoNames(
          s"$firstCommand 'txId1'",
          firstClause(ExpressionNames(literalString("txId1")), _, _, _, _),
          s"$secondCommand 'txId2'",
          secondClause(ExpressionNames(literalString("txId2")), _, _, _, _),
          supportedInCypher5
        )
    } ++ commandCombinationsWithoutExpressions

  private def updateForCypher5(clause: ast.Clause): ast.Clause = clause match {
    case scc: ast.ShowConstraintsClause =>
      // the constraint type and the columns generated by the apply is what differs in versions
      // so for the columns we need to call the apply instead of copy the existing one
      // if we don't want to try and update the columns manually
      val updatedConstraintType = scc.constraintType match {
        case _: ast.UniqueConstraints         => ast.UniqueConstraints.cypher5
        case _: ast.NodeUniqueConstraints     => ast.NodeUniqueConstraints.cypher5
        case _: ast.RelUniqueConstraints      => ast.RelUniqueConstraints.cypher5
        case _: ast.PropExistsConstraints     => ast.PropExistsConstraints.cypher5
        case _: ast.NodePropExistsConstraints => ast.NodePropExistsConstraints.cypher5
        case _: ast.RelPropExistsConstraints  => ast.RelPropExistsConstraints.cypher5
        case other                            => other
      }
      ast.ShowConstraintsClause(
        updatedConstraintType,
        scc.where,
        scc.yieldItems,
        scc.yieldAll,
        scc.yieldWith,
        returnCypher5Columns = true
      )(scc.position)
    case stc: ast.ShowTransactionsClause =>
      // the columns generated by the apply is what differs in versions
      // so we need to call the apply instead of copy the existing one
      // if we don't want to try and update the columns manually
      ast.ShowTransactionsClause(
        stc.names,
        stc.where,
        stc.yieldItems,
        stc.yieldAll,
        stc.yieldWith,
        returnCypher5Types = true
      )(stc.position)
    case other => other
  }

  private def assertAst(expectedClauses: ast.Clause*): Unit =
    assertAstVersionAware(supportedInCypher5 = true, expectedClauses: _*)

  // Can't be named `assertAst` or `assertAstVersionBased` as that leads to compile errors on `Cannot resolve overloaded method`
  private def assertAstVersionAware(supportedInCypher5: Boolean, expectedClauses: ast.Clause*): Unit =
    assertAstVersionAware(supportedInCypher5, true, expectedClauses*)

  private def assertAstVersionAware(
    supportedInCypher5: Boolean,
    obfuscator: Boolean,
    expectedClauses: ast.Clause*
  ): Unit = {
    parsesIn[ast.Statements] {
      case Cypher5 if !supportedInCypher5 =>
        _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ",
          fuzzyStatusDescr = true
        )
      case Cypher5 =>
        _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses.map(updateForCypher5): _*))), obfuscator)
      case _ => _.toAstPositioned(ast.Statements(Seq(singleQuery(expectedClauses: _*))), obfuscator)
    }
  }

  private def assertAstDontComparePosVersionBased(
    supportedInCypher5: Boolean,
    expectedClauses: Boolean => Seq[ast.Clause]
  ): Unit = {
    parsesIn[ast.Statements] {
      case Cypher5 if !supportedInCypher5 =>
        _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ",
          fuzzyStatusDescr = true
        )
      case Cypher5 => _.toAst(ast.Statements(Seq(singleQuery(expectedClauses(true).map(updateForCypher5): _*))))
      case _       => _.toAst(ast.Statements(Seq(singleQuery(expectedClauses(false): _*))))
    }
  }

  Random.shuffle(commandCombinationsAll).take(35).foreach {
    case CommandCombinationsNoNames(firstCommand, firstClause, secondCommand, secondClause, supportedInCypher5) =>
      test(s"$firstCommand $secondCommand") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(None, false, List.empty, None)(defaultPos),
          secondClause(None, false, List.empty, None)(pos)
        )
      }

      test(s"USE db $firstCommand $secondCommand") {
        assertAstDontComparePosVersionBased(
          supportedInCypher5,
          cypherVersion5 =>
            Seq(
              use(List("db"), !cypherVersion5),
              firstClause(None, false, List.empty, None)(pos),
              secondClause(None, false, List.empty, None)(pos)
            )
        )
      }

      test(s"$firstCommand WHERE transactionId = '123' $secondCommand") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty,
            None
          )(defaultPos),
          secondClause(None, false, List.empty, None)(pos)
        )
      }

      test(s"$firstCommand $secondCommand WHERE transactionId = '123'") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(None, false, List.empty, None)(defaultPos),
          secondClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty,
            None
          )(pos)
        )
      }

      test(s"$firstCommand WHERE transactionId = '123' $secondCommand WHERE transactionId = '123'") {
        val where1Pos = getWherePosition()
        val where2Pos = getWherePosition(where1Pos.offset + 1)
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), where1Pos)),
            false,
            List.empty,
            None
          )(defaultPos),
          secondClause(
            Some((where(equals(varFor("transactionId"), literalString("123"))), where2Pos)),
            false,
            List.empty,
            None
          )(pos)
        )
      }

      test(s"$firstCommand YIELD transactionId AS txId $secondCommand") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(None, false, List.empty, None)(pos)
        )
      }

      test(s"$firstCommand $secondCommand YIELD transactionId AS txId") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(None, false, List.empty, None)(defaultPos),
          secondClause(
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(pos)
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand YIELD username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(
            None,
            false,
            List(commandResultItem("username")),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))))
          )(pos)
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand YIELD username RETURN txId, username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(
            None,
            false,
            List(commandResultItem("username")),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))))
          )(pos),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD * $secondCommand YIELD username RETURN txId, username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(defaultPos),
          secondClause(
            None,
            false,
            List(commandResultItem("username")),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))))
          )(pos),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId $secondCommand YIELD * RETURN txId, username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(pos),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD * $secondCommand YIELD * RETURN txId, username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(defaultPos),
          secondClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(pos),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand YIELD transactionId AS txId RETURN txId $secondCommand YIELD username RETURN txId, username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          returnClause(returnItems(variableReturnItem("txId"))),
          secondClause(
            None,
            false,
            List(commandResultItem("username")),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))))
          )(pos),
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
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            None,
            false,
            List(
              commandResultItem("transactionId", Some("txId")),
              commandResultItem("currentQuery"),
              commandResultItem("username", Some("user"))
            ),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "currentQuery", "user"))))
          )(defaultPos),
          secondClause(
            None,
            false,
            List(
              commandResultItem("username"),
              commandResultItem("message")
            ),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username", "message"))))
          )(pos),
          returnAll
        )
      }

      // more commands per query

      Random.shuffle(commandCombinationsAll).take(35).foreach {
        case CommandCombinationsNoNames(
            thirdCommand,
            thirdClause,
            fourthCommand,
            fourthClause,
            secondSupportedInCypher5
          ) =>
          test(
            s"$firstCommand $secondCommand $thirdCommand $fourthCommand"
          ) {
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(None, false, List.empty, None)(defaultPos),
              secondClause(None, false, List.empty, None)(pos),
              thirdClause(None, false, List.empty, None)(pos),
              fourthClause(None, false, List.empty, None)(pos)
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
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(defaultPos),
              secondClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(pos),
              thirdClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(pos),
              fourthClause(None, true, List.empty, Some(withFromYield(returnAllItems)))(pos)
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
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(
                None,
                false,
                List(commandResultItem("transactionId", Some("txId"))),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
              )(defaultPos),
              secondClause(
                None,
                false,
                List(
                  commandResultItem("transactionId", Some("txId")),
                  commandResultItem("username")
                ),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "username"))))
              )(pos),
              thirdClause(
                None,
                false,
                List(commandResultItem("transactionId", Some("txId"))),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
              )(pos),
              fourthClause(
                None,
                false,
                List(
                  commandResultItem("transactionId", Some("txId")),
                  commandResultItem("message", Some("status"))
                ),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "status"))))
              )(pos),
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
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where1Pos)),
                false,
                List.empty,
                None
              )(defaultPos),
              secondClause(
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where2Pos)),
                false,
                List.empty,
                None
              )(pos),
              thirdClause(
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where3Pos)),
                false,
                List.empty,
                None
              )(pos),
              fourthClause(
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where4Pos)),
                false,
                List.empty,
                None
              )(pos)
            )
          }
      }
  }

  commandCombinationsAllowingStringExpressions.foreach {
    case CommandCombinationsWithNames(firstCommand, firstClause, secondCommand, secondClause, supportedInCypher5) =>
      test(s"$firstCommand 'db1-transaction-123' $secondCommand 'db1-transaction-123'") {
        assertAstVersionAware(
          supportedInCypher5,
          false,
          firstClause(ExpressionNames(literalString("db1-transaction-123")), None, false, List.empty, None)(defaultPos),
          secondClause(ExpressionNames(literalString("db1-transaction-123")), None, false, List.empty, None)(pos)
        )
      }

      test(
        s"$firstCommand 'db1-transaction-123', 'db1-transaction-123' $secondCommand 'db1-transaction-123', 'db1-transaction-123'"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          false,
          firstClause(
            CommaSeparatedNames(listOfString("db1-transaction-123", "db1-transaction-123")),
            None,
            false,
            List.empty,
            None
          )(defaultPos),
          secondClause(
            CommaSeparatedNames(listOfString("db1-transaction-123", "db1-transaction-123")),
            None,
            false,
            List.empty,
            None
          )(pos)
        )
      }

      test(s"$firstCommand $$txId $secondCommand $$txId") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(ExpressionNames(parameter("txId", CTAny)), None, false, List.empty, None)(defaultPos),
          secondClause(ExpressionNames(parameter("txId", CTAny)), None, false, List.empty, None)(pos)
        )
      }

      test(s"$firstCommand 'id' WHERE transactionId = '123' $secondCommand 'db1-transaction-123'") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty,
            None
          )(defaultPos),
          secondClause(ExpressionNames(literalString("db1-transaction-123")), None, false, List.empty, None)(pos)
        )
      }

      test(s"$firstCommand 'id' $secondCommand 'db1-transaction-123' WHERE transactionId = '123'") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(ExpressionNames(literalString("id")), None, false, List.empty, None)(defaultPos),
          secondClause(
            ExpressionNames(literalString("db1-transaction-123")),
            Some((where(equals(varFor("transactionId"), literalString("123"))), getWherePosition())),
            false,
            List.empty,
            None
          )(pos)
        )
      }

      test(
        s"$firstCommand 'id' WHERE transactionId = '123' $secondCommand 'db1-transaction-123' WHERE transactionId = '123'"
      ) {
        val where1Pos = getWherePosition()
        val where2Pos = getWherePosition(where1Pos.offset + 1)
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            Some((where(equals(varFor("transactionId"), literalString("123"))), where1Pos)),
            false,
            List.empty,
            None
          )(defaultPos),
          secondClause(
            ExpressionNames(literalString("db1-transaction-123")),
            Some((where(equals(varFor("transactionId"), literalString("123"))), where2Pos)),
            false,
            List.empty,
            None
          )(pos)
        )
      }

      test(s"$firstCommand 'id' YIELD transactionId AS txId $secondCommand 'db1-transaction-123'") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(ExpressionNames(literalString("db1-transaction-123")), None, false, List.empty, None)(pos)
        )
      }

      test(s"$firstCommand 'id' $secondCommand 'db1-transaction-123' YIELD transactionId AS txId") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(ExpressionNames(literalString("id")), None, false, List.empty, None)(defaultPos),
          secondClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(pos)
        )
      }

      test(
        s"$firstCommand 'id' YIELD transactionId AS txId $secondCommand 'db1-transaction-123' YIELD username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("username")),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))))
          )(pos)
        )
      }

      test(
        s"$firstCommand 'id' YIELD transactionId AS txId RETURN txId $secondCommand 'db1-transaction-123' YIELD username RETURN txId, username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          returnClause(returnItems(variableReturnItem("txId"))),
          secondClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("username")),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))))
          )(pos),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"$firstCommand 'id' YIELD transactionId AS txId $secondCommand 'db1-transaction-123' YIELD username RETURN txId, username"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            false,
            List(commandResultItem("username")),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username"))))
          )(pos),
          returnClause(returnItems(variableReturnItem("txId"), variableReturnItem("username")))
        )
      }

      test(
        s"""$firstCommand 'db1-transaction-123'
           |YIELD transactionId AS txId, currentQuery, username AS user
           |$secondCommand 'db1-transaction-123'
           |YIELD username, message
           |RETURN *""".stripMargin
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            false,
            List(
              commandResultItem("transactionId", Some("txId")),
              commandResultItem("currentQuery"),
              commandResultItem("username", Some("user"))
            ),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "currentQuery", "user"))))
          )(defaultPos),
          secondClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            false,
            List(
              commandResultItem("username"),
              commandResultItem("message")
            ),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username", "message"))))
          )(pos),
          returnAll
        )
      }

      // more commands per query

      commandCombinationsAllowingStringExpressions.foreach {
        case CommandCombinationsWithNames(
            thirdCommand,
            thirdClause,
            fourthCommand,
            fourthClause,
            secondSupportedInCypher5
          ) =>
          test(
            s"""$firstCommand 'db1-transaction-123'
               |${secondCommand}S 'db1-transaction-123'
               |$thirdCommand 'db1-transaction-123'
               |${fourthCommand}S 'db1-transaction-123'""".stripMargin
          ) {
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                false,
                List.empty,
                None
              )(defaultPos),
              secondClause(ExpressionNames(literalString("db1-transaction-123")), None, false, List.empty, None)(pos),
              thirdClause(ExpressionNames(literalString("db1-transaction-123")), None, false, List.empty, None)(pos),
              fourthClause(ExpressionNames(literalString("db1-transaction-123")), None, false, List.empty, None)(pos)
            )
          }

          test(
            s"${firstCommand}S $$txId $secondCommand $$txId ${thirdCommand}S $$txId $fourthCommand $$txId"
          ) {
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(ExpressionNames(parameter("txId", CTAny)), None, false, List.empty, None)(defaultPos),
              secondClause(ExpressionNames(parameter("txId", CTAny)), None, false, List.empty, None)(pos),
              thirdClause(ExpressionNames(parameter("txId", CTAny)), None, false, List.empty, None)(pos),
              fourthClause(ExpressionNames(parameter("txId", CTAny)), None, false, List.empty, None)(pos)
            )
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
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                true,
                List.empty,
                Some(withFromYield(returnAllItems))
              )(defaultPos),
              secondClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                true,
                List.empty,
                Some(withFromYield(returnAllItems))
              )(pos),
              thirdClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                true,
                List.empty,
                Some(withFromYield(returnAllItems))
              )(pos),
              fourthClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                true,
                List.empty,
                Some(withFromYield(returnAllItems))
              )(pos)
            )
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
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                false,
                List(commandResultItem("transactionId", Some("txId"))),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
              )(defaultPos),
              secondClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                false,
                List(
                  commandResultItem("transactionId", Some("txId")),
                  commandResultItem("username")
                ),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "username"))))
              )(pos),
              thirdClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                false,
                List(commandResultItem("transactionId", Some("txId"))),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
              )(pos),
              fourthClause(
                ExpressionNames(literalString("db1-transaction-123")),
                None,
                false,
                List(
                  commandResultItem("transactionId", Some("txId")),
                  commandResultItem("message", Some("status"))
                ),
                Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId", "status"))))
              )(pos),
              returnAll
            )
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
            assertAstVersionAware(
              supportedInCypher5 && secondSupportedInCypher5,
              firstClause(
                ExpressionNames(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where1Pos)),
                false,
                List.empty,
                None
              )(defaultPos),
              secondClause(
                ExpressionNames(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where2Pos)),
                false,
                List.empty,
                None
              )(pos),
              thirdClause(
                ExpressionNames(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where3Pos)),
                false,
                List.empty,
                None
              )(pos),
              fourthClause(
                ExpressionNames(literalString("db1-transaction-123")),
                Some((where(equals(varFor("message"), literalString("Transaction terminated."))), where4Pos)),
                false,
                List.empty,
                None
              )(pos)
            )
          }
      }

      // general expression and not just string/param

      test(s"${firstCommand}S 'id' YIELD transactionId AS txId $secondCommand txId") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(ExpressionNames(varFor("txId")), None, false, List.empty, None)(pos)
        )
      }

      test(s"${firstCommand}S foo YIELD transactionId AS show $secondCommand show") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(varFor("foo")),
            None,
            false,
            List(commandResultItem("transactionId", Some("show"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("show"))))
          )(defaultPos),
          secondClause(ExpressionNames(varFor("show")), None, false, List.empty, None)(pos)
        )
      }

      test(
        s"${firstCommand}S ['db1-transaction-123', 'db2-transaction-456'] YIELD transactionId AS show $secondCommand show"
      ) {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(listOfString("db1-transaction-123", "db2-transaction-456")),
            None,
            false,
            List(commandResultItem("transactionId", Some("show"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("show"))))
          )(defaultPos),
          secondClause(ExpressionNames(varFor("show")), None, false, List.empty, None)(pos)
        )
      }

      test(s"${firstCommand}S 'id' YIELD transactionId AS txId $secondCommand txId + '123'") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(literalString("id")),
            None,
            false,
            List(commandResultItem("transactionId", Some("txId"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("txId"))))
          )(defaultPos),
          secondClause(ExpressionNames(add(varFor("txId"), literalString("123"))), None, false, List.empty, None)(pos)
        )
      }

      test(s"${firstCommand}S yield YIELD transactionId AS show $secondCommand show") {
        assertAstVersionAware(
          supportedInCypher5,
          firstClause(
            ExpressionNames(varFor("yield")),
            None,
            false,
            List(commandResultItem("transactionId", Some("show"))),
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("show"))))
          )(defaultPos),
          secondClause(ExpressionNames(varFor("show")), None, false, List.empty, None)(pos)
        )
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
    def expected(variablesAreEscaped: Boolean, returnCypher5Types: Boolean, resolveStrictly: Boolean) =
      singleQuery(
        use(List("test"), resolveStrictly),
        ast.ShowTransactionsClause(
          ExpressionNames(literalString("")),
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          returnCypher5Types
        )(pos),
        ast.ShowTransactionsClause(
          CommaSeparatedNames(listOfString("", "", "")),
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          returnCypher5Types
        )(pos),
        ast.ShowTransactionsClause(
          ExpressionNames(varFor("콺", variablesAreEscaped)),
          None,
          List(
            commandResultItem("碌", variablesAreEscaped),
            commandResultItem("脃", variablesAreEscaped),
            commandResultItem("麪", variablesAreEscaped)
          ),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("碌", "脃", "麪")),
            Some(orderBy(sortItem(nullLiteral))),
            Some(skip(1)),
            where = Some(where(SignedOctalIntegerLiteral("0o1")(pos)))
          )),
          returnCypher5Types
        )(pos),
        returnClause(
          returnAllItems(
            ast.AliasedReturnItem(isNotNull(literalString("")), varFor("ጤ", variablesAreEscaped))(pos)
          ),
          Some(orderBy(
            ast.DescSortItem(SignedHexIntegerLiteral("-0x1")(pos))(pos),
            ast.DescSortItem(varFor("怭", variablesAreEscaped))(pos)
          )),
          Some(ast.Limit(nullLiteral)(pos)),
          skip = Some(ast.Skip(literalFloat(1.9121409685506285E89))(pos))
        )
      )
    parsesIn[ast.Statement] {
      case Cypher5 => _.toAst(expected(variablesAreEscaped = true, returnCypher5Types = true, resolveStrictly = false))
      case _       => _.toAst(expected(variablesAreEscaped = false, returnCypher5Types = false, resolveStrictly = true))
    }
  }

  test(
    "SHOW TRANSACTIONS YIELD a1, b1 AS c1, d1 AS d1, e1 AS f1, g1 AS e1 ORDER BY a1, b1, d1, e1 WHERE a1 AND b1 AND d1 AND e1 " +
      "TERMINATE TRANSACTIONS 'id' YIELD a2, b2 AS c2, d2 AS d2, e2 AS f2, g2 AS e2 ORDER BY a2, b2, d2, e2 WHERE a2 AND b2 AND d2 AND e2 " +
      "SHOW SETTINGS YIELD a3, b3 AS c3, d3 AS d3, e3 AS f3, g3 AS e3 ORDER BY a3, b3, d3, e3 WHERE a3 AND b3 AND d3 AND e3 " +
      "SHOW FUNCTIONS YIELD a4, b4 AS c4, d4 AS d4, e4 AS f4, g4 AS e4 ORDER BY a4, b4, d4, e4 WHERE a4 AND b4 AND d4 AND e4 " +
      "SHOW PROCEDURES YIELD a5, b5 AS c5, d5 AS d5, e5 AS f5, g5 AS e5 ORDER BY a5, b5, d5, e5 WHERE a5 AND b5 AND d5 AND e5 " +
      "SHOW INDEXES YIELD a6, b6 AS c6, d6 AS d6, e6 AS f6, g6 AS e6 ORDER BY a6, b6, d6, e6 WHERE a6 AND b6 AND d6 AND e6 " +
      "SHOW CONSTRAINTS YIELD a7, b7 AS c7, d7 AS d7, e7 AS f7, g7 AS e7 ORDER BY a7, b7, d7, e7 WHERE a7 AND b7 AND d7 AND e7 " +
      "RETURN *"
  ) {
    assertAstVersionAware(
      supportedInCypher5 = false,
      showTx(
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
      ),
      terminateTx(
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
      ),
      showSetting(
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
      ),
      returnAll
    )
  }

  test(
    "SHOW TRANSACTIONS YIELD a1, b1 AS c1, d1 AS d1, e1 AS f1, g1 AS e1 ORDER BY a1, b1, d1, e1 WHERE a1 AND b1 AND d1 AND e1 " +
      "TERMINATE TRANSACTIONS 'id' YIELD a2, b2 AS c2, d2 AS d2, e2 AS f2, g2 AS e2 ORDER BY a2, b2, d2, e2 WHERE a2 AND b2 AND d2 AND e2 " +
      "SHOW SETTINGS YIELD a3, b3 AS c3, d3 AS d3, e3 AS f3, g3 AS e3 ORDER BY a3, b3, d3, e3 WHERE a3 AND b3 AND d3 AND e3 " +
      "SHOW FUNCTIONS YIELD a4, b4 AS c4, d4 AS d4, e4 AS f4, g4 AS e4 ORDER BY a4, b4, d4, e4 WHERE a4 AND b4 AND d4 AND e4 " +
      "SHOW PROCEDURES YIELD a5, b5 AS c5, d5 AS d5, e5 AS f5, g5 AS e5 ORDER BY a5, b5, d5, e5 WHERE a5 AND b5 AND d5 AND e5 " +
      "SHOW INDEXES YIELD a6, b6 AS c6, d6 AS d6, e6 AS f6, g6 AS e6 ORDER BY a6, b6, d6, e6 WHERE a6 AND b6 AND d6 AND e6 " +
      "SHOW CONSTRAINTS YIELD a7, b7 AS c7, d7 AS d7, e7 AS f7, g7 AS e7 ORDER BY a7, b7, d7, e7 WHERE a7 AND b7 AND d7 AND e7 " +
      "SHOW CURRENT GRAPH TYPE YIELD a8, b8 AS c8, d8 AS d8, e8 AS f8, g8 AS e8 ORDER BY a8, b8, d8, e8 WHERE a8 AND b8 AND d8 AND e8 " +
      "SHOW HOME DATABASE YIELD a9, b9 AS c9, d9 AS d9, e9 AS f9, g9 AS e9 ORDER BY a9, b9, d9, e9 WHERE a9 AND b9 AND d9 AND e9 " +
      "RETURN *"
  ) {
    assertAstVersionAware(
      supportedInCypher5 = false,
      showTx(
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
      ),
      terminateTx(
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
      ),
      showSetting(
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
      ),
      showCurrentGraphType(
        asGraph = false,
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
      ),
      showDatabase(
        ast.HomeDatabaseScope()(pos),
        None,
        yieldAll = false,
        List(
          commandResultItem("a9"),
          commandResultItem("b9", Some("c9")),
          commandResultItem("d9", Some("d9")),
          commandResultItem("e9", Some("f9")),
          commandResultItem("g9", Some("e9"))
        ),
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a9", "c9", "d9", "f9", "e9")),
          Some(orderBy(
            sortItem(varFor("a9")),
            sortItem(varFor("c9")),
            sortItem(varFor("d9")),
            sortItem(varFor("e9"))
          )),
          where = Some(where(
            and(
              and(
                and(
                  varFor("a9"),
                  varFor("c9")
                ),
                varFor("d9")
              ),
              varFor("e9")
            )
          ))
        ))
      ),
      returnAll
    )
  }

  test(
    "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "RETURN *"
  ) {
    assertAstVersionAware(
      supportedInCypher5 = false,
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      returnAll
    )
  }

  test(
    "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW CURRENT GRAPH TYPE YIELD a " +
      "SHOW DATABASES YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW CURRENT GRAPH TYPE YIELD a " +
      "SHOW DATABASES YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW CURRENT GRAPH TYPE YIELD a " +
      "SHOW DATABASES YIELD a " +
      "SHOW TRANSACTIONS YIELD a " +
      "TERMINATE TRANSACTIONS 'id' YIELD a " +
      "SHOW SETTINGS YIELD a " +
      "SHOW FUNCTIONS YIELD a " +
      "SHOW PROCEDURES YIELD a " +
      "SHOW INDEXES YIELD a " +
      "SHOW CONSTRAINTS YIELD a " +
      "SHOW CURRENT GRAPH TYPE YIELD a " +
      "SHOW DATABASES YIELD a " +
      "RETURN *"
  ) {
    assertAstVersionAware(
      false,
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showCurrentGraphType(
        asGraph = false,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showDatabase(
        ast.AllDatabasesScope()(pos),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showCurrentGraphType(
        asGraph = false,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showDatabase(
        ast.AllDatabasesScope()(pos),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showCurrentGraphType(
        asGraph = false,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showDatabase(
        ast.AllDatabasesScope()(pos),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      terminateTx(
        ExpressionNames(literalString("id")),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showSetting(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showFunction(
        ast.AllFunctions,
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showProcedure(
        None,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showIndex(
        ast.AllIndexes,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showConstraint(
        ast.AllConstraints,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showCurrentGraphType(
        asGraph = false,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      showDatabase(
        ast.AllDatabasesScope()(pos),
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      ),
      returnAll
    )
  }

  private val manyCommands = (for (_ <- 1 to 300) yield "SHOW TRANSACTIONS YIELD a").mkString(" ")

  test(manyCommands) {
    val clauses = (for (_ <- 1 to 300) yield List(
      showTx(
        NoNames,
        None,
        yieldAll = false,
        List(commandResultItem("a")),
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("a"))))
      )(pos)
    )).flatten
    assertAst(clauses: _*)
  }

  test("TERMINATE TRANSACTION SHOW SETTINGS") {
    // missing id for terminate transaction (parses the SHOW as the id)
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'SETTINGS': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF> (line 1, column 28 (offset: 27))
            |"TERMINATE TRANSACTION SHOW SETTINGS"
            |                            ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'SETTINGS': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF> (line 1, column 28 (offset: 27))
            |"TERMINATE TRANSACTION SHOW SETTINGS"
            |                            ^""".stripMargin
        )
    }
  }

  test("SHOW FUNCTIONS TERMINATE TRANSACTION") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'TERMINATE': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF> (line 1, column 16 (offset: 15))
            |"SHOW FUNCTIONS TERMINATE TRANSACTION"
            |                ^""".stripMargin
        )
      // missing id for terminate transaction
      case _ => _.withSyntaxError(
          """Invalid input '': expected a string or an expression (line 1, column 37 (offset: 36))
            |"SHOW FUNCTIONS TERMINATE TRANSACTION"
            |                                     ^""".stripMargin
        )
    }
  }

  test("SHOW TRANSACTIONS TERMINATE TRANSACTION") {
    // missing id for terminate transaction
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '': expected a string or an expression (line 1, column 40 (offset: 39))
        |"SHOW TRANSACTIONS TERMINATE TRANSACTION"
        |                                        ^""".stripMargin
    )
  }

  // show indexes/constraints brief/verbose when combined with other commands

  test("SHOW CONSTRAINTS BRIEF SHOW CONSTRAINTS") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CONSTRAINTS VERBOSE SHOW CONSTRAINTS") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CONSTRAINTS SHOW CONSTRAINTS BRIEF") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CONSTRAINTS SHOW CONSTRAINTS VERBOSE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CONSTRAINTS BRIEF SHOW CONSTRAINTS VERBOSE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CONSTRAINTS BRIEF SHOW PROCEDURES") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CONSTRAINTS VERBOSE SHOW FUNCTIONS") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW FUNCTIONS SHOW CONSTRAINTS BRIEF") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW PROCEDURES SHOW CONSTRAINTS VERBOSE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CONSTRAINTS VERBOSE SHOW CURRENT GRAPH TYPE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CURRENT GRAPH TYPE SHOW CONSTRAINTS BRIEF") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'GRAPH', expected: 'USER'."
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW INDEXES BRIEF SHOW INDEXES") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW INDEXES VERBOSE SHOW INDEXES") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW INDEXES SHOW INDEXES BRIEF") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW INDEXES SHOW INDEXES VERBOSE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'BRIEF', 'VERBOSE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW BTREE INDEXES VERBOSE SHOW BTREE INDEXES BRIEF") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'BTREE': expected 'ALIAS', 'ALIASES', 'ALL', 'AUTH', 'CONSTRAINT', 'CONSTRAINTS', 'CURRENT', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', " +
            "'EXIST', 'EXISTENCE', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', " +
            "'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', " +
            "'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'USERS' or 'VECTOR'"
        )
    }
  }

  test("SHOW INDEXES BRIEF SHOW PROCEDURES") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW INDEXES VERBOSE SHOW FUNCTIONS") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW FUNCTIONS SHOW INDEXES BRIEF") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW PROCEDURES SHOW INDEXES VERBOSE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SHOW': expected 'EXECUTABLE', 'WHERE', 'YIELD' or <EOF> ("
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW INDEXES BRIEF SHOW CURRENT GRAPH TYPE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(
          """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
            |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BRIEF': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }

  test("SHOW CURRENT GRAPH TYPE SHOW INDEXES VERBOSE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'GRAPH', expected: 'USER'."
        )
      case _ =>
        _.withSyntaxErrorContaining(
          "Invalid input 'VERBOSE': expected 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', " +
            "'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
            "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
        )
    }
  }
}
