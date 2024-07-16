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
package org.neo4j.cypher.internal.parser.v5.ast.factory

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.SyntaxChecker
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.cast
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.common.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.parser.common.ast.factory.ConstraintType
import org.neo4j.cypher.internal.parser.common.ast.factory.HintIndexType
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.ConstraintExistsContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.ConstraintIsNotNullContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.ConstraintIsUniqueContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.ConstraintKeyContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.ConstraintTypedContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.DropConstraintContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.GlobContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.GlobRecursiveContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.SymbolicAliasNameOrParameterContext
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5SyntaxChecker.MAX_ALIAS_NAME_COMPONENTS
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5SyntaxChecker.MAX_DATABASE_NAME_COMPONENTS
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.internal.helpers.NameUtil

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala

final class Cypher5SyntaxChecker(exceptionFactory: CypherExceptionFactory) extends SyntaxChecker {
  private[this] var _errors: Seq[Exception] = Seq.empty

  override def errors: Seq[Throwable] = _errors

  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}

  override def exitEveryRule(ctx: ParserRuleContext): Unit = {
    // Note, this has been shown to be significantly faster than using the generated listener.
    // Compiles into a lookupswitch (or possibly tableswitch)
    ctx.getRuleIndex match {
      case Cypher5Parser.RULE_periodicCommitQueryHintFailure   => checkPeriodicCommitQueryHintFailure(cast(ctx))
      case Cypher5Parser.RULE_subqueryInTransactionsParameters => checkSubqueryInTransactionsParameters(cast(ctx))
      case Cypher5Parser.RULE_createCommand                    => checkCreateCommand(cast(ctx))
      case Cypher5Parser.RULE_createConstraint                 => checkCreateConstraint(cast(ctx))
      case Cypher5Parser.RULE_enclosedPropertyList             => checkEnclosedPropertyList(cast(ctx))
      case Cypher5Parser.RULE_dropConstraint                   => checkDropConstraint(cast(ctx))
      case Cypher5Parser.RULE_showConstraintCommand            => checkShowConstraint(cast(ctx))
      case Cypher5Parser.RULE_showBriefAndYield                => checkBriefAndVerbose(cast(ctx))
      case Cypher5Parser.RULE_dropIndex                        => checkDropIndex(cast(ctx))
      case Cypher5Parser.RULE_createLookupIndex                => checkCreateLookupIndex(cast(ctx))
      case Cypher5Parser.RULE_createUser                       => checkCreateUser(cast(ctx))
      case Cypher5Parser.RULE_alterUser                        => checkAlterUser(cast(ctx))
      case Cypher5Parser.RULE_allPrivilege                     => checkAllPrivilege(cast(ctx))
      case Cypher5Parser.RULE_createDatabase                   => checkCreateDatabase(cast(ctx))
      case Cypher5Parser.RULE_alterDatabase                    => checkAlterDatabase(cast(ctx))
      case Cypher5Parser.RULE_alterDatabaseTopology            => checkAlterDatabaseTopology(cast(ctx))
      case Cypher5Parser.RULE_createAlias                      => checkCreateAlias(cast(ctx))
      case Cypher5Parser.RULE_alterAlias                       => checkAlterAlias(cast(ctx))
      case Cypher5Parser.RULE_globPart                         => checkGlobPart(cast(ctx))
      case Cypher5Parser.RULE_insertPattern                    => checkInsertPattern(cast(ctx))
      case Cypher5Parser.RULE_insertNodeLabelExpression        => checkInsertLabelConjunction(cast(ctx))
      case Cypher5Parser.RULE_functionInvocation               => checkFunctionInvocation(cast(ctx))
      case Cypher5Parser.RULE_typePart                         => checkTypePart(cast(ctx))
      case Cypher5Parser.RULE_hint                             => checkHint(cast(ctx))
      case Cypher5Parser.RULE_symbolicAliasNameOrParameter     => checkSymbolicAliasNameOrParameter(cast(ctx))
      case _                                                   =>
    }
  }

  override def check(ctx: ParserRuleContext): Boolean = {
    exitEveryRule(ctx)
    _errors.isEmpty
  }

  private def inputPosition(symbol: Token): InputPosition = {
    InputPosition(symbol.getStartIndex, symbol.getLine, symbol.getCharPositionInLine + 1)
  }

  private def errorOnDuplicate(
    token: Token,
    description: String,
    isParam: Boolean
  ): Unit = {
    if (isParam) {
      _errors :+= exceptionFactory.syntaxException(
        s"Duplicated $description parameters",
        inputPosition(token)
      )
    } else {
      _errors :+= exceptionFactory.syntaxException(
        s"Duplicate $description clause",
        inputPosition(token)
      )

    }
  }

  private def errorOnDuplicateCtx[T <: AstRuleCtx](
    ctx: java.util.List[T],
    description: String,
    isParam: Boolean = false
  ): Unit = {
    if (ctx.size > 1) {
      errorOnDuplicate(nodeChild(ctx.get(1), 0).getSymbol, description, isParam)
    }
  }

  private def errorOnDuplicateRule[T <: ParserRuleContext](
    params: java.util.List[T],
    description: String,
    isParam: Boolean = false
  ): Unit = {
    if (params.size() > 1) {
      errorOnDuplicate(params.get(1).start, description, isParam)
    }
  }

  private def errorOnAliasNameContainingDots(aliasesNames: java.util.List[SymbolicAliasNameOrParameterContext])
    : Unit = {
    if (aliasesNames.size() > 0) {
      val aliasName = aliasesNames.get(0)
      if (
        aliasName.symbolicAliasName() != null && aliasName.symbolicAliasName().symbolicNameString().size() > MAX_ALIAS_NAME_COMPONENTS
      ) {
        val start = aliasName.symbolicAliasName().symbolicNameString().get(0).getStart
        _errors :+= exceptionFactory.syntaxException(
          s"'.' is not a valid character in the remote alias name '${aliasName.getText}'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`.",
          inputPosition(start)
        )
      }
    }
  }

  private def errorOnAliasNameContainingTooManyComponents(
    aliasesNames: Seq[SymbolicAliasNameOrParameterContext],
    maxComponents: Int,
    errorTemplate: String
  ): Unit = {
    if (aliasesNames.nonEmpty) {
      val literalAliasNames = aliasesNames.filter(_.symbolicAliasName() != null)
      for (aliasName <- literalAliasNames) {
        val nameComponents = aliasName.symbolicAliasName().symbolicNameString().asScala.toList
        val componentCount = nameComponents.sliding(2, 1).foldLeft(1) {
          case (count, a :: b :: Nil)
            if a.escapedSymbolicNameString() != null || b.escapedSymbolicNameString() != null => count + 1
          case (count, _) => count
        }
        if (componentCount > maxComponents) {
          val start = aliasName.symbolicAliasName().symbolicNameString().get(0).getStart
          _errors :+= exceptionFactory.syntaxException(
            errorTemplate.formatted(
              aliasName.symbolicAliasName().symbolicNameString().asScala.map {
                case context if context.unescapedSymbolicNameString() != null =>
                  context.unescapedSymbolicNameString().ast
                case context if context.escapedSymbolicNameString() != null =>
                  NameUtil.forceEscapeName(context.escapedSymbolicNameString().ast())
                case _ => ""
              }.mkString(".")
            ),
            inputPosition(start)
          )
        }
      }
    }
  }

  private def checkSubqueryInTransactionsParameters(ctx: Cypher5Parser.SubqueryInTransactionsParametersContext)
    : Unit = {
    errorOnDuplicateRule(ctx.subqueryInTransactionsBatchParameters(), "OF ROWS", isParam = true)
    errorOnDuplicateRule(ctx.subqueryInTransactionsErrorParameters(), "ON ERROR", isParam = true)
    errorOnDuplicateRule(ctx.subqueryInTransactionsReportParameters(), "REPORT STATUS", isParam = true)
  }

  private def checkCreateAlias(ctx: Cypher5Parser.CreateAliasContext): Unit = {
    if (ctx.stringOrParameter() != null) {
      if (!(ctx.AT() == null && ctx.USER() == null && ctx.PASSWORD() == null && ctx.DRIVER() == null))
        errorOnAliasNameContainingDots(ctx.symbolicAliasNameOrParameter())
    }
  }

  private def checkAlterAlias(ctx: Cypher5Parser.AlterAliasContext): Unit = {
    val aliasTargets = ctx.alterAliasTarget()
    val hasUrl = !aliasTargets.isEmpty && aliasTargets.get(0).AT() != null
    val usernames = ctx.alterAliasUser()
    val passwords = ctx.alterAliasPassword()
    val driverSettings = ctx.alterAliasDriver()

    // Should only be checked in case of remote
    if (hasUrl || !usernames.isEmpty || !passwords.isEmpty || !driverSettings.isEmpty)
      errorOnAliasNameContainingDots(java.util.List.of(ctx.symbolicAliasNameOrParameter()))

    errorOnDuplicateCtx(driverSettings, "DRIVER")
    errorOnDuplicateCtx(usernames, "USER")
    errorOnDuplicateCtx(passwords, "PASSWORD")
    errorOnDuplicateCtx(ctx.alterAliasProperties(), "PROPERTIES")
    errorOnDuplicateCtx(aliasTargets, "TARGET")
  }

  private def checkSymbolicAliasNameOrParameter(ctx: Cypher5Parser.SymbolicAliasNameOrParameterContext): Unit = {
    ctx.getParent.getRuleIndex match {
      case Cypher5Parser.RULE_createDatabase =>
        // `a`.`b` disallowed
        errorOnAliasNameContainingTooManyComponents(
          Seq(ctx),
          MAX_DATABASE_NAME_COMPONENTS,
          "Invalid input `%s` for database name. Expected name to contain at most one component."
        )
      case Cypher5Parser.RULE_createCompositeDatabase =>
      // Handled in semantic checks
      case _ =>
        // `a`.`b` allowed, `a`.`b`.`c` disallowed
        errorOnAliasNameContainingTooManyComponents(
          Seq(ctx),
          MAX_ALIAS_NAME_COMPONENTS,
          "Invalid input `%s` for name. Expected name to contain at most two components separated by `.`."
        )
    }
  }

  private def checkCreateUser(ctx: Cypher5Parser.CreateUserContext): Unit = {
    errorOnDuplicateRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicateRule(ctx.homeDatabase(), "SET HOME DATABASE")
  }

  private def checkAlterUser(ctx: Cypher5Parser.AlterUserContext): Unit = {
    errorOnDuplicateRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicateRule(ctx.homeDatabase(), "SET HOME DATABASE")
  }

  private def checkAllPrivilege(ctx: Cypher5Parser.AllPrivilegeContext): Unit = {
    val privilegeType = ctx.allPrivilegeType()
    val privilegeTarget = ctx.allPrivilegeTarget()

    if (privilegeType != null) {
      val privilege =
        if (privilegeType.GRAPH() != null) Some("GRAPH")
        else if (privilegeType.DBMS() != null) Some("DBMS")
        else if (privilegeType.DATABASE() != null) Some("DATABASE")
        else None

      val target = privilegeTarget match {
        case c: Cypher5Parser.DefaultTargetContext =>
          privilege match {
            case Some("DBMS") =>
              if (c.HOME() != null) ("HOME", c.HOME().getSymbol)
              else ("DEFAULT", c.DEFAULT().getSymbol)
            case _ =>
              if (c.GRAPH() != null) ("GRAPH", c.GRAPH().getSymbol)
              else ("DATABASE", c.DATABASE().getSymbol)
          }
        case c: Cypher5Parser.DatabaseVariableTargetContext =>
          if (c.DATABASE() != null) ("DATABASE", c.DATABASE().getSymbol)
          else ("DATABASES", c.DATABASES().getSymbol)
        case c: Cypher5Parser.GraphVariableTargetContext =>
          if (c.GRAPH() != null) ("GRAPH", c.GRAPH().getSymbol)
          else ("GRAPHS", c.GRAPHS().getSymbol)
        case c: Cypher5Parser.DBMSTargetContext =>
          ("DBMS", c.DBMS().getSymbol)
        case _ => throw new IllegalStateException("Unexpected privilege all command")
      }
      (privilege, target) match {
        case (Some(privilege), (target, symbol)) =>
          // This makes GRANT ALL DATABASE PRIVILEGES ON DATABASES * work
          if (!target.startsWith(privilege)) {
            _errors :+= exceptionFactory.syntaxException(
              s"Invalid input '$target': expected \"$privilege\"",
              inputPosition(symbol)
            )
          }
        case _ =>
      }
    }
  }

  private def checkGlobPart(ctx: Cypher5Parser.GlobPartContext): Unit = {
    if (ctx.DOT() == null) {
      ctx.parent.parent match {
        case r: GlobRecursiveContext if r.globPart().escapedSymbolicNameString() != null =>
          addError()

        case r: GlobContext if r.escapedSymbolicNameString() != null =>
          addError()

        case _ =>
      }

      def addError(): Unit = {
        _errors :+= exceptionFactory.syntaxException(
          "Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all.",
          inputPosition(ctx.start)
        )
      }
    }
  }

  private def checkCreateConstraint(ctx: Cypher5Parser.CreateConstraintContext): Unit = {
    // Error messages for mixing old and new constraint syntax
    val errorMessageOnRequire: String =
      "Invalid constraint syntax, ON should not be used in combination with REQUIRE. Replace ON with FOR."

    val errorMessageForAssert: String =
      "Invalid constraint syntax, FOR should not be used in combination with ASSERT. Replace ASSERT with REQUIRE."

    val errorMessageForAssertExists: String =
      "Invalid constraint syntax, FOR should not be used in combination with ASSERT EXISTS. Replace ASSERT EXISTS with REQUIRE ... IS NOT NULL."

    val errorMessageOnAssert: String =
      "Invalid constraint syntax, ON and ASSERT should not be used. Replace ON with FOR and ASSERT with REQUIRE."

    val errorMessageOnAssertExists: String =
      "Invalid constraint syntax, ON and ASSERT EXISTS should not be used. Replace ON with FOR and ASSERT EXISTS with REQUIRE ... IS NOT NULL."

    def checkForInvalidExistence(assert: TerminalNode): Unit = {
      val containsOn = ctx.ON() != null
      val containsFor = ctx.FOR() != null
      val containsAssert = assert != null

      if (containsFor && containsAssert) {
        // FOR ... ASSERT EXISTS ...
        _errors :+= exceptionFactory.syntaxException(
          errorMessageForAssertExists,
          inputPosition(assert.getSymbol)
        )
      } else if (containsOn && containsAssert) {
        // ON ... ASSERT EXISTS ...
        _errors :+= exceptionFactory.syntaxException(
          errorMessageOnAssertExists,
          inputPosition(ctx.ON().getSymbol)
        )
      }
    }

    def checkForInvalidOthers(assert: TerminalNode, require: TerminalNode): Unit = {
      val containsOn = ctx.ON() != null
      val containsFor = ctx.FOR() != null
      val containsAssert = assert != null
      val containsRequire = require != null

      if (containsOn && containsRequire) {
        // ON ... REQUIRE
        _errors :+= exceptionFactory.syntaxException(
          errorMessageOnRequire,
          inputPosition(ctx.ON().getSymbol)
        )
      } else if (containsFor && containsAssert) {
        // FOR ... ASSERT
        _errors :+= exceptionFactory.syntaxException(
          errorMessageForAssert,
          inputPosition(assert.getSymbol)
        )
      } else if (containsOn && containsAssert) {
        // ON ... ASSERT
        _errors :+= exceptionFactory.syntaxException(
          errorMessageOnAssert,
          inputPosition(ctx.ON().getSymbol)
        )
      }
    }

    ctx.constraintType() match {
      case c: ConstraintIsUniqueContext =>
        if (ctx.commandNodePattern() != null && (c.RELATIONSHIP() != null || c.REL() != null)) {
          _errors :+= exceptionFactory.syntaxException(
            s"'${ConstraintType.REL_UNIQUE.description()}' does not allow node patterns",
            inputPosition(ctx.commandNodePattern().getStart)
          )
        }
        if (ctx.commandRelPattern() != null && c.NODE() != null) {
          _errors :+= exceptionFactory.syntaxException(
            s"'${ConstraintType.NODE_UNIQUE.description()}' does not allow relationship patterns",
            inputPosition(ctx.commandRelPattern().getStart)
          )
        }
        checkForInvalidOthers(c.ASSERT(), c.REQUIRE())
      case c: ConstraintKeyContext =>
        if (ctx.commandNodePattern() != null && (c.RELATIONSHIP() != null || c.REL() != null)) {
          _errors :+= exceptionFactory.syntaxException(
            s"'${ConstraintType.REL_KEY.description()}' does not allow node patterns",
            inputPosition(ctx.commandNodePattern().getStart)
          )
        }
        if (ctx.commandRelPattern() != null && c.NODE() != null) {
          _errors :+= exceptionFactory.syntaxException(
            s"'${ConstraintType.NODE_KEY.description()}' does not allow relationship patterns",
            inputPosition(ctx.commandRelPattern().getStart)
          )
        }
        checkForInvalidOthers(c.ASSERT(), c.REQUIRE())
      case c: ConstraintTypedContext =>
        checkForInvalidOthers(c.ASSERT(), c.REQUIRE())
      case c: ConstraintIsNotNullContext =>
        checkForInvalidOthers(c.ASSERT(), c.REQUIRE())
      case c: ConstraintExistsContext =>
        checkForInvalidExistence(c.ASSERT())
      case _ =>
        _errors :+= exceptionFactory.syntaxException(
          "Constraint type is not recognized",
          inputPosition(ctx.constraintType().getStart)
        )
    }
  }

  private def checkEnclosedPropertyList(ctx: Cypher5Parser.EnclosedPropertyListContext): Unit = {
    if (ctx.property().size() > 1 && ctx.getParent != null) {
      val secondProperty = ctx.property(1).start
      ctx.getParent.getParent match {
        case _: ConstraintExistsContext =>
          _errors :+= exceptionFactory.syntaxException(
            "Constraint type 'EXISTS' does not allow multiple properties",
            inputPosition(secondProperty)
          )
        case _: ConstraintTypedContext =>
          _errors :+= exceptionFactory.syntaxException(
            "Constraint type 'IS TYPED' does not allow multiple properties",
            inputPosition(secondProperty)
          )
        case _: ConstraintIsNotNullContext =>
          _errors :+= exceptionFactory.syntaxException(
            "Constraint type 'IS NOT NULL' does not allow multiple properties",
            inputPosition(secondProperty)
          )
        case dropCtx: DropConstraintContext if dropCtx.EXISTS() != null =>
          _errors :+= exceptionFactory.syntaxException(
            "Constraint type 'EXISTS' does not allow multiple properties",
            inputPosition(secondProperty)
          )
        case _ =>
      }
    }
  }

  private def checkDropConstraint(ctx: Cypher5Parser.DropConstraintContext): Unit = {
    val relPattern = ctx.commandRelPattern()
    if (relPattern != null) {
      val errorMessageEnd = "does not allow relationship patterns"
      if (ctx.KEY() != null) {
        _errors :+= exceptionFactory.syntaxException(
          s"'${ConstraintType.NODE_KEY.description()}' $errorMessageEnd",
          inputPosition(relPattern.getStart)
        )
      } else if (ctx.UNIQUE() != null) {
        _errors :+= exceptionFactory.syntaxException(
          s"'${ConstraintType.NODE_UNIQUE.description()}' $errorMessageEnd",
          inputPosition(relPattern.getStart)
        )
      }
    }

    val alwaysInvalidDropCommand = "Unsupported drop constraint command: Please delete the constraint by name instead"
    if (ctx.NULL() != null) {
      _errors :+= exceptionFactory.syntaxException(
        alwaysInvalidDropCommand,
        inputPosition(ctx.start)
      )
    }

    val constraintName = ctx.symbolicNameOrStringParameter()
    if (constraintName == null) {
      // old drop constraint by schema
      def invalidPreviouslyAllowedDropConstraint(constraintType: String) =
        s"$constraintType constraints cannot be dropped by schema, please drop by name instead: DROP CONSTRAINT constraint_name. The constraint name can be found using SHOW CONSTRAINTS."

      if (ctx.commandNodePattern() != null) {
        if (ctx.EXISTS() != null) {
          _errors :+= exceptionFactory.syntaxException(
            invalidPreviouslyAllowedDropConstraint("Node property existence"),
            inputPosition(ctx.start)
          )
        } else if (ctx.UNIQUE() != null) {
          _errors :+= exceptionFactory.syntaxException(
            invalidPreviouslyAllowedDropConstraint("Uniqueness"),
            inputPosition(ctx.start)
          )
        } else if (ctx.KEY() != null) {
          _errors :+= exceptionFactory.syntaxException(
            invalidPreviouslyAllowedDropConstraint("Node key"),
            inputPosition(ctx.start)
          )
        } else {
          _errors :+= exceptionFactory.syntaxException(
            alwaysInvalidDropCommand,
            inputPosition(ctx.start)
          )
        }
      } else {
        if (ctx.EXISTS() != null) {
          _errors :+= exceptionFactory.syntaxException(
            invalidPreviouslyAllowedDropConstraint("Relationship property existence"),
            inputPosition(ctx.start)
          )
        } else {
          _errors :+= exceptionFactory.syntaxException(
            alwaysInvalidDropCommand,
            inputPosition(ctx.start)
          )
        }
      }
    }
  }

  private def checkShowConstraint(ctx: Cypher5Parser.ShowConstraintCommandContext): Unit = {
    ctx match {
      case c: Cypher5Parser.ShowConstraintOldExistsContext =>
        _errors :+= exceptionFactory.syntaxException(
          "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead.",
          inputPosition(c.EXISTS().getSymbol)
        )
      case _ =>
    }
  }

  private def checkBriefAndVerbose(ctx: Cypher5Parser.ShowBriefAndYieldContext): Unit = {
    if (ctx.BRIEF() != null || ctx.VERBOSE() != null) {
      val posSymbol =
        if (ctx.BRIEF() != null) ctx.BRIEF().getSymbol
        else ctx.VERBOSE().getSymbol

      val command = ctx.parent match {
        case _: Cypher5Parser.ShowIndexesAllowBriefContext =>
          "SHOW INDEXES"
        case _: Cypher5Parser.ShowConstraintsAllowBriefAndYieldContext =>
          "SHOW CONSTRAINTS"
        case _ =>
          // should not get here
          ""
      }

      _errors :+= exceptionFactory.syntaxException(
        s"""`$command` no longer allows the `BRIEF` and `VERBOSE` keywords,
           |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin,
        inputPosition(posSymbol)
      )
    }
  }

  private def checkCreateDatabase(ctx: Cypher5Parser.CreateDatabaseContext): Unit = {
    errorOnDuplicateRule[Cypher5Parser.PrimaryTopologyContext](ctx.primaryTopology(), "PRIMARY")
    errorOnDuplicateRule[Cypher5Parser.SecondaryTopologyContext](ctx.secondaryTopology(), "SECONDARY")
  }

  private def checkAlterDatabase(ctx: Cypher5Parser.AlterDatabaseContext): Unit = {
    if (!ctx.REMOVE().isEmpty) {
      val keyNames = astSeq[String](ctx.symbolicNameString())
      val keySet = scala.collection.mutable.Set.empty[String]
      var i = 0
      keyNames.foreach(k =>
        if (keySet.contains(k)) {
          _errors :+= exceptionFactory.syntaxException(
            s"Duplicate 'REMOVE OPTION $k' clause",
            pos(ctx.symbolicNameString(i))
          )
        } else {
          keySet.addOne(k)
          i += 1
        }
      )
    }

    if (!ctx.alterDatabaseOption().isEmpty) {
      val optionCtxs = astSeq[Map[String, Expression]](ctx.alterDatabaseOption())
      // TODO odd why can m be null, shouldn't it fail before this.
      val keyNames = optionCtxs.flatMap(m => if (m != null) m.keys else Seq.empty)
      val keySet = mutable.Set.empty[String]
      var i = 0
      keyNames.foreach(k =>
        if (keySet.contains(k)) {
          _errors :+= exceptionFactory.syntaxException(
            s"Duplicate 'SET OPTION $k' clause",
            pos(ctx.alterDatabaseOption(i))
          )
        } else {
          keySet.addOne(k)
          i += 1
        }
      )
    }

    errorOnDuplicateCtx(ctx.alterDatabaseAccess(), "ACCESS")

    val topology = ctx.alterDatabaseTopology()
    errorOnDuplicateCtx(topology, "TOPOLOGY")
  }

  private def checkAlterDatabaseTopology(ctx: Cypher5Parser.AlterDatabaseTopologyContext): Unit = {
    errorOnDuplicateRule[Cypher5Parser.PrimaryTopologyContext](ctx.primaryTopology(), "PRIMARY")
    errorOnDuplicateRule[Cypher5Parser.SecondaryTopologyContext](ctx.secondaryTopology(), "SECONDARY")
  }

  private def checkPeriodicCommitQueryHintFailure(ctx: Cypher5Parser.PeriodicCommitQueryHintFailureContext): Unit = {
    val periodic = ctx.PERIODIC().getSymbol

    _errors :+= exceptionFactory.syntaxException(
      "The PERIODIC COMMIT query hint is no longer supported. Please use CALL { ... } IN TRANSACTIONS instead.",
      inputPosition(periodic)
    )
  }

  private def checkCreateCommand(ctx: Cypher5Parser.CreateCommandContext): Unit = {
    val createIndex = ctx.createIndex()
    if (createIndex != null) {
      val replace = ctx.REPLACE()
      val oldIndex = createIndex.oldCreateIndex()

      if (replace != null && oldIndex != null) {
        _errors :+= exceptionFactory.syntaxException(
          "'REPLACE' is not allowed for this index syntax",
          inputPosition(replace.getSymbol)
        )
      }
      if (oldIndex != null) {
        _errors :+= exceptionFactory.syntaxException(
          "Invalid create index syntax, use `CREATE INDEX FOR ...` instead.",
          inputPosition(createIndex.ON().getSymbol)
        )
      }
    }
  }

  private def checkCreateLookupIndex(ctx: Cypher5Parser.CreateLookupIndexContext): Unit = {
    val functionName = ctx.symbolicNameString()
    /* This should not be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH(x)

         This should be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH EACH(x)
     */
    val relPattern = ctx.lookupIndexRelPattern()
    if (functionName.getText.toUpperCase() == "EACH" && relPattern != null && relPattern.EACH() == null) {

      _errors :+= exceptionFactory.syntaxException(
        "Missing function name for the LOOKUP INDEX",
        inputPosition(ctx.LPAREN().getSymbol)
      )
    }
  }

  private def checkDropIndex(ctx: Cypher5Parser.DropIndexContext): Unit = {
    val indexName = ctx.symbolicNameOrStringParameter()
    if (indexName == null) {
      // old drop index by schema
      _errors :+= exceptionFactory.syntaxException(
        "Indexes cannot be dropped by schema, please drop by name instead: DROP INDEX index_name. The index name can be found using SHOW INDEXES.",
        inputPosition(ctx.ON().getSymbol)
      )
    }
  }

  private def checkInsertPattern(ctx: Cypher5Parser.InsertPatternContext): Unit = {
    if (ctx.EQ() != null) {
      _errors :+= exceptionFactory.syntaxException(
        "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name.",
        pos(ctxChild(ctx, 0))
      )
    }
  }

  private def checkInsertLabelConjunction(ctx: Cypher5Parser.InsertNodeLabelExpressionContext): Unit = {
    val colons = ctx.COLON()
    val firstIsColon = nodeChild(ctx, 0).getSymbol.getType == Cypher5Parser.COLON

    if (firstIsColon && colons.size > 1) {
      _errors :+= exceptionFactory.syntaxException(
        "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.",
        inputPosition(colons.get(1).getSymbol)
      )
    } else if (!firstIsColon && colons.size() > 0) {
      _errors :+= exceptionFactory.syntaxException(
        "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.",
        inputPosition(colons.get(0).getSymbol)
      )
    }
  }

  private def checkFunctionInvocation(ctx: Cypher5Parser.FunctionInvocationContext): Unit = {
    val functionName = ctx.functionName().ast[FunctionName]()
    if (
      functionName.name == "normalize" &&
      functionName.namespace.parts.isEmpty &&
      ctx.functionArgument().size == 2
    ) {
      _errors :+= exceptionFactory.syntaxException(
        "Invalid normal form, expected NFC, NFD, NFKC, NFKD",
        ctx.functionArgument(1).expression().ast[Expression]().position
      )
    }
  }

  private def checkTypePart(ctx: Cypher5Parser.TypePartContext): Unit = {
    val cypherType = ctx.typeName().ast
    if (cypherType.isInstanceOf[ClosedDynamicUnionType] && ctx.typeNullability() != null) {
      _errors :+= exceptionFactory.syntaxException(
        "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead.",
        pos(ctx.typeNullability())
      )
    }
  }

  private def checkHint(ctx: Cypher5Parser.HintContext): Unit = {
    nodeChild(ctx, 1).getSymbol.getType match {
      case Cypher5Parser.BTREE => _errors :+= exceptionFactory.syntaxException(
          ASTExceptionFactory.invalidHintIndexType(HintIndexType.BTREE),
          pos(nodeChild(ctx, 1))
        )
      case _ =>
    }
  }
}

object Cypher5SyntaxChecker {
  private val MAX_ALIAS_NAME_COMPONENTS: Int = 2
  private val MAX_DATABASE_NAME_COMPONENTS: Int = 1
}
