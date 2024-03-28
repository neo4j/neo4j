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
package org.neo4j.cypher.internal.cst.factory.neo4j

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeListener
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOpt
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOptFromList
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeq
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.cast
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.ctxChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintExistsContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintIsNotNullContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintIsUniqueContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintKeyContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintTypedContext
import org.neo4j.cypher.internal.parser.CypherParser.GlobContext
import org.neo4j.cypher.internal.parser.CypherParser.GlobRecursiveContext
import org.neo4j.cypher.internal.parser.CypherParser.SymbolicAliasNameOrParameterContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala

final class SyntaxChecker extends ParseTreeListener {
  private val exceptionFactory = new OpenCypherExceptionFactory(None)
  private var errors: Seq[Exception] = Seq.empty

  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}

  override def exitEveryRule(ctx: ParserRuleContext): Unit = {
    // Note, this has been shown to be significantly faster than using the generated listener.
    // Compiles into a lookupswitch (or possibly tableswitch)
    ctx.getRuleIndex match {
      case CypherParser.RULE_periodicCommitQueryHintFailure   => checkPeriodicCommitQueryHintFailure(cast(ctx))
      case CypherParser.RULE_subqueryInTransactionsParameters => checkSubqueryInTransactionsParameters(cast(ctx))
      case CypherParser.RULE_createCommand                    => checkCreateCommand(cast(ctx))
      case CypherParser.RULE_createConstraint                 => checkCreateConstraint(cast(ctx))
      case CypherParser.RULE_dropConstraint                   => checkDropConstraint(cast(ctx))
      case CypherParser.RULE_createLookupIndex                => checkCreateLookupIndex(cast(ctx))
      case CypherParser.RULE_createUser                       => checkCreateUser(cast(ctx))
      case CypherParser.RULE_alterUser                        => checkAlterUser(cast(ctx))
      case CypherParser.RULE_allPrivilege                     => checkAllPrivilege(cast(ctx))
      case CypherParser.RULE_createDatabase                   => checkCreateDatabase(cast(ctx))
      case CypherParser.RULE_alterDatabase                    => checkAlterDatabase(cast(ctx))
      case CypherParser.RULE_createAlias                      => checkCreateAlias(cast(ctx))
      case CypherParser.RULE_alterAlias                       => checkAlterAlias(cast(ctx))
      case CypherParser.RULE_globPart                         => checkGlobPart(cast(ctx))
      case CypherParser.RULE_insertPattern                    => checkInsertPattern(cast(ctx))
      case CypherParser.RULE_insertNodeLabelExpression        => checkInsertLabelConjunction(cast(ctx))
      case CypherParser.RULE_functionInvocation               => checkFunctionInvocation(cast(ctx))
      case _                                                  =>
    }
  }

  def check(ctx: ParserRuleContext): Boolean = {
    exitEveryRule(ctx)
    errors.isEmpty
  }

  def getErrors: Seq[Exception] = errors

  def hasErrors: Boolean = errors.nonEmpty

  private def inputPosition(symbol: Token): InputPosition = {
    new InputPosition(symbol.getStartIndex, symbol.getLine, symbol.getCharPositionInLine + 1)
  }

  private def errorOnDuplicate(
    token: Token,
    description: String,
    isParam: Boolean = false
  ): Unit = {
    if (isParam) {
      errors :+= exceptionFactory.syntaxException(
        s"Duplicated $description parameters",
        inputPosition(token)
      )
    } else {
      errors :+= exceptionFactory.syntaxException(
        s"Duplicate $description clause",
        inputPosition(token)
      )

    }
  }

  private def errorOnDuplicateTokens(
    params: java.util.List[TerminalNode],
    description: String,
    isParam: Boolean = false
  ): Unit = {
    if (params.size() > 1) {
      errorOnDuplicate(params.get(1).getSymbol, description, isParam)
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
      if (aliasName.symbolicAliasName() != null && aliasName.symbolicAliasName().symbolicNameString().size() > 2) {
        val start = aliasName.symbolicAliasName().symbolicNameString().get(0).getStart
        errors :+= exceptionFactory.syntaxException(
          s"'.' is not a valid character in the remote alias name '${aliasName.symbolicAliasName().symbolicNameString().asScala.map(_.getText).mkString}'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`.",
          inputPosition(start)
        )
      }
    }
  }

  private def checkSubqueryInTransactionsParameters(ctx: CypherParser.SubqueryInTransactionsParametersContext): Unit = {
    errorOnDuplicateRule(ctx.subqueryInTransactionsBatchParameters(), "OF ROWS", isParam = true)
    errorOnDuplicateRule(ctx.subqueryInTransactionsErrorParameters(), "ON ERROR", isParam = true)
    errorOnDuplicateRule(ctx.subqueryInTransactionsReportParameters(), "REPORT STATUS AS", isParam = true)
  }

  private def checkCreateAlias(ctx: CypherParser.CreateAliasContext): Unit = {
    if (ctx.stringOrParameter() != null)
      errorOnAliasNameContainingDots(ctx.symbolicAliasNameOrParameter())

  }

  private def checkAlterAlias(ctx: CypherParser.AlterAliasContext): Unit = {
    // Should only be checked in case of remote
    val aliasTargetCtx = ctx.alterAliasTarget()
    val url =
      if (aliasTargetCtx.isEmpty) None else astOpt[Either[String, Parameter]](aliasTargetCtx.get(0).stringOrParameter())
    val username = astOptFromList[Expression](ctx.alterAliasUser(), None)
    val password = astOptFromList[Expression](ctx.alterAliasPassword(), None)
    val driverSettings = astOptFromList[Either[Map[String, Expression], Parameter]](ctx.alterAliasDriver(), None)
    if (url.isDefined || username.isDefined || password.isDefined || driverSettings.isDefined)
      errorOnAliasNameContainingDots(java.util.List.of(ctx.symbolicAliasNameOrParameter()))

    errorOnDuplicateCtx(ctx.alterAliasDriver(), "DRIVER")
    errorOnDuplicateCtx(ctx.alterAliasUser, "USER")
    errorOnDuplicateCtx(ctx.alterAliasPassword(), "PASSWORD")
    errorOnDuplicateCtx(ctx.alterAliasProperties(), "PROPERTIES")
    errorOnDuplicateCtx(aliasTargetCtx, "TARGET")
  }

  private def checkCreateUser(ctx: CypherParser.CreateUserContext): Unit = {
    val changeRequired = ctx.password().passwordChangeRequired()
    if (changeRequired != null && !ctx.PASSWORD().isEmpty) {
      errorOnDuplicate(ctx.PASSWORD().get(0).getSymbol, "SET PASSWORD CHANGE [NOT] REQUIRED")
    } else if (ctx.PASSWORD().size > 1) {
      errorOnDuplicate(ctx.PASSWORD().get(1).getSymbol, "SET PASSWORD CHANGE [NOT] REQUIRED")
    }
    errorOnDuplicateRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicateRule(ctx.homeDatabase(), "SET HOME DATABASE")
  }

  private def checkAlterUser(ctx: CypherParser.AlterUserContext): Unit = {
    val nbrSetPass = ctx.PASSWORD().size + ctx.password().size()
    if (nbrSetPass > 1) {
      if (ctx.PASSWORD().size > 1)
        errorOnDuplicateTokens(ctx.PASSWORD(), "SET PASSWORD CHANGE [NOT] REQUIRED")
      else if (ctx.password().size > 0)
        errorOnDuplicateCtx(ctx.password(), "SET PASSWORD")
    }
    errorOnDuplicateRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicateRule(ctx.homeDatabase(), "SET HOME DATABASE")
  }

  private def checkAllPrivilege(ctx: CypherParser.AllPrivilegeContext): Unit = {
    val privilegeType = ctx.allPrivilegeType()
    val privilegeTarget = ctx.allPrivilegeTarget()

    if (privilegeType != null && privilegeTarget != null) {
      val privilege =
        if (privilegeType.GRAPH() != null) Some("GRAPH")
        else if (privilegeType.DBMS() != null) Some("DBMS")
        else if (privilegeType.DATABASE() != null) Some("DATABASE")
        else None

      val target =
        if (privilegeTarget.GRAPH() != null) Some(("GRAPH", privilegeTarget.GRAPH().getSymbol))
        else if (privilegeTarget.DBMS() != null) Some(("DBMS", privilegeTarget.DBMS().getSymbol))
        else if (privilegeTarget.DATABASE() != null) Some(("DATABASE", privilegeTarget.DATABASE().getSymbol))
        else if (privilegeTarget.DATABASES() != null) Some(("DATABASES", privilegeTarget.DATABASES().getSymbol))
        else None

      (privilege, target) match {
        case (Some(privilege), Some((target, symbol))) =>
          // This makes GRANT ALL DATABASE PRIVILEGES ON DATABASES * work
          if (!target.startsWith(privilege)) {
            errors :+= exceptionFactory.syntaxException(
              s"Invalid input $target': expected \"$privilege\"",
              inputPosition(symbol)
            )
          }
        case _ =>
      }
    }
  }

  private def checkGlobPart(ctx: CypherParser.GlobPartContext): Unit = {
    if (ctx.DOT() == null) {
      ctx.parent.parent match {
        case r: GlobRecursiveContext if r.globPart().escapedSymbolicNameString() != null =>
          addError()

        case r: GlobContext if r.escapedSymbolicNameString() != null =>
          addError()

        case _ =>
      }

      def addError(): Unit = {
        errors :+= exceptionFactory.syntaxException(
          "Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all.",
          inputPosition(ctx.start)
        )
      }
    }
  }

  private def checkCreateConstraint(ctx: CypherParser.CreateConstraintContext): Unit = {

    ctx.constraintType() match {
      case c: ConstraintIsUniqueContext =>
        if (ctx.commandNodePattern() != null && (c.RELATIONSHIP() != null || c.REL() != null)) {
          errors :+= exceptionFactory.syntaxException(
            ConstraintType.REL_UNIQUE.toString ++ " does not allow node patterns",
            inputPosition(ctx.commandNodePattern().getStart)
          )
        }
        if (ctx.commandRelPattern() != null && c.NODE() != null) {
          errors :+= exceptionFactory.syntaxException(
            ConstraintType.NODE_UNIQUE.toString ++ " does not allow relationship patterns",
            inputPosition(ctx.commandRelPattern().getStart)
          )
        }
      case c: ConstraintKeyContext =>
        if (ctx.commandNodePattern() != null && (c.RELATIONSHIP() != null || c.REL() != null)) {
          errors :+= exceptionFactory.syntaxException(
            ConstraintType.REL_KEY.toString ++ " does not allow node patterns",
            inputPosition(ctx.commandNodePattern().getStart)
          )
        }
        if (ctx.commandRelPattern() != null && c.NODE() != null) {
          errors :+= exceptionFactory.syntaxException(
            ConstraintType.NODE_KEY.toString ++ " does not allow relationship patterns",
            inputPosition(ctx.commandRelPattern().getStart)
          )
        }
      case c: ConstraintExistsContext =>
        if (c.propertyList() != null && c.propertyList().property().size() > 1) {
          val secondProperty = c.propertyList().property(1).start
          errors :+= exceptionFactory.syntaxException(
            "Constraint type 'EXISTS' does not allow multiple properties",
            inputPosition(secondProperty)
          )
        }
      case c: ConstraintTypedContext =>
        if (c.propertyList() != null && c.propertyList().property().size() > 1) {
          val secondProperty = c.propertyList().property(1).start
          errors :+= exceptionFactory.syntaxException(
            "Constraint type 'IS TYPED' does not allow multiple properties",
            inputPosition(secondProperty)
          )
        }
      case c: ConstraintIsNotNullContext =>
        if (c.propertyList() != null && c.propertyList().property().size() > 1) {
          val secondProperty = c.propertyList().property(1).start
          errors :+= exceptionFactory.syntaxException(
            "Constraint type 'IS NOT NULL' does not allow multiple properties",
            inputPosition(secondProperty)
          )
        }
      case _ =>
        errors :+= exceptionFactory.syntaxException(
          "Constraint type is not recognized",
          inputPosition(ctx.constraintType().getStart)
        )
    }

  }

  private def checkDropConstraint(ctx: CypherParser.DropConstraintContext): Unit = {
    val relPattern = ctx.commandRelPattern()
    if (relPattern != null) {
      val errorMessageEnd = " does not allow relationship patterns"
      if (ctx.KEY() != null) {
        errors :+= exceptionFactory.syntaxException(
          ConstraintType.NODE_KEY.toString ++ errorMessageEnd,
          inputPosition(relPattern.getStart)
        )
      } else if (ctx.UNIQUE() != null) {
        errors :+= exceptionFactory.syntaxException(
          ConstraintType.NODE_UNIQUE.toString ++ errorMessageEnd,
          inputPosition(relPattern.getStart)
        )
      }
    }

    if (ctx.NULL() != null) {
      errors :+= exceptionFactory.syntaxException(
        "Unsupported drop constraint command: Please delete the constraint by name instead",
        inputPosition(ctx.start)
      )
    }
  }

  private def checkCreateDatabase(ctx: CypherParser.CreateDatabaseContext): Unit = {
    errorOnDuplicateRule[CypherParser.PrimaryTopologyContext](ctx.primaryTopology(), "PRIMARY")
    errorOnDuplicateRule[CypherParser.SecondaryTopologyContext](ctx.secondaryTopology(), "SECONDARY")
  }

  private def checkAlterDatabase(ctx: CypherParser.AlterDatabaseContext): Unit = {
    if (!ctx.REMOVE().isEmpty) {
      val keyNames = astSeq[String](ctx.symbolicNameString())
      val keySet = mutable.Set.empty[String]
      var i = 0
      keyNames.foreach(k =>
        if (keySet.contains(k)) {
          errors :+= exceptionFactory.syntaxException(
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
      val keyNames = optionCtxs.flatMap(m => m.keys)
      val keySet = mutable.Set.empty[String]
      var i = 0
      keyNames.foreach(k =>
        if (keySet.contains(k)) {
          errors :+= exceptionFactory.syntaxException(
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
    if (!topology.isEmpty) {
      errorOnDuplicateRule[CypherParser.PrimaryTopologyContext](topology.get(0).primaryTopology(), "PRIMARY")
      errorOnDuplicateRule[CypherParser.SecondaryTopologyContext](topology.get(0).secondaryTopology(), "SECONDARY")
    }
  }

  private def checkPeriodicCommitQueryHintFailure(ctx: CypherParser.PeriodicCommitQueryHintFailureContext): Unit = {
    val periodic = ctx.PERIODIC().getSymbol

    errors :+= exceptionFactory.syntaxException(
      "The PERIODIC COMMIT query hint is no longer supported. Please use CALL { ... } IN TRANSACTIONS instead.",
      inputPosition(periodic)
    )
  }

  private def checkCreateCommand(ctx: CypherParser.CreateCommandContext): Unit = {
    val createIndex = ctx.createIndex()
    val replace = ctx.REPLACE()

    if (createIndex != null && replace != null) {
      if (createIndex.oldCreateIndex() != null) {
        errors :+= exceptionFactory.syntaxException(
          "'REPLACE' is not allowed for this index syntax",
          inputPosition(replace.getSymbol)
        )
      }
    }
  }

  private def checkCreateLookupIndex(ctx: CypherParser.CreateLookupIndexContext): Unit = {
    val functionName = ctx.symbolicNameString()
    /* This should not be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH(x)

         This should be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH EACH(x)
     */
    val relPattern = ctx.lookupIndexRelPattern()
    if (functionName.getText.toUpperCase() == "EACH" && relPattern != null && relPattern.EACH() == null) {
      errors :+= exceptionFactory.syntaxException(
        "Missing function name for the LOOKUP INDEX",
        inputPosition(functionName.start)
      )
    }
  }

  private def checkInsertPattern(ctx: CypherParser.InsertPatternContext): Unit = {
    if (ctx.EQ() != null) {
      errors :+= exceptionFactory.syntaxException(
        "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name.",
        pos(ctxChild(ctx, 0))
      )
    }
  }

  private def checkInsertLabelConjunction(ctx: CypherParser.InsertNodeLabelExpressionContext): Unit = {
    val colons = ctx.COLON()
    val firstIsColon = nodeChild(ctx, 0).getSymbol.getType == CypherParser.COLON

    if (firstIsColon && colons.size > 1) {
      errors :+= exceptionFactory.syntaxException(
        "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.",
        inputPosition(colons.get(1).getSymbol)
      )
    } else if (!firstIsColon && colons.size() > 0) {
      errors :+= exceptionFactory.syntaxException(
        "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.",
        inputPosition(colons.get(0).getSymbol)
      )
    }
  }

  private def checkFunctionInvocation(ctx: CypherParser.FunctionInvocationContext): Unit = {
    val functionName = ctx.symbolicNameString().ast[String]()
    functionName match {
      case "normalize" =>
        if (ctx.expression().size == 2) {
          errors :+= exceptionFactory.syntaxException(
            "Invalid normal form, expected NFC, NFD, NFKC, NFKD",
            ctx.expression(1).ast[Expression]().position
          )
        }
      case _ =>
    }
  }
}
