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
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.cast
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintNodePatternContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintRelPatternContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateConstraintNodeCheckContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateConstraintRelCheckContext
import org.neo4j.cypher.internal.parser.CypherParser.DropConstraintNodeCheckContext
import org.neo4j.cypher.internal.parser.CypherParser.GlobContext
import org.neo4j.cypher.internal.parser.CypherParser.GlobRecursiveContext
import org.neo4j.cypher.internal.parser.CypherParser.SymbolicAliasNameOrParameterContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

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
      case CypherParser.RULE_insertLabelConjunction           => checkInsertLabelConjunction(cast(ctx))
      case _                                                  =>
    }
  }

  def getErrors: Iterator[Exception] = {
    errors.iterator
  }

  private def inputPosition(symbol: Token): InputPosition = {
    new InputPosition(symbol.getStartIndex, symbol.getLine, symbol.getCharPositionInLine + 1)
  }

  private def errorOnDuplicated(
    token: Token,
    paramDescription: String
  ): Unit = {
    errors :+= exceptionFactory.syntaxException(
      s"Duplicated $paramDescription parameters",
      inputPosition(token)
    )
  }

  private def errorOnDuplicated(
    params: java.util.List[TerminalNode],
    paramDescription: String
  ): Unit = {
    if (params.size() > 1) {
      errorOnDuplicated(params.get(1).getSymbol, paramDescription)
    }
  }

  private def errorOnDuplicatedRule[T <: ParserRuleContext](
    params: java.util.List[T],
    paramDescription: String
  ): Unit = {
    if (params.size() > 1) {
      errorOnDuplicated(params.get(1).start, paramDescription)
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

  private def errorOnRelationshipAndNodeConstraints[S <: ParserRuleContext, T <: ParserRuleContext](
    relPattern: S,
    constraint: T,
    containsKey: T => Boolean
  ): Unit = {
    if (relPattern != null && constraint != null) {
      val nodeType: ConstraintType = if (containsKey(constraint)) {
        ConstraintType.NODE_KEY
      } else {
        ConstraintType.NODE_UNIQUE
      }

      errors :+= exceptionFactory.syntaxException(
        nodeType.toString ++ " does not allow relationship patterns",
        inputPosition(relPattern.getStart)
      )
    }
  }

  private def errorOnNodesAndRelConstraints[S <: ParserRuleContext, T <: ParserRuleContext](
    nodePattern: S,
    constraint: T,
    containsKey: T => Boolean
  ): Unit = {
    if (nodePattern != null && constraint != null) {
      val relType: ConstraintType = if (containsKey(constraint)) {
        ConstraintType.REL_KEY
      } else {
        ConstraintType.REL_UNIQUE
      }

      errors :+= exceptionFactory.syntaxException(
        relType.toString ++ " does not allow node patterns",
        inputPosition(nodePattern.getStart)
      )
    }
  }

  private def checkSubqueryInTransactionsParameters(ctx: CypherParser.SubqueryInTransactionsParametersContext): Unit = {
    errorOnDuplicatedRule(ctx.subqueryInTransactionsBatchParameters(), "OF ROWS")
    errorOnDuplicatedRule(ctx.subqueryInTransactionsErrorParameters(), "ON ERROR")
    errorOnDuplicatedRule(ctx.subqueryInTransactionsReportParameters(), "ON ERROR")
  }

  private def checkCreateAlias(ctx: CypherParser.CreateAliasContext): Unit = {
    errorOnAliasNameContainingDots(ctx.symbolicAliasNameOrParameter())
  }

  private def checkAlterAlias(ctx: CypherParser.AlterAliasContext): Unit = {
    errorOnAliasNameContainingDots(ctx.symbolicAliasNameOrParameter())
    errorOnDuplicated(ctx.DRIVER(), "DRIVER")
    errorOnDuplicated(ctx.AT(), "AT")
    errorOnDuplicated(ctx.USER(), "USER")
    errorOnDuplicated(ctx.PASSWORD(), "PASSWORD")
    errorOnDuplicated(ctx.PROPERTIES(), "PROPERTIES")
    errorOnDuplicated(ctx.TARGET(), "TARGET")
  }

  private def checkCreateUser(ctx: CypherParser.CreateUserContext): Unit = {
    errorOnDuplicatedRule(ctx.passwordChangeRequired(), "SET PASSWORD CHANGE [NOT] REQUIRED")
    errorOnDuplicatedRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicatedRule(ctx.homeDatabase(), "SET HOME DATABASE")
  }

  private def checkAlterUser(ctx: CypherParser.AlterUserContext): Unit = {
    errorOnDuplicatedRule(ctx.setPassword(), "SET PASSWORD")
    errorOnDuplicatedRule(ctx.passwordChangeRequired(), "SET PASSWORD CHANGE [NOT] REQUIRED")
    errorOnDuplicatedRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicatedRule(ctx.homeDatabase(), "SET HOME DATABASE")
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
    errorOnNodesAndRelConstraints[ConstraintNodePatternContext, CreateConstraintRelCheckContext](
      ctx.constraintNodePattern(),
      ctx.createConstraintRelCheck(),
      _.KEY() != null
    )

    errorOnRelationshipAndNodeConstraints[ConstraintRelPatternContext, CreateConstraintNodeCheckContext](
      ctx.constraintRelPattern(),
      ctx.createConstraintNodeCheck(),
      _.KEY() != null
    )

    if (ctx.propertyList() != null && ctx.propertyList().property().size() > 1) {
      val secondProperty = ctx.propertyList().property(1).start

      if (ctx.NULL() != null) {
        errors :+= exceptionFactory.syntaxException(
          "Constraint type 'IS NOT NULL' does not allow multiple properties",
          inputPosition(secondProperty)
        )
      } else if (ctx.TYPED() != null || ctx.COLONCOLON() != null) {
        errors :+= exceptionFactory.syntaxException(
          "Constraint type 'IS TYPED' does not allow multiple properties",
          inputPosition(secondProperty)
        )
      } else if (ctx.EXISTS().size() == 2 || ctx.EXISTS().size() == 1 && ctx.IF() == null) {
        errors :+= exceptionFactory.syntaxException(
          "Constraint type 'EXISTS' does not allow multiple properties",
          inputPosition(secondProperty)
        )
      }
    }
  }

  private def checkDropConstraint(ctx: CypherParser.DropConstraintContext): Unit = {
    errorOnRelationshipAndNodeConstraints[ConstraintRelPatternContext, DropConstraintNodeCheckContext](
      ctx.constraintRelPattern(),
      ctx.dropConstraintNodeCheck(),
      _.KEY() != null
    )

    if (ctx.NULL() != null) {
      errors :+= exceptionFactory.syntaxException(
        "Unsupported drop constraint command: Please delete the constraint by name instead",
        inputPosition(ctx.start)
      )
    }
  }

  private def checkCreateDatabase(ctx: CypherParser.CreateDatabaseContext): Unit = {
    val primaries =
      (ctx.PRIMARY().asScala ++ ctx.PRIMARIES().asScala).sortBy(_.getSymbol.getStartIndex)
    val secondaries =
      (ctx.SECONDARY().asScala ++ ctx.SECONDARIES().asScala).sortBy(_.getSymbol.getStartIndex)

    errorOnDuplicated(primaries.asJava, "PRIMARY")
    errorOnDuplicated(secondaries.asJava, "SECONDARY")
  }

  private def checkAlterDatabase(ctx: CypherParser.AlterDatabaseContext): Unit = {
    val primaries =
      (ctx.PRIMARY().asScala ++ ctx.PRIMARIES().asScala).sortBy(_.getSymbol.getStartIndex)
    val secondaries =
      (ctx.SECONDARY().asScala ++ ctx.SECONDARIES().asScala).sortBy(_.getSymbol.getStartIndex)

    errorOnDuplicated(primaries.asJava, "PRIMARY")
    errorOnDuplicated(secondaries.asJava, "SECONDARY")
    errorOnDuplicated(ctx.REMOVE(), "REMOVE")
    errorOnDuplicated(ctx.ACCESS(), "ACCESS")
    errorOnDuplicated(ctx.TOPOLOGY(), "TOPOLOGY")
    errorOnDuplicated(ctx.OPTION(), "OPTION")
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
    val lookupIndexFunctionName = ctx.lookupIndexFunctionName()

    if (lookupIndexFunctionName != null) {
      val functionName = lookupIndexFunctionName.symbolicNameString()
      /* This should not be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH(x)

         This should be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH EACH(x)
       */
      if (functionName.getText.toUpperCase() == "EACH" && ctx.EACH() == null) {
        errors :+= exceptionFactory.syntaxException(
          "Missing function name for the LOOKUP INDEX",
          inputPosition(functionName.start)
        )
      }
    }
  }

  private def checkInsertPattern(ctx: CypherParser.InsertPatternContext): Unit = {
    val firstEquality = ctx.children.asScala.collectFirst {
      case x: TerminalNode if x.getText.equals("=") => x.getSymbol
    }

    if (firstEquality.nonEmpty) {
      errors :+= exceptionFactory.syntaxException(
        "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name.",
        inputPosition(firstEquality.get)
      )
    }
  }

  private def checkInsertLabelConjunction(ctx: CypherParser.InsertLabelConjunctionContext): Unit = {
    val firstColon = ctx.children.asScala.collectFirst {
      case x: TerminalNode if x.getText.equals(":") => x.getSymbol
    }

    if (firstColon.nonEmpty) {
      errors :+= exceptionFactory.syntaxException(
        "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.",
        inputPosition(firstColon.get)
      )
    }
  }
}
