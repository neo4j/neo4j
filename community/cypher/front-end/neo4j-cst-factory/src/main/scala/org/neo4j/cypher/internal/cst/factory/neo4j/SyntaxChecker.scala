/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.cst.factory.neo4j

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintNodePatternContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintRelPatternContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateConstraintNodeCheckContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateConstraintRelCheckContext
import org.neo4j.cypher.internal.parser.CypherParser.DropConstraintNodeCheckContext
import org.neo4j.cypher.internal.parser.CypherParser.GlobContext
import org.neo4j.cypher.internal.parser.CypherParser.GlobRecursiveContext
import org.neo4j.cypher.internal.parser.CypherParser.SymbolicAliasNameContext
import org.neo4j.cypher.internal.parser.CypherParserBaseListener
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

class SyntaxChecker extends CypherParserBaseListener {
  private val exceptionFactory = new OpenCypherExceptionFactory(None)
  private var errors: mutable.Seq[Exception] = mutable.Seq.empty

  def getErrors: Iterator[Exception] = {
    errors.iterator
  }

  private def errorOnDuplicated(
    token: Token,
    paramDescription: String
  ): Unit = {
    errors :+= exceptionFactory.syntaxException(
      s"Duplicated $paramDescription parameters",
      new InputPosition(
        token.getStartIndex,
        token.getLine,
        token.getCharPositionInLine
      )
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

  private def errorOnAliasNameContainingDots(aliasesNames: java.util.List[SymbolicAliasNameContext]): Unit = {
    if (aliasesNames.size() > 0) {
      val aliasName = aliasesNames.get(0)
      if (aliasName.symbolicNameString().size() > 2) {
        val start = aliasName.symbolicNameString().get(0).getStart
        errors :+= exceptionFactory.syntaxException(
          s"'.' is not a valid character in the remote alias name '${aliasName.symbolicNameString().asScala.map(_.getText).mkString}'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`.",
          new InputPosition(start.getStartIndex, start.getLine, start.getCharPositionInLine)
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
        new InputPosition(
          relPattern.getStart.getStartIndex,
          relPattern.getStart.getLine,
          relPattern.getStart.getCharPositionInLine
        )
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
        new InputPosition(
          nodePattern.getStart.getStartIndex,
          nodePattern.getStart.getLine,
          nodePattern.getStart.getCharPositionInLine
        )
      )
    }
  }

  override def exitSubqueryInTransactionsParameters(ctx: CypherParser.SubqueryInTransactionsParametersContext): Unit = {
    errorOnDuplicatedRule(ctx.subqueryInTransactionsBatchParameters(), "OF ROWS")
    errorOnDuplicatedRule(ctx.subqueryInTransactionsErrorParameters(), "ON ERROR")
    errorOnDuplicatedRule(ctx.subqueryInTransactionsReportParameters(), "ON ERROR")
  }

  override def exitCreateAlias(ctx: CypherParser.CreateAliasContext): Unit = {
    errorOnAliasNameContainingDots(ctx.symbolicAliasName())
  }

  override def exitAlterAlias(ctx: CypherParser.AlterAliasContext): Unit = {
    errorOnAliasNameContainingDots(ctx.symbolicAliasName())
    errorOnDuplicated(ctx.DRIVER(), "DRIVER")
    errorOnDuplicated(ctx.AT(), "AT")
    errorOnDuplicated(ctx.USER(), "USER")
    errorOnDuplicated(ctx.PASSWORD(), "PASSWORD")
    errorOnDuplicated(ctx.PROPERTIES(), "PROPERTIES")
    errorOnDuplicated(ctx.TARGET(), "TARGET")
  }

  override def exitCreateUser(ctx: CypherParser.CreateUserContext): Unit = {
    errorOnDuplicatedRule(ctx.passwordChangeRequired(), "SET PASSWORD CHANGE [NOT] REQUIRED")
    errorOnDuplicatedRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicatedRule(ctx.homeDatabase(), "SET HOME DATABASE")
  }

  override def exitAlterUser(ctx: CypherParser.AlterUserContext): Unit = {
    errorOnDuplicatedRule(ctx.setPassword(), "SET PASSWORD")
    errorOnDuplicatedRule(ctx.passwordChangeRequired(), "SET PASSWORD CHANGE [NOT] REQUIRED")
    errorOnDuplicatedRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicatedRule(ctx.homeDatabase(), "SET HOME DATABASE")
  }

  override def exitAllPrivilege(ctx: CypherParser.AllPrivilegeContext): Unit = {
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
              new InputPosition(
                symbol.getStartIndex,
                symbol.getLine,
                symbol.getCharPositionInLine
              )
            )
          }
        case _ =>
      }
    }
  }

  override def exitGlobPart(ctx: CypherParser.GlobPartContext): Unit = {
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
          new InputPosition(ctx.start.getStartIndex, ctx.getStart.getLine, ctx.getStart.getCharPositionInLine)
        )
      }
    }
  }

  override def exitCreateConstraint(ctx: CypherParser.CreateConstraintContext): Unit = {
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

    if (ctx.propertyList().property().size() > 1) {
      val secondProperty = ctx.propertyList().property(1).start

      if (ctx.NULL() != null) {
        errors :+= exceptionFactory.syntaxException(
          "Constraint type 'IS NOT NULL' does not allow multiple properties",
          new InputPosition(secondProperty.getStartIndex, secondProperty.getLine, secondProperty.getCharPositionInLine)
        )
      } else if (ctx.EXISTS().size() == 2 || ctx.EXISTS().size() == 1 && ctx.IF() == null) {
        errors :+= exceptionFactory.syntaxException(
          "Constraint type 'EXISTS' does not allow multiple properties",
          new InputPosition(secondProperty.getStartIndex, secondProperty.getLine, secondProperty.getCharPositionInLine)
        )
      }
    }
  }

  override def exitDropConstraint(ctx: CypherParser.DropConstraintContext): Unit = {
    errorOnRelationshipAndNodeConstraints[ConstraintRelPatternContext, DropConstraintNodeCheckContext](
      ctx.constraintRelPattern(),
      ctx.dropConstraintNodeCheck(),
      _.KEY() != null
    )

    if (ctx.NULL() != null) {
      errors :+= exceptionFactory.syntaxException(
        "Unsupported drop constraint command: Please delete the constraint by name instead",
        new InputPosition(ctx.start.getStartIndex, ctx.start.getLine, ctx.start.getCharPositionInLine)
      )
    }
  }

  override def exitCreateDatabase(ctx: CypherParser.CreateDatabaseContext): Unit = {
    val primaries =
      (ctx.PRIMARY().asScala ++ ctx.PRIMARIES().asScala).sortBy(_.getSymbol.getStartIndex)
    val secondaries =
      (ctx.SECONDARY().asScala ++ ctx.SECONDARIES().asScala).sortBy(_.getSymbol.getStartIndex)

    errorOnDuplicated(primaries.asJava, "PRIMARY")
    errorOnDuplicated(secondaries.asJava, "SECONDARY")
  }

  override def exitAlterDatabase(ctx: CypherParser.AlterDatabaseContext): Unit = {
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

  override def exitPeriodicCommitQueryHintFailure(ctx: CypherParser.PeriodicCommitQueryHintFailureContext): Unit = {
    errors :+= exceptionFactory.syntaxException(
      "The PERIODIC COMMIT query hint is no longer supported. Please use CALL { ... } IN TRANSACTIONS instead.",
      new InputPosition(ctx.start.getStartIndex, ctx.start.getLine, ctx.start.getCharPositionInLine)
    )
  }

  override def exitCreateCommand(ctx: CypherParser.CreateCommandContext): Unit = {
    val createIndex = ctx.createIndex()
    val replace = ctx.REPLACE()

    if (createIndex != null && replace != null) {
      if (createIndex.oldCreateIndex() != null) {
        errors :+= exceptionFactory.syntaxException(
          "'REPLACE' is not allowed for this index syntax",
          new InputPosition(
            replace.getSymbol.getStartIndex,
            replace.getSymbol.getLine,
            replace.getSymbol.getCharPositionInLine
          )
        )
      }
    }
  }

  override def exitCreateLookupIndex(ctx: CypherParser.CreateLookupIndexContext): Unit = {
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
          new InputPosition(
            functionName.start.getStartIndex,
            functionName.start.getLine,
            functionName.start.getCharPositionInLine
          )
        )
      }
    }
  }
}
