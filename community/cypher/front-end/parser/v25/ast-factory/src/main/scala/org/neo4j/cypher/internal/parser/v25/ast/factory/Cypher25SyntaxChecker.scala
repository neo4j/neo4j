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
package org.neo4j.cypher.internal.parser.v25.ast.factory

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AuthRuleCondition
import org.neo4j.cypher.internal.ast.AuthRuleEnabled
import org.neo4j.cypher.internal.ast.AuthRuleSetClause
import org.neo4j.cypher.internal.ast.PropertyType
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.EnableParsingOfObfuscatedLiterals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.SyntaxChecker
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.cast
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.common.ast.factory.ConstraintType
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintIsNotNullContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintIsUniqueContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintKeyContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintTypedContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.DropConstraintContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.GlobContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.GlobRecursiveContext
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.gqlstatus.GqlHelper

import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

final class Cypher25SyntaxChecker(
  exceptionFactory: CypherExceptionFactory,
  semanticFeatures: Seq[SemanticFeature]
) extends SyntaxChecker {
  private[this] var _errors: Seq[Exception] = Seq.empty

  override def errors: Seq[Throwable] = _errors

  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}

  override def exitEveryRule(ctx: ParserRuleContext): Unit = {
    // Note, this has been shown to be significantly faster than using the generated listener.
    // Compiles into a lookupswitch (or possibly tableswitch)
    ctx.getRuleIndex match {
      case Cypher25Parser.RULE_subqueryInTransactionsParameters => checkSubqueryInTransactionsParameters(cast(ctx))
      case Cypher25Parser.RULE_createConstraint                 => checkCreateConstraint(cast(ctx))
      case Cypher25Parser.RULE_enclosedPropertyList             => checkEnclosedPropertyList(cast(ctx))
      case Cypher25Parser.RULE_createLookupIndex                => checkCreateLookupIndex(cast(ctx))
      case Cypher25Parser.RULE_createUser                       => checkCreateUser(cast(ctx))
      case Cypher25Parser.RULE_alterUser                        => checkAlterUser(cast(ctx))
      case Cypher25Parser.RULE_alterUsers                       => checkAlterUsers(cast(ctx))
      case Cypher25Parser.RULE_allPrivilege                     => checkAllPrivilege(cast(ctx))
      case Cypher25Parser.RULE_topology                         => checkTopology(cast(ctx))
      case Cypher25Parser.RULE_alterDatabase                    => checkAlterDatabase(cast(ctx))
      case Cypher25Parser.RULE_alterDatabaseTopology            => checkAlterDatabaseTopology(cast(ctx))
      case Cypher25Parser.RULE_alterAlias                       => checkAlterAlias(cast(ctx))
      case Cypher25Parser.RULE_globPart                         => checkGlobPart(cast(ctx))
      case Cypher25Parser.RULE_insertPattern                    => checkInsertPattern(cast(ctx))
      case Cypher25Parser.RULE_insertNodeLabelExpression        => checkInsertLabelConjunction(cast(ctx))
      case Cypher25Parser.RULE_functionInvocation               => checkFunctionInvocation(cast(ctx))
      case Cypher25Parser.RULE_typePart                         => checkTypePart(cast(ctx))
      case Cypher25Parser.RULE_symbolicAliasName                => checkSymbolicAliasName(cast(ctx))
      case Cypher25Parser.RULE_defaultLanguageSpecification     => checkDefaultLanguageSpecification(cast(ctx))
      case Cypher25Parser.RULE_propertyTypeList                 => checkPropertyTypeList(cast(ctx))
      case Cypher25Parser.RULE_nodeTypeSpecification            => checkNodeTypeSpecification(cast(ctx))
      case Cypher25Parser.RULE_edgeTypeSpecification            => checkEdgeTypeSpecification(cast(ctx))
      case Cypher25Parser.RULE_constraintSpecification          => checkConstraintSpecification(cast(ctx))
      case Cypher25Parser.RULE_nodeTypeInlineConstraintList     => checkNodeTypeInlineConstraintList(cast(ctx))
      case Cypher25Parser.RULE_edgeTypeInlineConstraintList     => checkEdgeTypeInlineConstraintList(cast(ctx))
      case Cypher25Parser.RULE_obfuscatedLiteral                => failOnObfuscatedLiteral(ctx)
      case Cypher25Parser.RULE_createAuthRule                   => checkCreateAuthRule(cast(ctx))
      case Cypher25Parser.RULE_alterAuthRule                    => checkAlterAuthRule(cast(ctx))
      case _                                                    =>
    }
  }

  override def check(ctx: ParserRuleContext): Boolean = {
    exitEveryRule(ctx)
    _errors.isEmpty
  }

  private def inputPosition(symbol: Token): InputPosition = {
    InputPosition(symbol.getStartIndex, symbol.getLine, symbol.getCharPositionInLine + 1)
  }

  private def errorOnDuplicate(token: Token, description: String, isParam: Boolean): Unit =
    if (isParam) {
      _errors :+= exceptionFactory.duplicateClauseParameter(description, inputPosition(token))
    } else {
      _errors :+= exceptionFactory.duplicateClause(description, inputPosition(token))
    }

  private def errorOnDuplicateCtx[T <: AstRuleCtx](
    ctx: java.util.List[T],
    description: String,
    isParam: Boolean = false
  ): Unit = if (ctx.size > 1) errorOnDuplicate(nodeChild(ctx.get(1), 0).getSymbol, description, isParam)

  private def errorOnDuplicateRule[T <: ParserRuleContext](
    params: java.util.List[T],
    description: String,
    isParam: Boolean = false
  ): Unit = if (params.size() > 1) errorOnDuplicate(params.get(1).start, description, isParam)

  private def checkSubqueryInTransactionsParameters(
    ctx: Cypher25Parser.SubqueryInTransactionsParametersContext
  ): Unit = {
    errorOnDuplicateRule(ctx.subqueryInTransactionsBatchParameters(), "OF ROWS", isParam = true)
    errorOnDuplicateRule(ctx.subqueryInTransactionsErrorParameters(), "ON ERROR", isParam = true)
    errorOnDuplicateRule(ctx.subqueryInTransactionsReportParameters(), "REPORT STATUS", isParam = true)
    errorOnDuplicateRule(ctx.subqueryInTransactionsDisjointByParameters(), "DISJOINT BY", isParam = true)
  }

  private def checkAlterAlias(ctx: Cypher25Parser.AlterAliasContext): Unit = {
    val aliasTargets = ctx.alterAliasTarget()
    val usernames = ctx.alterAliasUser()
    val passwords = ctx.alterAliasPassword()
    val driverSettings = ctx.alterAliasDriver()
    val defaultLanguages = ctx.defaultLanguageSpecification()

    // Should only be checked in case of remote

    errorOnDuplicateCtx(driverSettings, "DRIVER")
    errorOnDuplicateCtx(usernames, "USER")
    errorOnDuplicateCtx(passwords, "PASSWORD")
    errorOnDuplicateCtx(ctx.alterAliasProperties(), "PROPERTIES")
    errorOnDuplicateCtx(aliasTargets, "TARGET")
    errorOnDuplicateCtx(defaultLanguages, "DEFAULT LANGUAGE")
  }

  private def checkSymbolicAliasName(ctx: Cypher25Parser.SymbolicAliasNameContext): Unit = {
    val nameComponents = ctx.symbolicNameString().asScala.toList
    if (nameComponents.size > 1 && nameComponents.exists(s => s.escapedSymbolicNameString() != null)) {
      // Disallow `foo`.`bar`, `foo`.bar, foo.`bar`, etc.
      _errors :+= exceptionFactory.invalidGraphReferenceFormat(
        nameComponents.map(_.getText).mkString("."),
        inputPosition(nameComponents.head.start)
      )
    }
  }

  private def checkCreateUser(ctx: Cypher25Parser.CreateUserContext): Unit = {
    errorOnDuplicateRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicateRule(ctx.homeDatabase(), "SET HOME DATABASE")
    errorOnDuplicateRule(ctx.userSetTagsClause(), "SET TAGS")
  }

  private def checkAlterUser(ctx: Cypher25Parser.AlterUserContext): Unit = {
    errorOnDuplicateRule(ctx.userStatus(), "SET STATUS {SUSPENDED|ACTIVE}")
    errorOnDuplicateRule(ctx.homeDatabase(), "SET HOME DATABASE")
    errorOnDuplicateRule(ctx.userSetTagsClause(), "SET TAGS")
    errorOnDuplicateRule(ctx.userAddTagsClause(), "ADD TAGS")
    errorOnDuplicateRule(ctx.userRemoveTagsClause(), "REMOVE TAGS")
  }

  private def checkAlterUsers(ctx: Cypher25Parser.AlterUsersContext): Unit = {
    errorOnDuplicateRule(ctx.userSetTagsClause(), "SET TAGS")
    errorOnDuplicateRule(ctx.userAddTagsClause(), "ADD TAGS")
    errorOnDuplicateRule(ctx.userRemoveTagsClause(), "REMOVE TAGS")
  }

  private def checkCreateAuthRule(ctx: Cypher25Parser.CreateAuthRuleContext): Unit = {
    checkAuthRuleSetClauses(ctx.authRuleSetClause())
  }

  private def checkAlterAuthRule(ctx: Cypher25Parser.AlterAuthRuleContext): Unit = {
    checkAuthRuleSetClauses(ctx.authRuleSetClause())
  }

  private def checkAuthRuleSetClauses(clauses: util.List[Cypher25Parser.AuthRuleSetClauseContext]): Unit = {
    val (setConditions, setEnableds) = clauses.asScala.toList
      .partition(_.ast[AuthRuleSetClause] match {
        case _: AuthRuleCondition => true
        case _: AuthRuleEnabled   => false
      })

    errorOnDuplicateRule(setConditions.asJava, "SET CONDITION")
    errorOnDuplicateRule(setEnableds.asJava, "SET ENABLED")
  }

  private def checkAllPrivilege(ctx: Cypher25Parser.AllPrivilegeContext): Unit = {
    val privilegeType = ctx.allPrivilegeType()
    val privilegeTarget = ctx.allPrivilegeTarget()

    if (privilegeType != null) {
      val privilege =
        if (privilegeType.GRAPH() != null) Some("GRAPH")
        else if (privilegeType.DBMS() != null) Some("DBMS")
        else if (privilegeType.DATABASE() != null) Some("DATABASE")
        else None

      val target = privilegeTarget match {
        case c: Cypher25Parser.DefaultTargetContext =>
          privilege match {
            case Some("DBMS") => ("HOME", c.HOME().getSymbol)
            case _ =>
              if (c.GRAPH() != null) ("GRAPH", c.GRAPH().getSymbol)
              else ("DATABASE", c.DATABASE().getSymbol)
          }
        case c: Cypher25Parser.DatabaseVariableTargetContext =>
          if (c.DATABASE() != null) ("DATABASE", c.DATABASE().getSymbol)
          else ("DATABASES", c.DATABASES().getSymbol)
        case c: Cypher25Parser.GraphVariableTargetContext =>
          if (c.GRAPH() != null) ("GRAPH", c.GRAPH().getSymbol)
          else ("GRAPHS", c.GRAPHS().getSymbol)
        case c: Cypher25Parser.DBMSTargetContext =>
          ("DBMS", c.DBMS().getSymbol)
        case _ => throw exceptionFactory.internalError("Unexpected privilege all command", pos(ctx))
      }
      (privilege, target) match {
        case (Some(privilege), (target, symbol)) =>
          // This makes GRANT ALL DATABASE PRIVILEGES ON DATABASES * work
          if (!target.startsWith(privilege)) {
            _errors :+= exceptionFactory.invalidInputException(
              target,
              List(privilege),
              s"Invalid input '$target': expected \"$privilege\"",
              inputPosition(symbol)
            )
          }
        case _ =>
      }
    }
  }

  private def checkGlobPart(ctx: Cypher25Parser.GlobPartContext): Unit =
    if (ctx.DOT() == null) {
      ctx.parent.parent match {
        case r: GlobRecursiveContext if r.globPart().escapedSymbolicNameString() != null =>
          _errors :+= exceptionFactory.invalidGlobEscaping(inputPosition(ctx.start))

        case r: GlobContext if r.escapedSymbolicNameString() != null =>
          _errors :+= exceptionFactory.invalidGlobEscaping(inputPosition(ctx.start))

        case _ =>
      }
    }

  private def checkCreateConstraint(ctx: Cypher25Parser.CreateConstraintContext): Unit = {
    checkConstraintTypeMatchesElementType(ctx.constraintType(), ctx.commandNodePattern(), ctx.commandRelPattern())
  }

  private def checkEnclosedPropertyList(ctx: Cypher25Parser.EnclosedPropertyListContext): Unit = {
    if (ctx.property().size() > 1 && ctx.getParent != null) {
      val secondProperty = ctx.property(1).start
      ctx.getParent.getParent match {
        case _: ConstraintTypedContext =>
          _errors :+= exceptionFactory.unsupportedMultiplePropertiesInConstraint(
            "IS TYPED",
            inputPosition(secondProperty)
          )
        case _: ConstraintIsNotNullContext =>
          _errors :+= exceptionFactory.unsupportedMultiplePropertiesInConstraint(
            "IS NOT NULL",
            inputPosition(secondProperty)
          )
        case dropCtx: DropConstraintContext if dropCtx.EXISTS() != null =>
          _errors :+= exceptionFactory.unsupportedMultiplePropertiesInConstraint(
            "EXISTS",
            inputPosition(secondProperty)
          )
        case _ =>
      }
    }
  }

  private def checkTopology(ctx: Cypher25Parser.TopologyContext): Unit = {
    errorOnDuplicateRule[Cypher25Parser.PrimaryTopologyContext](ctx.primaryTopology(), "PRIMARY")
    errorOnDuplicateRule[Cypher25Parser.SecondaryTopologyContext](ctx.secondaryTopology(), "SECONDARY")
  }

  private def checkAlterDatabase(ctx: Cypher25Parser.AlterDatabaseContext): Unit = {
    if (!ctx.REMOVE().isEmpty) {
      val keyNames = astSeq[String](ctx.symbolicNameString())
      val keySet = scala.collection.mutable.Set.empty[String]
      var i = 0
      keyNames.foreach(k =>
        if (keySet.contains(k)) {
          _errors :+= exceptionFactory.duplicateClause(s"'REMOVE OPTION $k'", pos(ctx.symbolicNameString(i)))
        } else {
          keySet.addOne(k)
          i += 1
        }
      )
    }

    if (!ctx.alterDatabaseOption().isEmpty) {
      val optionCtxs = astSeq[Map[String, Expression]](ctx.alterDatabaseOption())
      val keyNames = optionCtxs.flatMap(m => if (m != null) m.keys else Seq.empty)
      val keySet = mutable.Set.empty[String]
      var i = 0
      keyNames.foreach(k =>
        if (keySet.contains(k)) {
          _errors :+= exceptionFactory.duplicateClause(s"'SET OPTION $k'", pos(ctx.alterDatabaseOption(i)))
        } else {
          keySet.addOne(k)
          i += 1
        }
      )
    }

    errorOnDuplicateCtx(ctx.alterDatabaseAccess(), "ACCESS")
    errorOnDuplicateCtx(ctx.alterDatabaseTopology(), "TOPOLOGY")
    errorOnDuplicateCtx(ctx.alterReplicaTopology(), "TOPOLOGY")
    if (ctx.alterDatabaseTopology().size() == 1 && ctx.alterReplicaTopology().size() == 1) {
      val topologyPos = inputPosition(nodeChild(ctx.alterDatabaseTopology().get(0), 0).getSymbol)
      val replicaPos = inputPosition(nodeChild(ctx.alterReplicaTopology().get(0), 0).getSymbol)
      if (topologyPos.offset < replicaPos.offset) {
        _errors :+= exceptionFactory.duplicateClause("TOPOLOGY", replicaPos)
      } else {
        _errors :+= exceptionFactory.duplicateClause("TOPOLOGY", topologyPos)
      }
    }
    errorOnDuplicateCtx(ctx.alterGraphShard(), "GRAPH SHARD")
    errorOnDuplicateCtx(ctx.alterPropertyShards(), "PROPERTY SHARD")
  }

  private def checkAlterDatabaseTopology(ctx: Cypher25Parser.AlterDatabaseTopologyContext): Unit = {
    errorOnDuplicateRule[Cypher25Parser.PrimaryTopologyContext](ctx.primaryTopology(), "PRIMARY")
    errorOnDuplicateRule[Cypher25Parser.SecondaryTopologyContext](ctx.secondaryTopology(), "SECONDARY")
  }

  private def checkCreateLookupIndex(ctx: Cypher25Parser.CreateLookupIndexContext): Unit = {
    val functionName = ctx.symbolicNameString()
    /* This should not be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH(x)

         This should be valid:
         CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH EACH(x)
     */
    val relPattern = ctx.lookupIndexRelPattern()
    if (functionName.getText.toUpperCase() == "EACH" && relPattern != null && relPattern.EACH() == null)
      _errors :+= exceptionFactory.missingLookupIndexFunctionName(inputPosition(ctx.LPAREN().getSymbol))
  }

  private def checkInsertPattern(ctx: Cypher25Parser.InsertPatternContext): Unit =
    if (ctx.EQ() != null)
      _errors :+= exceptionFactory.invalidUseOfInsert(
        "Named patterns are",
        "remove the name",
        "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name.",
        pos(ctxChild(ctx, 0))
      )

  private def checkInsertLabelConjunction(ctx: Cypher25Parser.InsertNodeLabelExpressionContext): Unit = {
    val colons = ctx.COLON()
    val firstIsColon = nodeChild(ctx, 0).getSymbol.getType == Cypher25Parser.COLON

    if (firstIsColon && colons.size > 1) {
      _errors :+= exceptionFactory.invalidUseOfInsert(
        "Colon `:` conjunction is",
        "conjunction with ampersand `&` instead",
        "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.",
        inputPosition(colons.get(1).getSymbol)
      )
    } else if (!firstIsColon && colons.size() > 0) {
      _errors :+= exceptionFactory.invalidUseOfInsert(
        "Colon `:` conjunction is",
        "conjunction with ampersand `&` instead",
        "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.",
        inputPosition(colons.get(0).getSymbol)
      )
    }
  }

  private def checkFunctionInvocation(ctx: Cypher25Parser.FunctionInvocationContext): Unit = {
    val functionName = ctx.functionName().ast[FunctionName]()
    if (
      functionName.name.toLowerCase == "normalize" &&
      functionName.namespace.parts.isEmpty &&
      ctx.functionArgument().size == 2
    ) {
      val normalForm = ctx.functionArgument(1).expression().ast[Expression]()
      _errors :+= exceptionFactory.invalidNormalForm(normalForm)
    } else if (
      functionName.name.toLowerCase == "vector" &&
      functionName.namespace.parts.isEmpty &&
      ctx.functionArgument().size == 3
    ) {
      val coordinateType = ctx.functionArgument(2).expression().ast[Expression]()
      _errors :+= exceptionFactory.invalidVectorType(coordinateType)
    } else if (
      functionName.name.toLowerCase == "vector_distance" &&
      functionName.namespace.parts.isEmpty &&
      ctx.functionArgument().size == 3
    ) {
      val distanceMetric = ctx.functionArgument(2).expression().ast[Expression]()
      _errors :+= exceptionFactory.invalidVectorDistanceMetric(distanceMetric, normFunction = false)
    } else if (
      functionName.name.toLowerCase == "vector_norm" &&
      functionName.namespace.parts.isEmpty &&
      ctx.functionArgument().size == 2
    ) {
      val distanceMetric = ctx.functionArgument(1).expression().ast[Expression]()
      _errors :+= exceptionFactory.invalidVectorDistanceMetric(distanceMetric, normFunction = true)
    }
  }

  private def checkTypePart(ctx: Cypher25Parser.TypePartContext): Unit = {
    val cypherType = ctx.typeName().ast
    if (cypherType.isInstanceOf[ClosedDynamicUnionType] && ctx.typeNullability() != null) {
      _errors :+= exceptionFactory.invalidNotNullClosedDynamicUnion(pos(ctx.typeNullability()))
    }
  }

  private def checkDefaultLanguageSpecification(ctx: Cypher25Parser.DefaultLanguageSpecificationContext): Unit = {
    val versionNumberStr = ctx.UNSIGNED_DECIMAL_INTEGER().getText
    CypherVersion.values().find(v => v.versionName.equals(versionNumberStr)) match {
      case Some(_) =>
      case None => _errors :+= exceptionFactory.invalidInputException(
          versionNumberStr,
          "Cypher version",
          CypherVersion.values().map(_.description).toList,
          s"Invalid Cypher version '$versionNumberStr'. Valid Cypher versions are: ${CypherVersion.values().map(_.versionName).mkString(", ")}",
          pos(ctx.UNSIGNED_DECIMAL_INTEGER())
        )
    }
  }

  private def checkPropertyTypeList(ctx: Cypher25Parser.PropertyTypeListContext): Unit = {
    // check for duplicates
    astSeq[PropertyType](ctx.propertyType()).groupBy(_.name).values.find(_.size > 1).foreach(prop =>
      _errors :+= exceptionFactory.duplicatePropertyTypeInGraphTypeElement(
        prop.head.name.name,
        s"duplicate property key `${prop.head.name.name}`",
        prop(1).position
      )
    )
  }

  private def checkNodeTypeSpecification(ctx: Cypher25Parser.NodeTypeSpecificationContext): Unit = {
    // Node level constraint
    if (ctx.nodeTypeInlineConstraintList() != null) {
      ctx.nodeTypeInlineConstraintList().constraintType().forEach {
        case c: ConstraintIsUniqueContext if c.RELATIONSHIP() != null || c.REL() != null =>
          _errors :+= exceptionFactory.invalidInputException(
            ConstraintType.REL_UNIQUE.description(),
            "node element type",
            List(ConstraintType.NODE_UNIQUE.description()),
            s"node element type does not allow '${ConstraintType.REL_UNIQUE.description()}'",
            inputPosition(Option(c.RELATIONSHIP()).getOrElse(c.REL()).getSymbol)
          )
        case c: ConstraintKeyContext if c.RELATIONSHIP() != null || c.REL() != null =>
          _errors :+= exceptionFactory.invalidInputException(
            ConstraintType.REL_KEY.description(),
            "node element type",
            List(ConstraintType.NODE_KEY.description()),
            s"node element type does not allow '${ConstraintType.REL_KEY.description()}'",
            inputPosition(Option(c.RELATIONSHIP()).getOrElse(c.REL()).getSymbol)
          )
        case _ => ()
      }
    }
    // Property level constraint
    if (ctx.propertyTypeList() != null) {
      ctx.propertyTypeList().propertyType().asScala.collect {
        case c
          if c.propertyTypeInlineConstraint() != null && (c.propertyTypeInlineConstraint().REL() != null || c.propertyTypeInlineConstraint().RELATIONSHIP() != null) =>
          if (c.propertyTypeInlineConstraint().UNIQUE() != null) {
            _errors :+= exceptionFactory.invalidInputException(
              ConstraintType.REL_UNIQUE.description(),
              "node element type property",
              List(ConstraintType.NODE_UNIQUE.description()),
              s"node element type property does not allow '${ConstraintType.REL_UNIQUE.description()}'",
              inputPosition(Option(
                c.propertyTypeInlineConstraint().RELATIONSHIP()
              ).getOrElse(c.propertyTypeInlineConstraint().REL()).getSymbol)
            )
          } else if (c.propertyTypeInlineConstraint().KEY() != null) {
            _errors :+= exceptionFactory.invalidInputException(
              ConstraintType.REL_KEY.description(),
              "node element type property",
              List(ConstraintType.NODE_KEY.description()),
              s"node element type property does not allow '${ConstraintType.REL_KEY.description()}'",
              inputPosition(Option(
                c.propertyTypeInlineConstraint().RELATIONSHIP()
              ).getOrElse(c.propertyTypeInlineConstraint().REL()).getSymbol)
            )
          }
      }
    }
  }

  private def checkEdgeTypeSpecification(ctx: Cypher25Parser.EdgeTypeSpecificationContext): Unit = {
    // Edge level constraint
    if (ctx.edgeTypeInlineConstraintList() != null) {
      ctx.edgeTypeInlineConstraintList().constraintType().forEach {
        case c: ConstraintIsUniqueContext if c.NODE() != null =>
          _errors :+= exceptionFactory.invalidInputException(
            ConstraintType.NODE_UNIQUE.description(),
            "edge element type",
            List(ConstraintType.REL_UNIQUE.description()),
            s"edge element type does not allow '${ConstraintType.NODE_UNIQUE.description()}'",
            inputPosition(c.NODE().getSymbol)
          )
        case c: ConstraintKeyContext if c.NODE() != null =>
          _errors :+= exceptionFactory.invalidInputException(
            ConstraintType.NODE_KEY.description(),
            "edge element type",
            List(ConstraintType.REL_KEY.description()),
            s"edge element type does not allow '${ConstraintType.NODE_KEY.description()}'",
            inputPosition(c.NODE().getSymbol)
          )
        case _ => ()
      }
    }
    // Property level constraint
    if (ctx.arcTypePointingRight().propertyTypeList() != null) {
      ctx.arcTypePointingRight().propertyTypeList().propertyType().asScala.collect {
        case c
          if c.propertyTypeInlineConstraint() != null && c.propertyTypeInlineConstraint().NODE() != null =>
          if (c.propertyTypeInlineConstraint().UNIQUE() != null) {
            _errors :+= exceptionFactory.invalidInputException(
              ConstraintType.NODE_UNIQUE.description(),
              "edge element type property",
              List(ConstraintType.REL_UNIQUE.description()),
              s"edge element type property does not allow '${ConstraintType.NODE_UNIQUE.description()}'",
              inputPosition(c.propertyTypeInlineConstraint().NODE().getSymbol)
            )
          } else if (c.propertyTypeInlineConstraint().KEY() != null) {
            _errors :+= exceptionFactory.invalidInputException(
              ConstraintType.NODE_KEY.description(),
              "edge element type property",
              List(ConstraintType.REL_KEY.description()),
              s"edge element type property does not allow '${ConstraintType.NODE_KEY.description()}'",
              inputPosition(c.propertyTypeInlineConstraint().NODE().getSymbol)
            )
          }
      }
    }
  }

  private def checkConstraintSpecification(ctx: Cypher25Parser.ConstraintSpecificationContext): Unit = {
    // check for incorrect NODE and REL/RELATIONSHIP
    checkConstraintTypeMatchesElementType(ctx.constraintType(), ctx.nodeTypeReference(), ctx.edgeTypeReference())
  }

  private def checkNodeTypeInlineConstraintList(ctx: Cypher25Parser.NodeTypeInlineConstraintListContext): Unit = {
    ctx.constraintType().forEach {
      case c: Cypher25Parser.ConstraintTypedContext =>
        val errorPos = inputPosition(c.getStart)
        _errors :+= exceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC9("type", "node", errorPos.offset, errorPos.line, errorPos.column),
          "Property type constraints cannot be specified inline of a node element type",
          errorPos
        )
      case c: Cypher25Parser.ConstraintIsNotNullContext =>
        val errorPos = inputPosition(c.getStart)
        _errors :+= exceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC9("existence", "node", errorPos.offset, errorPos.line, errorPos.column),
          "Property existence constraints cannot be specified inline of a node element type",
          errorPos
        )
      case _ => ()
    }
  }

  private def checkEdgeTypeInlineConstraintList(ctx: Cypher25Parser.EdgeTypeInlineConstraintListContext): Unit = {
    ctx.constraintType().forEach {
      case c: Cypher25Parser.ConstraintTypedContext =>
        val errorPos = inputPosition(c.getStart)
        _errors :+= exceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC9("type", "relationship", errorPos.offset, errorPos.line, errorPos.column),
          "Property type constraints cannot be specified inline of a relationship element type",
          errorPos
        )
      case c: Cypher25Parser.ConstraintIsNotNullContext =>
        val errorPos = inputPosition(c.getStart)
        _errors :+= exceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC9("existence", "relationship", errorPos.offset, errorPos.line, errorPos.column),
          "Property existence constraints cannot be specified inline of a relationship element type",
          errorPos
        )
      case _ => ()
    }
  }

  private def checkConstraintTypeMatchesElementType(
    constraintTypeContext: Cypher25Parser.ConstraintTypeContext,
    nodePattern: AstRuleCtx,
    relPattern: AstRuleCtx
  ): Unit = {
    constraintTypeContext match {
      case c: ConstraintIsUniqueContext =>
        if (nodePattern != null && (c.RELATIONSHIP() != null || c.REL() != null)) {
          _errors :+= exceptionFactory.invalidInputException(
            "node pattern",
            ConstraintType.REL_UNIQUE.description(),
            List("relationship patterns"),
            s"'${ConstraintType.REL_UNIQUE.description()}' does not allow node patterns",
            inputPosition(nodePattern.getStart)
          )
        }
        if (relPattern != null && c.NODE() != null) {
          _errors :+= exceptionFactory.invalidInputException(
            "relationship pattern",
            ConstraintType.NODE_UNIQUE.description(),
            List("node patterns"),
            s"'${ConstraintType.NODE_UNIQUE.description()}' does not allow relationship patterns",
            inputPosition(relPattern.getStart)
          )
        }
      case c: ConstraintKeyContext =>
        if (nodePattern != null && (c.RELATIONSHIP() != null || c.REL() != null)) {
          _errors :+= exceptionFactory.invalidInputException(
            "node pattern",
            ConstraintType.REL_KEY.description(),
            List("relationship patterns"),
            s"'${ConstraintType.REL_KEY.description()}' does not allow node patterns",
            inputPosition(nodePattern.getStart)
          )
        }
        if (relPattern != null && c.NODE() != null) {
          _errors :+= exceptionFactory.invalidInputException(
            "relationship pattern",
            ConstraintType.NODE_KEY.description(),
            List("node patterns"),
            s"'${ConstraintType.NODE_KEY.description()}' does not allow relationship patterns",
            inputPosition(relPattern.getStart)
          )
        }
      case _: ConstraintTypedContext | _: ConstraintIsNotNullContext =>
      case _ => throw exceptionFactory.internalError("Constraint type is not recognized", pos(constraintTypeContext))
    }
  }

  def failOnObfuscatedLiteral(ctx: ParserRuleContext): Unit = {
    if (!semanticFeatures.contains(EnableParsingOfObfuscatedLiterals)) {
      throw exceptionFactory.invalidInputException("******", List("an expression"), "Invalid input '******'", pos(ctx))
    }
  }
}
