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
package org.neo4j.cypher.internal.cst.factory.neo4j.ast

import org.neo4j.cypher.internal.ast.ConstraintVersion0
import org.neo4j.cypher.internal.ast.ConstraintVersion1
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.CreateBtreeNodeIndex
import org.neo4j.cypher.internal.ast.CreateBtreeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateIndexOldSyntax
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyTypeConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.CreatePointNodeIndex
import org.neo4j.cypher.internal.ast.CreatePointRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRangeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRelationshipKeyConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyTypeConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateVectorNodeIndex
import org.neo4j.cypher.internal.ast.CreateVectorRelationshipIndex
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.WriteAdministrationCommand
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOpt
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOptFromList
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeq
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeqPositioned
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.ifExistsDo
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.lastChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nonEmptyPropertyKeyName
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintExistsContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintIsNotNullContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintIsUniqueContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintKeyContext
import org.neo4j.cypher.internal.parser.CypherParser.ConstraintTypedContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateConstraintContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateDatabaseContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateIndexContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateRoleContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateUserContext
import org.neo4j.cypher.internal.parser.CypherParserListener
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.collection.immutable.ArraySeq

trait DdlCreateBuilder extends CypherParserListener {

  final override def exitCreateCommand(
    ctx: CypherParser.CreateCommandContext
  ): Unit = {
    val replace = ctx.REPLACE() != null
    val createCmd = lastChild[AstRuleCtx](ctx)
    ctx.ast = createCmd match {
      case c: CypherParser.CreateAliasContext             => createAliasBuilder(c, replace, pos(ctx))
      case c: CypherParser.CreateCompositeDatabaseContext => createCompositeDatabase(c, replace, pos(ctx))
      case c: CypherParser.CreateConstraintContext        => createConstraintBuilder(c, replace, pos(ctx))
      case c: CypherParser.CreateDatabaseContext          => createDatabaseBuilder(c, replace, pos(ctx))
      case c: CypherParser.CreateIndexContext             => createIndexBuilder(c, replace, pos(ctx))
      case c: CypherParser.CreateRoleContext              => createRoleBuilder(c, replace, pos(ctx))
      case c: CypherParser.CreateUserContext              => createUserBuilder(c, replace, pos(ctx))
      case _                                              => null // TODO ERROR HANDLING
    }
  }

  private def createRoleBuilder(c: CreateRoleContext, replace: Boolean, pos: InputPosition): CreateRole = {
    val nameExpressions = c.commandNameExpression()
    val roleName = nameExpressions.get(0).ast[Expression]()
    val from =
      if (nameExpressions.size > 1) {
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(nameExpressions.size == 2)
        Some(nameExpressions.get(1).ast[Expression])
      } else
        None
    val ifNotExists = c.EXISTS() != null
    CreateRole(roleName, from, ifExistsDo(replace, ifNotExists))(pos)
  }

  private def createUserBuilder(c: CreateUserContext, replace: Boolean, pos: InputPosition): CreateUser = {
    val userName = c.commandNameExpression().ast[Expression]()
    val passCtx = c.password()
    val isEncryptedPassword = passCtx.ENCRYPTED() != null
    val initialPassword = passCtx.passwordExpression().ast[Expression]()
    val passwordReq = if (passCtx.passwordChangeRequired() != null) {
      Some(passCtx.passwordChangeRequired().ast[Boolean]())
    } else astOptFromList[Boolean](c.passwordChangeRequired(), Some(true))
    val suspended = astOptFromList[Boolean](c.userStatus(), None)
    val homeDatabaseAction = astOptFromList[HomeDatabaseAction](c.homeDatabase(), None)
    val userOptions = UserOptions(passwordReq, suspended, homeDatabaseAction)
    val ifNotExists = c.EXISTS() != null
    CreateUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExistsDo(replace, ifNotExists))(pos)
  }

  private def createDatabaseBuilder(c: CreateDatabaseContext, replace: Boolean, pos: InputPosition): CreateDatabase = {
    val dbNameCtx = c.symbolicAliasNameOrParameter()
    val dbName = dbNameCtx.ast[DatabaseName]()
    val ifNotExists = c.EXISTS() != null
    val options = astOpt[Options](c.commandOptions(), NoOptions)
    val waitUntilComplete = astOpt[WaitUntilComplete](c.waitClause(), NoWait)
    val topology =
      if (c.TOPOLOGY() != null) {
        val pT = astOptFromList[Int](c.primaryTopology(), None)
        val sT = astOptFromList[Int](c.secondaryTopology(), None)
        Some(Topology(pT, sT))
      } else None
    CreateDatabase(dbName, ifExistsDo(replace, ifNotExists), options, waitUntilComplete, topology)(pos)
  }

  final override def exitPrimaryTopology(ctx: CypherParser.PrimaryTopologyContext): Unit = {
    ctx.ast = nodeChild(ctx, 0).getText.toInt
  }

  final override def exitSecondaryTopology(ctx: CypherParser.SecondaryTopologyContext): Unit = {
    ctx.ast = nodeChild(ctx, 0).getText.toInt
  }

  private def createConstraintBuilder(
    c: CreateConstraintContext,
    replace: Boolean,
    pos: InputPosition
  ): CreateConstraint = {
    val isNode = c.commandNodePattern() != null
    val constraintName = astOpt[Either[String, Parameter]](c.symbolicNameOrStringParameter())
    val ifNotExists = c.EXISTS() != null
    val containsOn = c.ON() != null
    val options = astOpt[Options](c.commandOptions(), NoOptions)
    val cT = c.constraintType()

    if (isNode) {
      val variable = c.commandNodePattern().variable().ast[Variable]()
      val label = c.commandNodePattern().labelType().ast[LabelName]()
      cT match {
        case cTC: ConstraintExistsContext =>
          val constraintVersion = ConstraintVersion0
          val property = cTC.propertyList.ast[ArraySeq[Property]]()(0)
          CreateNodePropertyExistenceConstraint(
            variable,
            label,
            property,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintIsNotNullContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion1
          val property = cTC.propertyList.ast[ArraySeq[Property]]()(0)
          CreateNodePropertyExistenceConstraint(
            variable,
            label,
            property,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintTypedContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
          val property = cTC.propertyList.ast[ArraySeq[Property]]()(0)
          val propertyType = cTC.`type`().ast[CypherType]()
          CreateNodePropertyTypeConstraint(
            variable,
            label,
            property,
            propertyType,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintIsUniqueContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
          val properties = cTC.propertyList.ast[ArraySeq[Property]]()
          CreateNodePropertyUniquenessConstraint(
            variable,
            label,
            properties,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintKeyContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
          val properties = cTC.propertyList().ast[Seq[Property]]()
          CreateNodeKeyConstraint(
            variable,
            label,
            properties,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case _ => throw new RuntimeException("Unknown Constraint") // TODO Error handling
      }
    } else {
      val variable = c.commandRelPattern().variable().ast[Variable]()
      val relType = c.commandRelPattern().relType().ast[RelTypeName]()
      cT match {
        case cTC: ConstraintExistsContext =>
          val constraintVersion = ConstraintVersion0
          val property = cTC.propertyList.ast[ArraySeq[Property]]()(0)
          CreateRelationshipPropertyExistenceConstraint(
            variable,
            relType,
            property,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintIsNotNullContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion1
          val property = cTC.propertyList.ast[ArraySeq[Property]]()(0)
          CreateRelationshipPropertyExistenceConstraint(
            variable,
            relType,
            property,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintTypedContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
          val property = cTC.propertyList.ast[ArraySeq[Property]]()(0)
          val propertyType = cTC.`type`().ast[CypherType]()
          CreateRelationshipPropertyTypeConstraint(
            variable,
            relType,
            property,
            propertyType,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintIsUniqueContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
          val properties = cTC.propertyList.ast[ArraySeq[Property]]()
          CreateRelationshipPropertyUniquenessConstraint(
            variable,
            relType,
            properties,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case cTC: ConstraintKeyContext =>
          val constraintVersion =
            if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
          val properties = cTC.propertyList().ast[Seq[Property]]()
          CreateRelationshipKeyConstraint(
            variable,
            relType,
            properties,
            constraintName,
            ifExistsDo(replace, ifNotExists),
            options,
            containsOn,
            constraintVersion
          )(pos)
        case _ => throw new RuntimeException("Unknown Constraint") // TODO Error handling
      }
    }
  }

  override def exitConstraintType(ctx: CypherParser.ConstraintTypeContext): Unit = {}

  private def createIndexBuilder(
    c: CreateIndexContext,
    replace: Boolean,
    pos: InputPosition
  ): SchemaCommand = {
    val token = nodeChild(c, 0).getSymbol.getType
    token match {
      case CypherParser.LOOKUP =>
        val cIndex = c.createLookupIndex()
        val ifNotExists = cIndex.EXISTS() != null
        val options = astOpt[Options](cIndex.commandOptions(), NoOptions)
        val indexName = astOpt[Either[String, Parameter]](cIndex.symbolicNameOrStringParameter())
        val isNode = cIndex.lookupIndexNodePattern() != null
        val functionName = cIndex.symbolicNameString
        val functionPos = Util.pos(functionName)
        val function = FunctionInvocation(
          FunctionName(functionName.ast[String]())(functionPos),
          distinct = false,
          IndexedSeq(cIndex.variable().ast[Variable]())
        )(functionPos)
        val variable =
          if (isNode) cIndex.lookupIndexNodePattern().variable().ast[Variable]()
          else cIndex.lookupIndexRelPattern().variable().ast[Variable]()
        CreateLookupIndex(
          variable,
          isNode,
          function,
          indexName,
          ifExistsDo(replace, ifNotExists),
          options
        )(pos)
      case CypherParser.FULLTEXT =>
        val cIndex = c.createFulltextIndex()
        val ifNotExists = cIndex.EXISTS() != null
        val options = astOpt[Options](cIndex.commandOptions(), NoOptions)
        val indexName = astOpt[Either[String, Parameter]](cIndex.symbolicNameOrStringParameter())
        val isNode = cIndex.fulltextNodePattern() != null
        val propertyList = {
          val exprs = astSeq[Expression](cIndex.variable())
          val propertyKeyNames = astSeq[PropertyKeyName](cIndex.property())
          exprs.zip(propertyKeyNames).map { case (e, p) => Property(e, p)(Util.pos(cIndex.LBRACKET().getSymbol)) }
        }.toList
        if (isNode) {
          val nodePattern = cIndex.fulltextNodePattern()
          val variable = nodePattern.variable().ast[Variable]()
          val labels = astSeqPositioned[LabelName, String](nodePattern.symbolicNameString(), LabelName.apply).toList
          CreateFulltextNodeIndex(
            variable,
            labels,
            propertyList,
            indexName,
            ifExistsDo(replace, ifNotExists),
            options
          )(pos)
        } else {
          val relPattern = cIndex.fulltextRelPattern()
          val variable = relPattern.variable().ast[Variable]()
          val relTypes =
            astSeqPositioned[RelTypeName, String](relPattern.symbolicNameString(), RelTypeName.apply).toList
          CreateFulltextRelationshipIndex(
            variable,
            relTypes,
            propertyList,
            indexName,
            ifExistsDo(replace, ifNotExists),
            options
          )(pos)
        }
      case CypherParser.INDEX =>
        if (c.ON() != null) {
          val cIndex = c.oldCreateIndex()
          val label = cIndex.labelType().ast[LabelName]()
          val propertyList = nonEmptyPropertyKeyName(cIndex.nonEmptyNameList()).toList
          CreateIndexOldSyntax(label, propertyList)(pos)
        } else {
          // TODO Find a way to merge with RangeNodeIndex?
          val cIndex = c.createIndex_()
          val ifNotExists = cIndex.EXISTS() != null
          val options = astOpt[Options](cIndex.commandOptions(), NoOptions)
          val indexName = astOpt[Either[String, Parameter]](cIndex.symbolicNameOrStringParameter())
          val isNode = cIndex.commandNodePattern() != null
          val propertyList = cIndex.propertyList().ast[ArraySeq[Property]]().toList
          if (isNode) {
            val nodePattern = cIndex.commandNodePattern()
            val variable = nodePattern.variable().ast[Variable]()
            val label = nodePattern.labelType().ast[LabelName]()
            CreateRangeNodeIndex(
              variable,
              label,
              propertyList,
              indexName,
              ifExistsDo(replace, ifNotExists),
              options,
              fromDefault = true
            )(pos)
          } else {
            val relPattern = cIndex.commandRelPattern()
            val variable = relPattern.variable().ast[Variable]()
            val relType = relPattern.relType().ast[RelTypeName]()
            CreateRangeRelationshipIndex(
              variable,
              relType,
              propertyList,
              indexName,
              ifExistsDo(replace, ifNotExists),
              options,
              fromDefault = true
            )(pos)
          }
        }
      case _ =>
        val cIndex = c.createIndex_()
        val ifNotExists = cIndex.EXISTS() != null
        val options = astOpt[Options](cIndex.commandOptions(), NoOptions)
        val indexName = astOpt[Either[String, Parameter]](cIndex.symbolicNameOrStringParameter())

        val nodePattern = cIndex.commandNodePattern()
        val relPattern = cIndex.commandRelPattern()
        val isNode = nodePattern != null
        val propertyList = cIndex.propertyList().ast[ArraySeq[Property]]().toList
        val variable = if (isNode) nodePattern.variable().ast[Variable]() else relPattern.variable().ast[Variable]()
        val labelOrRelType =
          if (isNode) nodePattern.labelType().ast[LabelName]() else relPattern.relType().ast[RelTypeName]()

        token match {
          case CypherParser.BTREE =>
            if (isNode) {
              val label = labelOrRelType.asInstanceOf[LabelName]
              CreateBtreeNodeIndex(
                variable,
                label,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            } else {
              val relType = labelOrRelType.asInstanceOf[RelTypeName]
              CreateBtreeRelationshipIndex(
                variable,
                relType,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            }
          case CypherParser.RANGE =>
            if (isNode) {
              val label = labelOrRelType.asInstanceOf[LabelName]
              CreateRangeNodeIndex(
                variable,
                label,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options,
                fromDefault = false
              )(pos)
            } else {
              val relType = labelOrRelType.asInstanceOf[RelTypeName]
              CreateRangeRelationshipIndex(
                variable,
                relType,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options,
                fromDefault = false
              )(pos)
            }
          case CypherParser.TEXT =>
            if (isNode) {
              val label = labelOrRelType.asInstanceOf[LabelName]
              CreateTextNodeIndex(
                variable,
                label,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            } else {
              val relType = labelOrRelType.asInstanceOf[RelTypeName]
              CreateTextRelationshipIndex(
                variable,
                relType,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            }
          case CypherParser.POINT =>
            if (isNode) {
              val label = labelOrRelType.asInstanceOf[LabelName]
              CreatePointNodeIndex(
                variable,
                label,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            } else {
              val relType = labelOrRelType.asInstanceOf[RelTypeName]
              CreatePointRelationshipIndex(
                variable,
                relType,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            }
          case CypherParser.VECTOR =>
            if (isNode) {
              val label = labelOrRelType.asInstanceOf[LabelName]
              CreateVectorNodeIndex(
                variable,
                label,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            } else {
              val relType = labelOrRelType.asInstanceOf[RelTypeName]
              CreateVectorRelationshipIndex(
                variable,
                relType,
                propertyList,
                indexName,
                ifExistsDo(replace, ifNotExists),
                options
              )(pos)
            }
        }
    }
  }

  final override def exitCreateIndex(
    ctx: CypherParser.CreateIndexContext
  ): Unit = {}

  final override def exitOldCreateIndex(
    ctx: CypherParser.OldCreateIndexContext
  ): Unit = {}

  final override def exitCreateIndex_(
    ctx: CypherParser.CreateIndex_Context
  ): Unit = {}

  final override def exitCreateFulltextIndex(
    ctx: CypherParser.CreateFulltextIndexContext
  ): Unit = {}

  def exitFulltextNodePattern(ctx: CypherParser.FulltextNodePatternContext): Unit = {}
  def exitFulltextRelPattern(ctx: CypherParser.FulltextRelPatternContext): Unit = {}

  def exitLookupIndexNodePattern(ctx: CypherParser.LookupIndexNodePatternContext): Unit = {}

  def exitLookupIndexRelPattern(ctx: CypherParser.LookupIndexRelPatternContext): Unit = {}

  final override def exitCreateLookupIndex(
    ctx: CypherParser.CreateLookupIndexContext
  ): Unit = {}

  private def createAliasBuilder(
    c: CypherParser.CreateAliasContext,
    replace: Boolean,
    pos: InputPosition
  ): WriteAdministrationCommand = {
    val aliasName = c.symbolicAliasNameOrParameter(0).ast[DatabaseName]()
    val dbName = c.symbolicAliasNameOrParameter(1).ast[DatabaseName]()
    val ifNotExists = c.EXISTS() != null
    val properties =
      if (c.PROPERTIES() != null) {
        if (c.DRIVER() != null) Some(c.mapOrParameter(1).ast[Either[Map[String, Expression], Parameter]]())
        else Some(c.mapOrParameter(0).ast[Either[Map[String, Expression], Parameter]]())
      } else None

    if (c.AT() == null) {
      CreateLocalDatabaseAlias(aliasName, dbName, ifExistsDo(replace, ifNotExists), properties)(pos)
    } else {
      val url = c.stringOrParameter().ast[Either[String, Parameter]]()
      val username = c.commandNameExpression().ast[Expression]()
      val password = c.passwordExpression().ast[Expression]()
      val driverSettings =
        if (c.DRIVER() != null) Some(c.mapOrParameter(0).ast[Either[Map[String, Expression], Parameter]]()) else None
      CreateRemoteDatabaseAlias(
        aliasName,
        dbName,
        ifExistsDo(replace, ifNotExists),
        url,
        username,
        password,
        driverSettings,
        properties
      )(pos)
    }
  }

  private def createCompositeDatabase(
    c: CypherParser.CreateCompositeDatabaseContext,
    replace: Boolean,
    pos: InputPosition
  ): CreateCompositeDatabase = {
    val dbName = c.symbolicAliasNameOrParameter().ast[DatabaseName]()
    val ifNotExists = c.EXISTS() != null
    val options = astOpt[Options](c.commandOptions(), NoOptions)
    val waitUntilComplete = astOpt[WaitUntilComplete](c.waitClause(), NoWait)
    CreateCompositeDatabase(dbName, ifExistsDo(replace, ifNotExists), options, waitUntilComplete)(pos)
  }

  final override def exitCreateConstraint(
    ctx: CypherParser.CreateConstraintContext
  ): Unit = {}

}
