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

import org.neo4j.cypher.internal.ast.ConstraintVersion
import org.neo4j.cypher.internal.ast.ConstraintVersion0
import org.neo4j.cypher.internal.ast.ConstraintVersion1
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.CreateBtreeNodeIndex
import org.neo4j.cypher.internal.ast.CreateBtreeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateIndex
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
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.WaitUntilComplete
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
import org.neo4j.cypher.internal.parser.CypherParser.CreateCommandContext
import org.neo4j.cypher.internal.parser.CypherParser.CreateIndexContext
import org.neo4j.cypher.internal.parser.CypherParserListener
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.collection.immutable.ArraySeq

trait DdlCreateBuilder extends CypherParserListener {

  final override def exitCreateCommand(
    ctx: CypherParser.CreateCommandContext
  ): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx).ast
  }

  final override def exitCreateAlias(
    ctx: CypherParser.CreateAliasContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val aliasName = ctx.symbolicAliasNameOrParameter(0).ast[DatabaseName]()
    val dbName = ctx.symbolicAliasNameOrParameter(1).ast[DatabaseName]()
    val ifNotExists = ctx.EXISTS() != null
    val properties =
      if (ctx.PROPERTIES() != null) {
        if (ctx.DRIVER() != null) Some(ctx.mapOrParameter(1).ast[Either[Map[String, Expression], Parameter]]())
        else Some(ctx.mapOrParameter(0).ast[Either[Map[String, Expression], Parameter]]())
      } else None

    ctx.ast = if (ctx.AT() == null) {
      CreateLocalDatabaseAlias(aliasName, dbName, ifExistsDo(parent.REPLACE() != null, ifNotExists), properties)(pos(
        parent
      ))
    } else {
      val driverSettings =
        if (ctx.DRIVER() != null) Some(ctx.mapOrParameter(0).ast[Either[Map[String, Expression], Parameter]]())
        else None
      CreateRemoteDatabaseAlias(
        aliasName,
        dbName,
        ifExistsDo(parent.REPLACE() != null, ifNotExists),
        ctx.stringOrParameter().ast[Either[String, Parameter]](),
        ctx.commandNameExpression().ast[Expression](),
        ctx.passwordExpression().ast[Expression](),
        driverSettings,
        properties
      )(pos(parent))
    }
  }

  final override def exitCreateCompositeDatabase(
    ctx: CypherParser.CreateCompositeDatabaseContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    ctx.ast = CreateCompositeDatabase(
      ctx.symbolicAliasNameOrParameter().ast[DatabaseName](),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null),
      astOpt[Options](ctx.commandOptions(), NoOptions),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(parent))
  }

  final override def exitCreateConstraint(
    ctx: CypherParser.CreateConstraintContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val nodePattern = ctx.commandNodePattern()
    val isNode = nodePattern != null
    val constraintName = astOpt[Either[String, Parameter]](ctx.symbolicNameOrStringParameter())
    val existsDo = ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null)
    val containsOn = ctx.ON() != null
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val cT = ctx.constraintType()
    val (constraintVersion, properties, propertyType) =
      cT.ast[(ConstraintVersion, ArraySeq[Property], Option[CypherType])]

    ctx.ast = if (isNode) {
      val (variable, label) = nodePattern.ast[(Variable, LabelName)]()
      cT match {
        case _: ConstraintExistsContext | _: ConstraintIsNotNullContext =>
          CreateNodePropertyExistenceConstraint(
            variable,
            label,
            properties(0),
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _: ConstraintTypedContext =>
          CreateNodePropertyTypeConstraint(
            variable,
            label,
            properties(0),
            propertyType.get,
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _: ConstraintIsUniqueContext =>
          CreateNodePropertyUniquenessConstraint(
            variable,
            label,
            properties,
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _: ConstraintKeyContext =>
          CreateNodeKeyConstraint(
            variable,
            label,
            properties,
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _ => throw new IllegalStateException("Unknown Constraint Command")
      }
    } else {
      val (variable, relType) = ctx.commandRelPattern().ast[(Variable, RelTypeName)]()
      cT match {
        case _: ConstraintExistsContext | _: ConstraintIsNotNullContext =>
          CreateRelationshipPropertyExistenceConstraint(
            variable,
            relType,
            properties(0),
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _: ConstraintTypedContext =>
          CreateRelationshipPropertyTypeConstraint(
            variable,
            relType,
            properties(0),
            propertyType.get,
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _: ConstraintIsUniqueContext =>
          CreateRelationshipPropertyUniquenessConstraint(
            variable,
            relType,
            properties,
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _: ConstraintKeyContext =>
          CreateRelationshipKeyConstraint(
            variable,
            relType,
            properties,
            constraintName,
            existsDo,
            options,
            containsOn,
            constraintVersion
          )(pos(parent))
        case _ => throw new IllegalStateException("Unexpected Constraint Command")
      }
    }
  }

  override def exitConstraintType(ctx: CypherParser.ConstraintTypeContext): Unit = {
    ctx.ast = ctx match {
      case cTC: ConstraintExistsContext =>
        val constraintVersion = ConstraintVersion0
        val properties = cTC.propertyList.ast[ArraySeq[Property]]()
        (constraintVersion, properties, None)
      case cTC: ConstraintIsNotNullContext =>
        val constraintVersion =
          if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion1
        val properties = cTC.propertyList.ast[ArraySeq[Property]]()
        (constraintVersion, properties, None)
      case cTC: ConstraintTypedContext =>
        val constraintVersion =
          if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
        val properties = cTC.propertyList.ast[ArraySeq[Property]]()
        val propertyType = cTC.`type`().ast[CypherType]()
        (constraintVersion, properties, Some(propertyType))
      case cTC: ConstraintIsUniqueContext =>
        val constraintVersion =
          if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
        val properties = cTC.propertyList.ast[ArraySeq[Property]]()
        (constraintVersion, properties, None)
      case cTC: ConstraintKeyContext =>
        val constraintVersion =
          if (cTC.REQUIRE() != null) ConstraintVersion2 else ConstraintVersion0
        val properties = cTC.propertyList().ast[Seq[Property]]()
        (constraintVersion, properties, None)
      case _ => throw new IllegalStateException("Unknown Constraint Command")
    }
  }

  final override def exitCreateDatabase(
    ctx: CypherParser.CreateDatabaseContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val topology =
      if (ctx.TOPOLOGY() != null) {
        val pT = astOptFromList[Int](ctx.primaryTopology(), None)
        val sT = astOptFromList[Int](ctx.secondaryTopology(), None)
        Some(Topology(pT, sT))
      } else None
    ctx.ast = CreateDatabase(
      ctx.symbolicAliasNameOrParameter().ast[DatabaseName](),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null),
      astOpt[Options](ctx.commandOptions(), NoOptions),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait),
      topology
    )(pos(parent))
  }

  final override def exitPrimaryTopology(ctx: CypherParser.PrimaryTopologyContext): Unit = {
    ctx.ast = nodeChild(ctx, 0).getText.toInt
  }

  final override def exitSecondaryTopology(ctx: CypherParser.SecondaryTopologyContext): Unit = {
    ctx.ast = nodeChild(ctx, 0).getText.toInt
  }

  final override def exitCreateIndex(
    ctx: CypherParser.CreateIndexContext
  ): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx).ast[CreateIndex]()
  }

  final override def exitOldCreateIndex(
    ctx: CypherParser.OldCreateIndexContext
  ): Unit = {
    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    ctx.ast = CreateIndexOldSyntax(
      ctx.labelType().ast[LabelName](),
      nonEmptyPropertyKeyName(ctx.nonEmptyNameList()).toList
    )(pos(grandparent))
  }

  final override def exitCreateIndex_(
    ctx: CypherParser.CreateIndex_Context
  ): Unit = {

    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    val parent = ctx.getParent.asInstanceOf[CreateIndexContext]
    val existsDo = ifExistsDo(grandparent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val indexName = astOpt[Either[String, Parameter]](ctx.symbolicNameOrStringParameter())

    val nodePattern = ctx.commandNodePattern()
    val relPattern = ctx.commandRelPattern()
    val isNode = nodePattern != null
    val propertyList = ctx.propertyList().ast[ArraySeq[Property]]().toList
    val (variable, labelOrRelType) =
      if (isNode) nodePattern.ast[(Variable, LabelName)]()
      else relPattern.ast[(Variable, RelTypeName)]()

    val token = nodeChild(parent, 0).getSymbol.getType

    ctx.ast = token match {
      case CypherParser.BTREE =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreateBtreeNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreateBtreeRelationshipIndex(
            variable,
            relType,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        }
      case CypherParser.RANGE | CypherParser.INDEX =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreateRangeNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options,
            fromDefault = token == CypherParser.INDEX
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreateRangeRelationshipIndex(
            variable,
            relType,
            propertyList,
            indexName,
            existsDo,
            options,
            fromDefault = token == CypherParser.INDEX
          )(pos(grandparent))
        }
      case CypherParser.TEXT =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreateTextNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreateTextRelationshipIndex(
            variable,
            relType,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        }
      case CypherParser.POINT =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreatePointNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreatePointRelationshipIndex(
            variable,
            relType,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        }
      case CypherParser.VECTOR =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreateVectorNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreateVectorRelationshipIndex(
            variable,
            relType,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        }
    }
  }

  final override def exitCreateFulltextIndex(
    ctx: CypherParser.CreateFulltextIndexContext
  ): Unit = {
    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    val existsDo = ifExistsDo(grandparent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val indexName = astOpt[Either[String, Parameter]](ctx.symbolicNameOrStringParameter())
    val nodePattern = ctx.fulltextNodePattern()
    val isNode = nodePattern != null
    val propertyList = {
      val exprs = astSeq[Expression](ctx.variable())
      val propertyKeyNames = astSeq[PropertyKeyName](ctx.property())
      exprs.zip(propertyKeyNames).map { case (e, p) => Property(e, p)(Util.pos(ctx.LBRACKET().getSymbol)) }
    }.toList
    ctx.ast = if (isNode) {
      val (variable, labels) = nodePattern.ast[(Variable, List[LabelName])]()
      CreateFulltextNodeIndex(
        variable,
        labels,
        propertyList,
        indexName,
        existsDo,
        options
      )(pos(grandparent))
    } else {
      val (variable, relTypes) = ctx.fulltextRelPattern().ast[(Variable, List[RelTypeName])]()
      CreateFulltextRelationshipIndex(
        variable,
        relTypes,
        propertyList,
        indexName,
        existsDo,
        options
      )(pos(grandparent))
    }
  }

  def exitFulltextNodePattern(ctx: CypherParser.FulltextNodePatternContext): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      astSeqPositioned[LabelName, String](ctx.symbolicNameString(), LabelName.apply).toList
    )
  }

  def exitFulltextRelPattern(ctx: CypherParser.FulltextRelPatternContext): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      astSeqPositioned[RelTypeName, String](ctx.symbolicNameString(), RelTypeName.apply).toList
    )
  }

  def exitLookupIndexNodePattern(ctx: CypherParser.LookupIndexNodePatternContext): Unit = {
    ctx.ast = ctx.variable().ast[Variable]()
  }

  def exitLookupIndexRelPattern(ctx: CypherParser.LookupIndexRelPatternContext): Unit = {
    ctx.ast = ctx.variable().ast[Variable]()
  }

  final override def exitCreateLookupIndex(
    ctx: CypherParser.CreateLookupIndexContext
  ): Unit = {
    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    val existsDo = ifExistsDo(grandparent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val indexName = astOpt[Either[String, Parameter]](ctx.symbolicNameOrStringParameter())
    val nodePattern = ctx.lookupIndexNodePattern()
    val isNode = nodePattern != null
    val functionName = ctx.symbolicNameString
    val functionPos = Util.pos(functionName)
    val function = FunctionInvocation(
      FunctionName(functionName.ast[String]())(functionPos),
      distinct = false,
      IndexedSeq(ctx.variable().ast[Variable]())
    )(functionPos)
    val variable =
      if (isNode) nodePattern.ast[Variable]()
      else ctx.lookupIndexRelPattern().ast[Variable]()
    ctx.ast = CreateLookupIndex(
      variable,
      isNode,
      function,
      indexName,
      existsDo,
      options
    )(pos(grandparent))
  }

  final override def exitCreateRole(
    ctx: CypherParser.CreateRoleContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val nameExpressions = ctx.commandNameExpression()
    val from =
      if (nameExpressions.size > 1) {
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(nameExpressions.size == 2)
        Some(nameExpressions.get(1).ast[Expression])
      } else
        None
    ctx.ast = CreateRole(
      nameExpressions.get(0).ast[Expression](),
      from,
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null)
    )(pos(parent))
  }

  final override def exitCreateUser(
    ctx: CypherParser.CreateUserContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val passCtx = ctx.password()
    val passwordReq =
      if (passCtx.passwordChangeRequired() != null) {
        Some(passCtx.passwordChangeRequired().ast[Boolean]())
      } else astOptFromList[Boolean](ctx.passwordChangeRequired(), Some(true))
    val suspended = astOptFromList[Boolean](ctx.userStatus(), None)
    val homeDatabaseAction = astOptFromList[HomeDatabaseAction](ctx.homeDatabase(), None)
    ctx.ast = CreateUser(
      ctx.commandNameExpression().ast[Expression](),
      passCtx.ENCRYPTED() != null,
      passCtx.passwordExpression().ast[Expression](),
      UserOptions(passwordReq, suspended, homeDatabaseAction),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null)
    )(pos(parent))
  }
}
