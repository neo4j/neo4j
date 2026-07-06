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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.Auth
import org.neo4j.cypher.internal.ast.AuthAttribute
import org.neo4j.cypher.internal.ast.AuthRuleCondition
import org.neo4j.cypher.internal.ast.AuthRuleEnabled
import org.neo4j.cypher.internal.ast.AuthRuleSetClause
import org.neo4j.cypher.internal.ast.CreateAuthRule
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateReplicaDatabase
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OidcCredentialForwarding
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.PasswordChange
import org.neo4j.cypher.internal.ast.RemoteAliasCredentials
import org.neo4j.cypher.internal.ast.RemoteAliasStoredCredentials
import org.neo4j.cypher.internal.ast.SetTags
import org.neo4j.cypher.internal.ast.ShardDefinition
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astOptFromList
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeqPositioned
import org.neo4j.cypher.internal.parser.ast.util.Util.ifExistsDo
import org.neo4j.cypher.internal.parser.ast.util.Util.lastChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.ast.util.Util.rangePos
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintIsNotNullContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintIsUniqueContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintKeyContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.ConstraintTypedContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.CreateCommandContext
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser.CreateIndexContext
import org.neo4j.cypher.internal.parser.v25.Cypher25ParserListener
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.ListHasAsScala

trait DdlCreateBuilder extends Cypher25ParserListener {

  final override def exitCreateCommand(
    ctx: Cypher25Parser.CreateCommandContext
  ): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx).ast
  }

  // Create constraint and index command contexts

  final override def exitCreateConstraint(
    ctx: Cypher25Parser.CreateConstraintContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val nodePattern = ctx.commandNodePattern()
    val isNode = nodePattern != null
    val constraintName = astOpt[Expression](ctx.commandNameExpression())
    val existsDo = ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val cT = ctx.constraintType()
    val (properties, propertyType) =
      cT.ast[(ArraySeq[Property], Option[CypherType])]

    ctx.ast = if (isNode) {
      val (variable, label) = nodePattern.ast[(Variable, LabelName)]()
      cT match {
        case _: ConstraintIsNotNullContext =>
          CreateConstraint.createNodePropertyExistenceConstraint(
            variable,
            label,
            properties(0),
            constraintName,
            existsDo,
            options
          )(pos(parent))
        case _: ConstraintTypedContext =>
          CreateConstraint.createNodePropertyTypeConstraint(
            variable,
            label,
            properties(0),
            propertyType.get,
            constraintName,
            existsDo,
            options
          )(pos(parent))
        case _: ConstraintIsUniqueContext =>
          CreateConstraint.createNodePropertyUniquenessConstraint(
            variable,
            label,
            properties,
            constraintName,
            existsDo,
            options,
            fromCypher5 = false
          )(pos(parent))
        case _: ConstraintKeyContext =>
          CreateConstraint.createNodeKeyConstraint(
            variable,
            label,
            properties,
            constraintName,
            existsDo,
            options,
            fromCypher5 = false
          )(pos(parent))
        case _ => throw new IllegalStateException("Unknown Constraint Command")
      }
    } else {
      val (variable, relType) = ctx.commandRelPattern().ast[(Variable, RelTypeName)]()
      cT match {
        case _: ConstraintIsNotNullContext =>
          CreateConstraint.createRelationshipPropertyExistenceConstraint(
            variable,
            relType,
            properties(0),
            constraintName,
            existsDo,
            options
          )(pos(parent))
        case _: ConstraintTypedContext =>
          CreateConstraint.createRelationshipPropertyTypeConstraint(
            variable,
            relType,
            properties(0),
            propertyType.get,
            constraintName,
            existsDo,
            options
          )(pos(parent))
        case _: ConstraintIsUniqueContext =>
          CreateConstraint.createRelationshipPropertyUniquenessConstraint(
            variable,
            relType,
            properties,
            constraintName,
            existsDo,
            options,
            fromCypher5 = false
          )(pos(parent))
        case _: ConstraintKeyContext =>
          CreateConstraint.createRelationshipKeyConstraint(
            variable,
            relType,
            properties,
            constraintName,
            existsDo,
            options,
            fromCypher5 = false
          )(pos(parent))
        case _ => throw new IllegalStateException("Unexpected Constraint Command")
      }
    }
  }

  override def exitConstraintType(ctx: Cypher25Parser.ConstraintTypeContext): Unit = {
    ctx.ast = ctx match {
      case cTC: ConstraintIsNotNullContext =>
        val properties = cTC.propertyList.ast[ArraySeq[Property]]()
        (properties, None)
      case cTC: ConstraintTypedContext =>
        val properties = cTC.propertyList.ast[ArraySeq[Property]]()
        val propertyType = cTC.`type`().ast[CypherType]()
        (properties, Some(propertyType))
      case cTC: ConstraintIsUniqueContext =>
        val properties = cTC.propertyList.ast[ArraySeq[Property]]()
        (properties, None)
      case cTC: ConstraintKeyContext =>
        val properties = cTC.propertyList().ast[Seq[Property]]()
        (properties, None)
      case _ => throw new IllegalStateException("Unknown Constraint Command")
    }
  }

  final override def exitCreateIndex(
    ctx: Cypher25Parser.CreateIndexContext
  ): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx).ast[CreateIndex]()
  }

  final override def exitCreateIndex_(
    ctx: Cypher25Parser.CreateIndex_Context
  ): Unit = {

    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    val parent = ctx.getParent.asInstanceOf[CreateIndexContext]
    val existsDo = ifExistsDo(grandparent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val indexName = astOpt[Expression](ctx.commandNameExpression())

    val nodePattern = ctx.commandNodePattern()
    val relPattern = ctx.commandRelPattern()
    val isNode = nodePattern != null
    val propertyList = ctx.propertyList().ast[ArraySeq[Property]]().toList
    val (variable, labelOrRelType) =
      if (isNode) nodePattern.ast[(Variable, LabelName)]()
      else relPattern.ast[(Variable, RelTypeName)]()

    val token = nodeChild(parent, 0).getSymbol.getType

    ctx.ast = token match {
      case Cypher25Parser.RANGE | Cypher25Parser.INDEX =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreateIndex.createRangeNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options,
            fromDefault = token == Cypher25Parser.INDEX
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreateIndex.createRangeRelationshipIndex(
            variable,
            relType,
            propertyList,
            indexName,
            existsDo,
            options,
            fromDefault = token == Cypher25Parser.INDEX
          )(pos(grandparent))
        }
      case Cypher25Parser.TEXT =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreateIndex.createTextNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreateIndex.createTextRelationshipIndex(
            variable,
            relType,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        }
      case Cypher25Parser.POINT =>
        if (isNode) {
          val label = labelOrRelType.asInstanceOf[LabelName]
          CreateIndex.createPointNodeIndex(
            variable,
            label,
            propertyList,
            indexName,
            existsDo,
            options
          )(pos(grandparent))
        } else {
          val relType = labelOrRelType.asInstanceOf[RelTypeName]
          CreateIndex.createPointRelationshipIndex(
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
    ctx: Cypher25Parser.CreateFulltextIndexContext
  ): Unit = {
    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    val existsDo = ifExistsDo(grandparent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val indexName = astOpt[Expression](ctx.commandNameExpression())
    val nodePattern = ctx.multiLabelNodePattern()
    val isNode = nodePattern != null
    val propertyList = ctx.enclosedPropertyList().ast[Seq[Property]]().toList
    ctx.ast = if (isNode) {
      val (variable, labels) = nodePattern.ast[(Variable, List[LabelName])]()
      CreateIndex.createFulltextNodeIndex(
        variable,
        labels,
        propertyList,
        indexName,
        existsDo,
        options
      )(pos(grandparent))
    } else {
      val (variable, relTypes) = ctx.multiRelTypeRelPattern().ast[(Variable, List[RelTypeName])]()
      CreateIndex.createFulltextRelationshipIndex(
        variable,
        relTypes,
        propertyList,
        indexName,
        existsDo,
        options
      )(pos(grandparent))
    }
  }

  final override def exitCreateVectorIndex(
    ctx: Cypher25Parser.CreateVectorIndexContext
  ): Unit = {
    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    val existsDo = ifExistsDo(grandparent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val indexName = astOpt[Expression](ctx.commandNameExpression())
    val nodePattern = ctx.multiLabelNodePattern()
    val isNode = nodePattern != null
    val propertyList = ctx.propertyList().ast[ArraySeq[Property]]().toList
    val additionalPropertiesList = astOpt[Seq[Property]](ctx.withProperties(), Seq.empty).toList
    ctx.ast = if (isNode) {
      val (variable, labels) = nodePattern.ast[(Variable, List[LabelName])]()
      CreateIndex.createVectorNodeIndex(
        variable,
        labels,
        propertyList,
        additionalPropertiesList,
        indexName,
        existsDo,
        options
      )(pos(grandparent))
    } else {
      val (variable, relTypes) = ctx.multiRelTypeRelPattern().ast[(Variable, List[RelTypeName])]()
      CreateIndex.createVectorRelationshipIndex(
        variable,
        relTypes,
        propertyList,
        additionalPropertiesList,
        indexName,
        existsDo,
        options
      )(pos(grandparent))
    }
  }

  def exitMultiLabelNodePattern(ctx: Cypher25Parser.MultiLabelNodePatternContext): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      astSeqPositioned[LabelName, String](ctx.symbolicNameString(), LabelName.apply).toList
    )
  }

  def exitMultiRelTypeRelPattern(ctx: Cypher25Parser.MultiRelTypeRelPatternContext): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      astSeqPositioned[RelTypeName, String](ctx.symbolicNameString(), RelTypeName.apply).toList
    )
  }

  def exitLookupIndexNodePattern(ctx: Cypher25Parser.LookupIndexNodePatternContext): Unit = {
    ctx.ast = ctx.variable().ast[Variable]()
  }

  def exitLookupIndexRelPattern(ctx: Cypher25Parser.LookupIndexRelPatternContext): Unit = {
    ctx.ast = ctx.variable().ast[Variable]()
  }

  final override def exitCreateLookupIndex(
    ctx: Cypher25Parser.CreateLookupIndexContext
  ): Unit = {
    val grandparent = ctx.getParent.getParent.asInstanceOf[CreateCommandContext]
    val existsDo = ifExistsDo(grandparent.REPLACE() != null, ctx.EXISTS() != null)
    val options = astOpt[Options](ctx.commandOptions(), NoOptions)
    val indexName = astOpt[Expression](ctx.commandNameExpression())
    val nodePattern = ctx.lookupIndexNodePattern()
    val isNode = nodePattern != null
    val functionName = ctx.symbolicNameString
    val functionPos = pos(functionName)
    val function = FunctionInvocation(
      FunctionName(functionName.ast[String]())(functionPos),
      distinct = false,
      IndexedSeq(ctx.variable().ast[Variable]()),
      maybeLocalFunction = None
    )(functionPos)
    val variable =
      if (isNode) nodePattern.ast[Variable]()
      else ctx.lookupIndexRelPattern().ast[Variable]()
    ctx.ast = CreateIndex.createLookupIndex(
      variable,
      isNode,
      function,
      indexName,
      existsDo,
      options
    )(pos(grandparent))
  }

  // Create admin command contexts (ordered as in parser file)

  final override def exitCreateRole(
    ctx: Cypher25Parser.CreateRoleContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val nameExpressions = ctx.commandNameExpression()
    val from =
      if (nameExpressions.size > 1) {
        AssertMacros3.checkOnlyWhenAssertionsAreEnabled(nameExpressions.size == 2)
        Some(nameExpressions.get(1).ast())
      } else
        None
    ctx.ast = CreateRole(
      nameExpressions.get(0).ast(),
      ctx.IMMUTABLE() != null,
      from,
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null)
    )(pos(parent))
  }

  final override def exitCreateUser(
    ctx: Cypher25Parser.CreateUserContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val nativePassAttributes = ctx.password().asScala.toList
      .map(_.ast[(Password, Option[PasswordChange])]())
      .foldLeft(List.empty[AuthAttribute]) { case (acc, (password, change)) => (acc :+ password) ++ change }
    val nativeAuthAttr = ctx.passwordChangeRequired().asScala.toList
      .map(c => PasswordChange(c.ast[Boolean]())(pos(c)))
      .foldLeft(nativePassAttributes) { case (acc, changeReq) => acc :+ changeReq }
      .sortBy(_.position)
    val nativeAuth =
      if (nativeAuthAttr.nonEmpty) Some(Auth(NATIVE_AUTH, nativeAuthAttr)(nativeAuthAttr.head.position)) else None
    val setAuth = ctx.setAuthClause().asScala.toList.map(_.ast[Auth]())
    val suspended = astOptFromList[Boolean](ctx.userStatus(), None)
    val homeDatabaseAction = astOptFromList[HomeDatabaseAction](ctx.homeDatabase(), None)
    val tags = astOptFromList[SetTags](ctx.userSetTagsClause(), None)
    ctx.ast = CreateUser(
      ctx.commandNameExpression().ast(),
      UserOptions(suspended, homeDatabaseAction),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null),
      setAuth,
      nativeAuth,
      tags
    )(pos(parent))
  }

  final override def exitCreateAuthRule(ctx: Cypher25Parser.CreateAuthRuleContext): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val setClauses = ctx.authRuleSetClause().asScala.toList
      .map(_.ast[AuthRuleSetClause])
      // Sorting the set clauses so the condition clause always comes before the enabled clause. To conform with the canonical syntax
      .sortBy {
        case _: AuthRuleCondition => false
        case _: AuthRuleEnabled   => true
      }

    ctx.ast = CreateAuthRule(
      ctx.commandNameExpression().ast(),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null),
      setClauses
    )(pos(parent))
  }

  final override def exitCreateCompositeDatabase(
    ctx: Cypher25Parser.CreateCompositeDatabaseContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    ctx.ast = CreateCompositeDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null),
      astOpt[Options](ctx.commandOptions(), NoOptions),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait()(InputPosition.NONE)),
      astOpt[CypherVersion](ctx.defaultLanguageSpecification())
    )(pos(parent))
  }

  final override def exitCreateDatabase(
    ctx: Cypher25Parser.CreateDatabaseContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    ctx.ast = CreateDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null),
      astOpt[Options](ctx.commandOptions(), NoOptions),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait()(InputPosition.NONE)),
      astOpt[Topology](ctx.topology()),
      astOpt[CypherVersion](ctx.defaultLanguageSpecification()),
      astOpt[ShardDefinition](ctx.shards())
    )(pos(parent))
  }

  final override def exitCreateReplicaDatabase(
    ctx: Cypher25Parser.CreateReplicaDatabaseContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    ctx.ast = CreateReplicaDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      ifExistsDo(parent.REPLACE() != null, ctx.EXISTS() != null),
      astOpt[Options](ctx.commandOptions(), NoOptions),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait()(InputPosition.NONE)),
      astOpt[Topology](ctx.topology()),
      astOpt[CypherVersion](ctx.defaultLanguageSpecification())
    )(pos(parent))
  }

  def exitShards(ctx: Cypher25Parser.ShardsContext): Unit = {
    ctx.ast = ShardDefinition(
      SignedDecimalIntegerLiteral(
        ctx.propertyShard().UNSIGNED_DECIMAL_INTEGER().getText
      )(rangePos(ctx)).value.intValue(),
      if (ctx.graphShard() != null && ctx.graphShard().topology() != null)
        Some(ctx.graphShard().topology().ast[Topology]())
      else None,
      if (ctx.propertyShard() != null && ctx.propertyShard().TOPOLOGY() != null)
        Some(ctx.propertyShard().uIntOrIntParameter().ast[Either[Int, Parameter]]())
      else None
    )
  }

  def exitGraphShard(ctx: Cypher25Parser.GraphShardContext): Unit = {}

  def exitPropertyShard(ctx: Cypher25Parser.PropertyShardContext): Unit = {}

  def exitTopology(ctx: Cypher25Parser.TopologyContext): Unit = {
    ctx.ast = Topology(
      astOptFromList[Either[Int, Parameter]](ctx.primaryTopology(), None),
      astOptFromList[Either[Int, Parameter]](ctx.secondaryTopology(), None)
    )
  }

  def exitRemoteTargetConnectionCredentials(ctx: Cypher25Parser.RemoteTargetConnectionCredentialsContext): Unit = {
    if (ctx.OIDC() != null) {
      ctx.ast = OidcCredentialForwarding()(pos(ctx))
    } else {
      ctx.ast = RemoteAliasStoredCredentials(
        ctx.commandNameExpression().ast(),
        ctx.passwordExpression().ast[Expression]()
      )(pos(ctx))
    }
  }

  final override def exitCreateAlias(
    ctx: Cypher25Parser.CreateAliasContext
  ): Unit = {
    val parent = ctx.getParent.asInstanceOf[CreateCommandContext]
    val aliasName = ctx.aliasName().ast[DatabaseName]()
    val targetName = ctx.aliasTargetName().ast[DatabaseName]()
    val ifNotExists = ctx.EXISTS() != null
    val properties =
      if (ctx.PROPERTIES() != null) {
        if (ctx.DRIVER() != null) Some(ctx.mapOrParameter(1).ast[Either[Map[String, Expression], Parameter]]())
        else Some(ctx.mapOrParameter(0).ast[Either[Map[String, Expression], Parameter]]())
      } else None

    ctx.ast = if (ctx.AT() == null) {
      CreateLocalDatabaseAlias(aliasName, targetName, ifExistsDo(parent.REPLACE() != null, ifNotExists), properties)(
        pos(parent)
      )
    } else {
      val driverSettings =
        if (ctx.DRIVER() != null) Some(ctx.mapOrParameter(0).ast[Either[Map[String, Expression], Parameter]]())
        else None
      CreateRemoteDatabaseAlias(
        aliasName,
        targetName,
        ifExistsDo(parent.REPLACE() != null, ifNotExists),
        ctx.stringOrParameter().ast[Either[String, Parameter]](),
        ctx.remoteTargetConnectionCredentials().ast[RemoteAliasCredentials](),
        driverSettings,
        properties,
        astOpt[CypherVersion](ctx.defaultLanguageSpecification())
      )(pos(parent))
    }
  }
}
