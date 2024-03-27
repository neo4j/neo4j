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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.RemovedSyntax
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.ast.ValidSyntax
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.YieldOrWhere
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOpt
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeq
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.buildClauses
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.ctxChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener
import org.neo4j.cypher.internal.util.InputPosition

trait DdlShowBuilder extends CypherParserListener {

  final override def exitShowCommand(
    ctx: CypherParser.ShowCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast match {
      case ast: Seq[Clause @unchecked] => SingleQuery(ast)(pos(ctx))
      case ast                         => ast
    }
  }

  // YIELD Context and helpers

  private def decomposeYield(
    ctx: CypherParser.ShowCommandYieldContext
  ): (Option[Where], List[CommandResultItem], Boolean) = {
    if (ctx != null) {
      val yieldWhere = ctx.ast[Option[Either[(Yield, Option[Return]), Where]]]()
      yieldWhere match {
        case Some(Left(yieldPair)) =>
          val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldPair._1)
          (None, yieldedItems, yieldAll)
        case Some(Right(where)) => (Some(where), List[CommandResultItem](), false)
        case None               => (None, List[CommandResultItem](), false)
      }
    } else (None, List[CommandResultItem](), false)
  }

  private def getYieldAllAndYieldItems(yieldClause: Yield): (Boolean, List[CommandResultItem]) = {
    val yieldAll = Option(yieldClause).exists(_.returnItems.includeExisting)
    val yieldedItems = Option(yieldClause)
      .map(_.returnItems.items.map(item => {
        // yield is always parsed as `variable` with potentially `AS variable` after
        val variable = item.expression.asInstanceOf[LogicalVariable]
        val aliasedVariable: LogicalVariable = item.alias.getOrElse(variable)
        CommandResultItem(variable.name, aliasedVariable)(item.position)
      }).toList)
      .getOrElse(List.empty)
    (yieldAll, yieldedItems)
  }

  final override def exitYieldItem(
    ctx: CypherParser.YieldItemContext
  ): Unit = {
    val variables = ctx.variable()
    val returnItem = variables.get(0).ast[Variable]()
    ctx.ast = if (variables.size == 1) {
      UnaliasedReturnItem(returnItem, returnItem.name)(pos(ctx))
    } else {
      AliasedReturnItem(returnItem, variables.get(1).ast[Variable]())(pos(ctx))
    }
  }

  final override def exitYieldSkip(ctx: CypherParser.YieldSkipContext): Unit = {
    ctx.ast = if (ctx.SKIPROWS() != null) Some(Skip(ctx.signedIntegerLiteral().ast[Expression]())(pos(ctx))) else None
  }

  final override def exitYieldLimit(ctx: CypherParser.YieldLimitContext): Unit = {
    ctx.ast = if (ctx.LIMITROWS() != null) Some(Limit(ctx.signedIntegerLiteral().ast[Expression]())(pos(ctx))) else None
  }

  final override def exitYieldOrderBy(ctx: CypherParser.YieldOrderByContext): Unit = {
    ctx.ast =
      if (ctx.ORDER() != null) Some(OrderBy(astSeq[SortItem](ctx.orderItem()))(pos(ctx.ORDER().getSymbol))) else None
  }

  final override def exitYieldClause(
    ctx: CypherParser.YieldClauseContext
  ): Unit = {
    val returnItems = if (ctx.TIMES() != null)
      ReturnItems(includeExisting = true, Seq.empty)(pos(ctx.YIELD().getSymbol))
    else {
      ReturnItems(includeExisting = false, astSeq[ReturnItem](ctx.yieldItem()))(pos(ctx.yieldItem().get(0)))
    }
    val orderBy = ctx.yieldOrderBy().ast[Option[OrderBy]]()
    val skip = ctx.yieldSkip().ast[Option[Skip]]()
    val limit = ctx.yieldLimit().ast[Option[Limit]]()
    val where = astOpt[Where](ctx.whereClause())

    ctx.ast = Yield(returnItems, orderBy, skip, limit, where)(pos(ctx))
  }

  final override def exitShowBriefAndYield(
    ctx: CypherParser.ShowBriefAndYieldContext
  ): Unit = {
    val yieldClause = ctx.yieldClause()
    val brief = ctx.BRIEF() != null
    val verbose = ctx.VERBOSE() != null
    val where = astOpt[Where](ctx.whereClause())
    val (yieldAll, yieldedItems) =
      if (yieldClause != null) getYieldAllAndYieldItems(yieldClause.ast()) else (false, List[CommandResultItem]())
    ctx.ast = (brief, verbose, where, yieldedItems, yieldAll)
  }

  final override def exitShowCommandYield(
    ctx: CypherParser.ShowCommandYieldContext
  ): Unit = {
    val yieldClause = ctx.yieldClause()
    val whereClause = ctx.whereClause()
    ctx.ast = if (yieldClause != null) {
      Some(Left[(Yield, Option[Return]), Where]((
        yieldClause.ast[Yield](),
        astOpt[Return](ctx.returnClause())
      )))
    } else if (whereClause != null) {
      Some(Right[(Yield, Option[Return]), Where](whereClause.ast[Where]()))
    } else None
  }

  // Show Command Contexts

  final override def exitShowAliases(
    ctx: CypherParser.ShowAliasesContext
  ): Unit = {
    ctx.ast = ShowAliases(
      astOpt[DatabaseName](ctx.symbolicAliasNameOrParameter()),
      ctx.showCommandYield.ast[YieldOrWhere]()
    )(pos(ctx))
  }

  override def exitShowConstraintCommand(ctx: CypherParser.ShowConstraintCommandContext): Unit = {
    val parentPos = pos(ctx.getParent)
    ctx.ast = ctx match {
      case c: CypherParser.ShowConstraintMultiContext =>
        val entityType =
          if (c.NODE() != null) Node else if (c.RELATIONSHIP() != null || c.REL() != null) Rel else NoEntity
        val allowYieldType = c.constraintAllowYieldType()
        val constraintType = if (allowYieldType.PROPERTY() != null) {
          entityType match {
            case Node     => NodePropTypeConstraints
            case Rel      => RelPropTypeConstraints
            case NoEntity => PropTypeConstraints
          }
        } else if (allowYieldType.UNIQUENESS() != null) {
          entityType match {
            case Node     => NodeUniqueConstraints
            case Rel      => RelUniqueConstraints
            case NoEntity => UniqueConstraints
          }
        } else {
          entityType match {
            case Node     => NodeExistsConstraints(ValidSyntax)
            case Rel      => RelExistsConstraints(ValidSyntax)
            case NoEntity => ExistsConstraints(ValidSyntax)
          }
        }
        buildShowConstraintClauses(constraintType, c.showConstraintsAllowYield(), parentPos)

      case c: CypherParser.ShowConstraintUniqueContext =>
        val entityType = if (c.NODE() != null) Node else Rel
        val constraintType = entityType match {
          case Node     => NodeUniqueConstraints
          case Rel      => RelUniqueConstraints
          case NoEntity => throw new IllegalStateException("Invalid Constraint Type")
        }
        buildShowConstraintClauses(constraintType, c.showConstraintsAllowYield(), parentPos)

      case c: CypherParser.ShowConstraintKeyContext =>
        val entityType = if (c.RELATIONSHIP() != null || c.REL() != null) Rel else NoEntity
        val constraintType = entityType match {
          case Rel      => RelKeyConstraints
          case NoEntity => KeyConstraints
          case Node     => throw new IllegalStateException("Invalid Constraint Type")
        }
        buildShowConstraintClauses(constraintType, c.showConstraintsAllowYield(), parentPos)

      case c: CypherParser.ShowConstraintRelExistContext =>
        val constraintType = RelExistsConstraints(ValidSyntax)
        buildShowConstraintClauses(constraintType, c.showConstraintsAllowYield(), parentPos)

      case c: CypherParser.ShowConstraintOldExistsContext =>
        val entityType = if (c.NODE() != null) Node else if (c.RELATIONSHIP() != null) Rel else NoEntity
        val constraintType = entityType match {
          case Node     => NodeExistsConstraints(RemovedSyntax)
          case Rel      => RelExistsConstraints(RemovedSyntax)
          case NoEntity => ExistsConstraints(RemovedSyntax)
        }
        buildShowConstraintClauses(constraintType, c.showConstraintsAllowBrief(), parentPos)

      case c: CypherParser.ShowConstraintBriefAndYieldContext =>
        val constraintType = if (c.constraintBriefAndYieldType() != null) {
          nodeChild(c.constraintBriefAndYieldType(), 0).getSymbol.getType match {
            case CypherParser.ALL    => AllConstraints
            case CypherParser.UNIQUE => UniqueConstraints
            case CypherParser.EXIST  => ExistsConstraints(ValidSyntax)
            case CypherParser.NODE =>
              if (c.constraintBriefAndYieldType().EXIST() != null) {
                NodeExistsConstraints(ValidSyntax)
              } else NodeKeyConstraints
            case CypherParser.RELATIONSHIP =>
              RelExistsConstraints(ValidSyntax)
          }
        } else AllConstraints
        buildShowConstraintClauses(constraintType, c.showConstraintsAllowBriefAndYield(), parentPos)
      case _ => throw new IllegalStateException("Invalid Constraint Type")
    }
  }

  override def exitConstraintAllowYieldType(
    ctx: CypherParser.ConstraintAllowYieldTypeContext
  ): Unit = {}

  override def exitConstraintExistType(
    ctx: CypherParser.ConstraintExistTypeContext
  ): Unit = {}

  override def exitConstraintBriefAndYieldType(
    ctx: CypherParser.ConstraintBriefAndYieldTypeContext
  ): Unit = {}

  sealed private trait ConstraintEntity
  private case object Node extends ConstraintEntity
  private case object Rel extends ConstraintEntity
  private case object NoEntity extends ConstraintEntity

  private def buildShowConstraintClauses(
    constraintType: ShowConstraintType,
    opts: CypherParser.ShowConstraintsAllowBriefAndYieldContext,
    pos: InputPosition
  ): Seq[Clause] = {
    val (isBrief, isVerbose, where, yieldedItems, yieldAll) =
      opts.showBriefAndYield().ast[(Boolean, Boolean, Option[Where], List[CommandResultItem], Boolean)]()
    buildClauses(
      opts.showBriefAndYield().returnClause(),
      opts.showBriefAndYield().yieldClause(),
      opts.composableCommandClauses(),
      ShowConstraintsClause(
        constraintType,
        isBrief,
        isVerbose,
        where,
        yieldedItems,
        yieldAll
      )(pos)
    )

  }

  private def buildShowConstraintClauses(
    constraintType: ShowConstraintType,
    opts: CypherParser.ShowConstraintsAllowBriefContext,
    pos: InputPosition
  ): Seq[Clause] = {
    buildClauses(
      null,
      null,
      opts.composableCommandClauses(),
      ShowConstraintsClause(
        constraintType,
        opts.BRIEF() != null,
        opts.VERBOSE() != null,
        None,
        List[CommandResultItem](),
        yieldAll = false
      )(pos)
    )
  }

  private def buildShowConstraintClauses(
    constraintType: ShowConstraintType,
    opts: CypherParser.ShowConstraintsAllowYieldContext,
    pos: InputPosition
  ): Seq[Clause] = {
    val cmdYield = opts.showCommandYield()
    val (where, yieldedItems, yieldAll) = decomposeYield(cmdYield)
    buildClauses(
      cmdYield.returnClause(),
      cmdYield.yieldClause(),
      opts.composableCommandClauses(),
      ShowConstraintsClause(
        constraintType,
        brief = false,
        verbose = false,
        where,
        yieldedItems,
        yieldAll = yieldAll
      )(pos)
    )
  }

  final override def exitShowConstraintsAllowBriefAndYield(
    ctx: CypherParser.ShowConstraintsAllowBriefAndYieldContext
  ): Unit = {}

  final override def exitShowConstraintsAllowBrief(
    ctx: CypherParser.ShowConstraintsAllowBriefContext
  ): Unit = {}

  final override def exitShowConstraintsAllowYield(
    ctx: CypherParser.ShowConstraintsAllowYieldContext
  ): Unit = {}

  final override def exitShowCurrentUser(
    ctx: CypherParser.ShowCurrentUserContext
  ): Unit = {
    ctx.ast = ShowCurrentUser(ctx.showCommandYield().ast())(pos(ctx))
  }

  final override def exitShowDatabase(
    ctx: CypherParser.ShowDatabaseContext
  ): Unit = {
    val dbName = ctx.symbolicAliasNameOrParameter()
    val dbScope =
      if (dbName != null) SingleNamedDatabaseScope(dbName.ast[DatabaseName]())(pos(ctx))
      else if (ctx.HOME() != null) HomeDatabaseScope()(pos(ctx))
      else if (ctx.DEFAULT() != null) DefaultDatabaseScope()(pos(ctx))
      else AllDatabasesScope()(pos(ctx))
    ctx.ast = ShowDatabase(dbScope, ctx.showCommandYield().ast[YieldOrWhere])(pos(ctx))
  }

  final override def exitShowFunctions(
    ctx: CypherParser.ShowFunctionsContext
  ): Unit = {
    val (where, yieldedItems, yieldAll) = decomposeYield(ctx.showCommandYield())
    val parentPos = pos(ctx.getParent)
    ctx.ast = buildClauses(
      ctx.showCommandYield().returnClause(),
      ctx.showCommandYield().yieldClause(),
      ctx.composableCommandClauses(),
      ShowFunctionsClause(ctx.showFunctionsType.ast(), ctx.executableBy.ast(), where, yieldedItems, yieldAll)(parentPos)
    )
  }

  override def exitShowIndexCommand(
    ctx: CypherParser.ShowIndexCommandContext
  ): Unit = {
    val noBrief = ctx.showIndexesNoBrief()
    val parentPos = pos(ctx.getParent)
    ctx.ast = if (noBrief != null) {
      val indexType = nodeChild(ctx, 0).getSymbol.getType match {
        case CypherParser.FULLTEXT => FulltextIndexes
        case CypherParser.LOOKUP   => LookupIndexes
        case CypherParser.POINT    => PointIndexes
        case CypherParser.RANGE    => RangeIndexes
        case CypherParser.TEXT     => TextIndexes
        case CypherParser.VECTOR   => VectorIndexes
        case _                     => throw new IllegalStateException("Unexpected index type")
      }
      val (where, yieldedItems, yieldAll) = decomposeYield(noBrief.showCommandYield())

      buildClauses(
        noBrief.showCommandYield().returnClause(),
        noBrief.showCommandYield().yieldClause(),
        noBrief.composableCommandClauses(),
        ShowIndexesClause(indexType, brief = false, verbose = false, where, yieldedItems, yieldAll = yieldAll)(
          parentPos
        )
      )
    } else {
      val brief = ctx.showIndexesAllowBrief()
      val indexType = if (ctx.BTREE() != null) BtreeIndexes else AllIndexes
      val (isBrief, isVerbose, where, yieldedItems, yieldAll) =
        brief.showBriefAndYield().ast[(Boolean, Boolean, Option[Where], List[CommandResultItem], Boolean)]()

      buildClauses(
        brief.showBriefAndYield().returnClause(),
        brief.showBriefAndYield().yieldClause(),
        brief.composableCommandClauses(),
        ShowIndexesClause(indexType, isBrief, isVerbose, where, yieldedItems, yieldAll)(parentPos)
      )
    }
  }

  final override def exitShowIndexesAllowBrief(
    ctx: CypherParser.ShowIndexesAllowBriefContext
  ): Unit = {}

  final override def exitShowIndexesNoBrief(
    ctx: CypherParser.ShowIndexesNoBriefContext
  ): Unit = {}

  final override def exitShowPrivileges(
    ctx: CypherParser.ShowPrivilegesContext
  ): Unit = {
    ctx.ast = if (ctx.AS != null) {
      ShowPrivilegeCommands(
        ShowAllPrivileges()(pos(ctx)),
        ctx.REVOKE() != null,
        ctx.showCommandYield().ast[YieldOrWhere]()
      )(pos(ctx))
    } else {
      ShowPrivileges(ShowAllPrivileges()(pos(ctx)), ctx.showCommandYield().ast[YieldOrWhere]())(pos(ctx))
    }
  }

  final override def exitShowProcedures(
    ctx: CypherParser.ShowProceduresContext
  ): Unit = {
    val (where, yieldedItems, yieldAll) = decomposeYield(ctx.showCommandYield())
    val parentPos = pos(ctx.getParent)
    ctx.ast = buildClauses(
      ctx.showCommandYield().returnClause(),
      ctx.showCommandYield().yieldClause(),
      ctx.composableCommandClauses(),
      ShowProceduresClause(ctx.executableBy.ast(), where, yieldedItems, yieldAll)(parentPos)
    )
  }

  final override def exitShowRolePrivileges(
    ctx: CypherParser.ShowRolePrivilegesContext
  ): Unit = {
    ctx.ast = ShowPrivileges(
      ShowRolesPrivileges(ctx.symbolicNameOrStringParameterList().ast())(pos(ctx)),
      ctx.showCommandYield().ast[YieldOrWhere]()
    )(pos(ctx))
  }

  final override def exitShowRoles(
    ctx: CypherParser.ShowRolesContext
  ): Unit = {
    ctx.ast = ShowRoles(ctx.WITH() != null, ctx.POPULATED() == null, ctx.showCommandYield().ast())(pos(ctx))
  }

  final override def exitShowServers(
    ctx: CypherParser.ShowServersContext
  ): Unit = {
    ctx.ast = ShowServers(ctx.showCommandYield().ast[YieldOrWhere]())(pos(ctx))
  }

  final override def exitShowSettings(
    ctx: CypherParser.ShowSettingsContext
  ): Unit = {
    val (strOrExpr, where, yieldedItems, yieldAll) = ctx.namesAndClauses().ast[(
      Either[List[String], Expression],
      Option[Where],
      List[CommandResultItem],
      Boolean
    )]()
    val parentPos = pos(ctx.getParent)
    ctx.ast = buildClauses(
      ctx.namesAndClauses().showCommandYield().returnClause(),
      ctx.namesAndClauses().showCommandYield().yieldClause(),
      ctx.namesAndClauses().composableCommandClauses(),
      ShowSettingsClause(strOrExpr, where, yieldedItems, yieldAll)(parentPos)
    )
  }

  final override def exitShowSupportedPrivileges(
    ctx: CypherParser.ShowSupportedPrivilegesContext
  ): Unit = {
    ctx.ast = ShowSupportedPrivilegeCommand(ctx.showCommandYield().ast[YieldOrWhere]())(pos(ctx))
  }

  final override def exitShowTransactions(
    ctx: CypherParser.ShowTransactionsContext
  ): Unit = {
    val (strOrExpr, where, yieldedItems, yieldAll) = ctx.namesAndClauses().ast[(
      Either[List[String], Expression],
      Option[Where],
      List[CommandResultItem],
      Boolean
    )]()
    val parentPos = pos(ctx.getParent)
    ctx.ast = buildClauses(
      ctx.namesAndClauses().showCommandYield().returnClause(),
      ctx.namesAndClauses().showCommandYield().yieldClause(),
      ctx.namesAndClauses().composableCommandClauses(),
      ShowTransactionsClause(strOrExpr, where, yieldedItems, yieldAll)(parentPos)
    )
  }

  final override def exitTerminateTransactions(
    ctx: CypherParser.TerminateTransactionsContext
  ): Unit = {
    val (strOrExpr, where, yieldedItems, yieldAll) = ctx.namesAndClauses().ast[(
      Either[List[String], Expression],
      Option[Where],
      List[CommandResultItem],
      Boolean
    )]()
    val parentPos = pos(ctx.getParent)
    ctx.ast = buildClauses(
      ctx.namesAndClauses().showCommandYield().returnClause(),
      ctx.namesAndClauses().showCommandYield().yieldClause(),
      ctx.namesAndClauses().composableCommandClauses(),
      TerminateTransactionsClause(strOrExpr, yieldedItems, yieldAll, where.map(_.position))(parentPos)
    )
  }

  final override def exitShowUserPrivileges(
    ctx: CypherParser.ShowUserPrivilegesContext
  ): Unit = {
    ctx.ast = ShowPrivileges(
      ShowUserPrivileges(astOpt(ctx.symbolicNameOrStringParameterList()))(pos(ctx)),
      ctx.showCommandYield().ast[YieldOrWhere]()
    )(pos(ctx))
  }

  final override def exitShowFunctionsType(
    ctx: CypherParser.ShowFunctionsTypeContext
  ): Unit = {
    ctx.ast = if (ctx.BUILT() != null) {
      BuiltInFunctions
    } else if (ctx.USER() != null) {
      UserDefinedFunctions
    } else AllFunctions
  }

  final override def exitShowUsers(
    ctx: CypherParser.ShowUsersContext
  ): Unit = {
    ctx.ast = ShowUsers(ctx.showCommandYield().ast())(pos(ctx))
  }

  final override def exitExecutableBy(ctx: CypherParser.ExecutableByContext): Unit = {
    val name = ctx.symbolicNameString()
    ctx.ast =
      if (name != null) {
        Some(User(ctx.symbolicNameString().ast()))
      } else if (ctx.EXECUTABLE() != null) {
        Some(CurrentUser)
      } else None
  }

  override def exitNamesAndClauses(
    ctx: CypherParser.NamesAndClausesContext
  ): Unit = {
    val (where, yieldedItems, yieldAll) = decomposeYield(ctx.showCommandYield())
    ctx.ast = (
      astOpt[Either[List[String], Expression]](ctx.stringsOrExpression(), Left(List.empty)),
      where,
      yieldedItems,
      yieldAll
    )
  }

  final override def exitStringsOrExpression(
    ctx: CypherParser.StringsOrExpressionContext
  ): Unit = {
    val stringList = ctx.stringList()
    ctx.ast = if (stringList != null) {
      Left[List[String], Expression](
        astSeq[StringLiteral](stringList.stringLiteral()).map(_.value).toList
      )
    } else {
      Right[List[String], Expression](ctx.expression.ast())
    }
  }

  override def exitComposableShowCommandClauses(
    ctx: CypherParser.ComposableShowCommandClausesContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitComposableCommandClauses(
    ctx: CypherParser.ComposableCommandClausesContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

}
