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

import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.AdditiveProjection
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.DefaultWith
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.ExpandHintAll
import org.neo4j.cypher.internal.ast.ExpandHintInto
import org.neo4j.cypher.internal.ast.ExpandStep
import org.neo4j.cypher.internal.ast.ExplicitGroupingElements
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.GroupingAll
import org.neo4j.cypher.internal.ast.GroupingNone
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.LocalFieldSignature
import org.neo4j.cypher.internal.ast.LocalFunctionBody
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.NonOptional
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.Optional
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.ParsedAsLimit
import org.neo4j.cypher.internal.ast.ParsedAsOrderBy
import org.neo4j.cypher.internal.ast.ParsedAsSkip
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.StrictlyAdditiveProjection
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.UsingExpandHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOrScan
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingIndexHintType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.PathMode
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.Selector
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.util.Util.astChild
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeqPositioned
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.inputText
import org.neo4j.cypher.internal.parser.ast.util.Util.lastChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser
import org.neo4j.cypher.internal.parser.v25.Cypher25ParserListener
import org.neo4j.cypher.internal.parser.v25.ast.factory.Cypher25AstUtil.nonEmptyPropertyKeyName
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.ProcedureOutput
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.gqlstatus.GqlHelper

import java.util.stream.Collectors

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

trait StatementBuilder extends Cypher25ParserListener {

  protected def exceptionFactory: CypherExceptionFactory

  final override def exitStatements(ctx: Cypher25Parser.StatementsContext): Unit = {
    ctx.ast = Statements(astSeq(ctx.statement()))
  }

  final override def exitStatement(ctx: Cypher25Parser.StatementContext): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx).ast match {
      case sq @ SingleQuery(Seq(call: UnresolvedCall)) =>
        sq.copy(Seq(call.copy(isStandalone = true)(call.position)))(sq.position)
      case sq @ SingleQuery(Seq(use: UseGraph, call: UnresolvedCall)) =>
        sq.copy(Seq(use, call.copy(isStandalone = true)(call.position)))(sq.position)
      case q => q
    }
    pushLastClauseStartTokenToParent(ctx)
  }

  final override def exitQueryWithLocalDefinitions(ctx: Cypher25Parser.QueryWithLocalDefinitionsContext): Unit = {
    val definitions = astSeq[LocalCallableDefinition](ctx.localDefinition())
    val query = ctx.nextStatement().ast[Query]()
    ctx.ast =
      if (definitions.isEmpty) query
      else QueryWithLocalDefinitions(definitions, query)(pos(ctx))
    pushLastClauseStartTokenToParent(ctx)
  }

  final override def exitLocalDefinition(ctx: Cypher25Parser.LocalDefinitionContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast[LocalCallableDefinition]()
  }

  final override def exitLocalProcedureDefinition(ctx: Cypher25Parser.LocalProcedureDefinitionContext): Unit = {
    val name = ctxChild(ctx, 0).ast[ProcedureName]()
    val inputSignature = ctxChild(ctx, 1).ast[Seq[LocalFieldSignature]]()
    val outputSignature = astOpt[Seq[LocalFieldSignature]](ctx.outputType)
    val body = ctx.queryWithLocalDefinitions().ast[Query]()
    ctx.ast = LocalProcedureDefinition(name, inputSignature, outputSignature, body)(pos(ctx))
  }

  final override def exitLocalFunctionDefinition(ctx: Cypher25Parser.LocalFunctionDefinitionContext): Unit = {
    val name = ctxChild(ctx, 0).ast[FunctionName]()
    val inputSignature = ctxChild(ctx, 1).ast[Seq[LocalFieldSignature]]()
    val outputSignature = astOpt[CypherType](ctx.outputType)
    val body = ctx.localFunctionBody().ast[LocalFunctionBody]()
    ctx.ast = LocalFunctionDefinition(name, inputSignature, outputSignature, body)(pos(ctx))
  }

  final override def exitLocalInputFieldsSignature(ctx: Cypher25Parser.LocalInputFieldsSignatureContext): Unit = {
    ctx.ast = astSeq[LocalFieldSignature](ctx.localOptionalFieldSignature())
  }

  final override def exitLocalOutputFieldsSignature(ctx: Cypher25Parser.LocalOutputFieldsSignatureContext): Unit = {
    ctx.ast = astSeq[LocalFieldSignature](ctx.localMandatoryFieldSignature())
  }

  final override def exitLocalMandatoryFieldSignature(ctx: Cypher25Parser.LocalMandatoryFieldSignatureContext): Unit = {
    val fieldName = ctx.symbolicNameString().ast[String]()
    val typ = astOpt[CypherType](ctx.`type`())
    ctx.ast = LocalFieldSignature(fieldName, typ, None)(pos(ctx))
  }

  final override def exitLocalOptionalFieldSignature(ctx: Cypher25Parser.LocalOptionalFieldSignatureContext): Unit = {
    val fieldName = ctx.symbolicNameString().ast[String]()
    val default = astOpt[Expression](ctx.expression())
    val typ = astOpt[CypherType](ctx.`type`())
    ctx.ast = LocalFieldSignature(fieldName, typ, default)(pos(ctx))
  }

  final override def exitLocalFunctionBody(ctx: Cypher25Parser.LocalFunctionBodyContext): Unit = {
    ctx.ast = ctx match {
      case _: Cypher25Parser.ExpressionBodyContext =>
        ExpressionBody(ctxChild(ctx, 1).ast[Expression]())(pos(ctx))
      case _: Cypher25Parser.QueryBodyContext =>
        QueryBody(ctxChild(ctx, 1).ast[Query]())(pos(ctx))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitNextStatement(ctx: Cypher25Parser.NextStatementContext): Unit = {
    ctx.ast =
      if (ctx.NEXT().isEmpty) ctxChild(ctx, 0).ast[Query]()
      else NextStatement(astSeq[Query](ctx.regularQuery()))(pos(ctx))
    pushLastClauseStartTokenToParent(ctx)
  }

  final override def exitRegularQuery(ctx: Cypher25Parser.RegularQueryContext): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast[Query]()
    pushLastClauseStartTokenToParent(ctx)
  }

  override def exitUnion(ctx: Cypher25Parser.UnionContext): Unit = {
    var result: Query = ctxChild(ctx, 0).ast[PartQuery]()
    val size = ctx.children.size()
    var i = 1; var all = false; var p: InputPosition = null
    while (i < size) {
      ctx.children.get(i) match {
        case sqCtx: Cypher25Parser.SingleQueryContext =>
          val rhs = sqCtx.ast[PartQuery]()
          result = if (all) UnionAll(result, rhs)(p)
          else UnionDistinct(result, rhs)(p)
          all = false
        case node: TerminalNode => node.getSymbol.getType match {
            case Cypher25Parser.ALL      => all = true
            case Cypher25Parser.DISTINCT => all = false
            case Cypher25Parser.UNION    => p = pos(node)
            case _                       => throw new IllegalStateException(s"Unexpected token $node")
          }
        case _ => throw new IllegalStateException(s"Unexpected ctx $ctx")
      }
      i += 1
    }
    ctx.ast = result
    pushLastClauseStartTokenToParent(ctx)
  }

  override def exitWhen(ctx: Cypher25Parser.WhenContext): Unit = {
    ctx.ast = ConditionalQueryWhen(astSeq[ConditionalQueryBranch](ctx.whenBranch()), astOpt(ctx.elseBranch()))(pos(ctx))
  }

  override def exitWhenBranch(ctx: Cypher25Parser.WhenBranchContext): Unit = {
    ctx.ast =
      ConditionalQueryBranch(Some(ctx.expression().ast[Expression]()), ctx.singleQuery().ast[PartQuery])(pos(ctx))
  }

  override def exitElseBranch(ctx: Cypher25Parser.ElseBranchContext): Unit = {
    ctx.ast = ConditionalQueryBranch(None, ctx.singleQuery().ast[PartQuery])(pos(ctx))
  }

  final override def exitSingleQuery(ctx: Cypher25Parser.SingleQueryContext): Unit = {
    ctx.ast = if (ctx.queryWithLocalDefinitions() != null) {
      TopLevelBraces(ctx.queryWithLocalDefinitions().ast[Query], astOpt[UseGraph](ctx.useClause()))(pos(ctx))
    } else {
      SingleQuery(astSeq[Clause](ctx.children))(pos(ctx))
    }
    pushLastClauseStartTokenToParent(ctx)
  }

  private def pushLastClauseStartTokenToParent(ctx: AstRuleCtx): Unit = {
    ctx.parent match {
      case parentCtx: AstRuleCtx => parentCtx.lastClauseContext = ctx.lastClauseContext
      case _                     =>
    }
  }

  final override def exitClause(ctx: Cypher25Parser.ClauseContext): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
    ctx.parent match {
      case parentCtx: AstRuleCtx => parentCtx.lastClauseContext = ctx
      case _                     =>
    }
  }

  final override def exitUseClause(ctx: Cypher25Parser.UseClauseContext): Unit = {
    ctx.ast = UseGraph(ctx.graphReference().ast())(pos(ctx))
  }

  final override def exitGraphReference(ctx: Cypher25Parser.GraphReferenceContext): Unit = {
    ctx.ast =
      if (ctx.graphReference() != null) ctx.graphReference().ast
      else if (ctx.functionInvocation() != null)
        GraphFunctionReference(ctx.functionInvocation().ast(), resolveByDisplayName = true)(pos(ctx))
      else GraphDirectReference(CatalogName(true, ctx.symbolicAliasName().ast[ArraySeq[String]](): _*))(pos(ctx))
  }

  final override def exitSymbolicAliasName(ctx: Cypher25Parser.SymbolicAliasNameContext): Unit = {
    ctx.ast = astSeq[String](ctx.symbolicNameString())
  }

  final override def exitReturnClause(ctx: Cypher25Parser.ReturnClauseContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast[Return]().copy()(pos(ctx))
  }

  final override def exitFinishClause(ctx: Cypher25Parser.FinishClauseContext): Unit = {
    ctx.ast = Finish()(pos(ctx))
  }

  final override def exitReturnBody(ctx: Cypher25Parser.ReturnBodyContext): Unit = {
    ctx.ast = Return(
      ctx.DISTINCT() != null,
      ctx.returnItems().ast[ReturnItems](),
      astOpt(ctx.groupBy()),
      astOpt(ctx.orderBy()),
      astOpt(ctx.skip()),
      astOpt(ctx.limit())
    )(pos(ctx))
  }

  final override def exitReturnItems(ctx: Cypher25Parser.ReturnItemsContext): Unit = {
    ctx.ast = ReturnItems(
      if (ctx.TIMES() != null) AdditiveProjection else FreeProjection,
      items = astSeq(ctx.returnItem())
    )(pos(ctx))
  }

  final override def exitReturnItem(ctx: Cypher25Parser.ReturnItemContext): Unit = {
    val position = pos(ctx)
    val expression = ctx.expression()
    val variable = ctx.variable()
    ctx.ast =
      if (variable != null) AliasedReturnItem(expression.ast(), variable.ast())(position)
      else UnaliasedReturnItem(expression.ast(), inputText(expression))(position)
  }

  final override def exitGroupBy(ctx: Cypher25Parser.GroupByContext): Unit = {
    val groupingElements = if (ctx.LPAREN() != null) {
      GroupingNone()(pos(ctx.LPAREN()))
    } else if (ctx.ALL() != null) {
      GroupingAll()(pos(ctx.ALL()))
    } else {
      ExplicitGroupingElements(astSeq[Expression](ctx.expression()))(pos(ctx.expression(0)))
    }

    ctx.ast = GroupBy(groupingElements)(pos(ctx))
  }

  final override def exitOrderItem(ctx: Cypher25Parser.OrderItemContext): Unit = {
    ctx.ast = if (ctx.children.size() == 1 || ctx.ascToken() != null) {
      AscSortItem(astChild(ctx, 0))(pos(ctx))
    } else {
      DescSortItem(astChild(ctx, 0))(pos(ctx))
    }
  }

  final override def exitSkip(ctx: Cypher25Parser.SkipContext): Unit =
    ctx.ast = Skip(astChild(ctx, 1), ctx.SKIPROWS() != null)(pos(ctx))

  final override def exitLimit(ctx: Cypher25Parser.LimitContext): Unit = ctx.ast = Limit(astChild(ctx, 1))(pos(ctx))

  final override def exitWhereClause(ctx: Cypher25Parser.WhereClauseContext): Unit = {
    ctx.ast = Where(astChild(ctx, 1))(pos(ctx))
  }

  final override def exitSearchClause(ctx: Cypher25Parser.SearchClauseContext): Unit = {

    val indexName = ctx.indexSpecificationClause().ast[Expression]
    val maybeScoreClause = ctx.scoreClause()
    val maybeScore = if (maybeScoreClause == null) None else Some(maybeScoreClause.variable().ast())

    val maybeWhereClause = ctx.whereClause()
    val maybeWhere = Option(maybeWhereClause).map(_.ast[Where]())

    ctx.ast = Search(
      ctx.variable.ast(),
      maybeScore,
      indexName,
      ctx.forClause().ast(),
      maybeWhere,
      ctx.limit().ast()
    )(pos(ctx))
  }

  final override def exitIndexSpecificationClause(ctx: Cypher25Parser.IndexSpecificationClauseContext): Unit = {
    ctx.ast = ctx.commandNameExpression().ast()
  }

  final override def exitForClause(ctx: Cypher25Parser.ForClauseContext): Unit = {
    ctx.ast = ctx.expression().ast()
  }

  final override def exitScoreClause(ctx: Cypher25Parser.ScoreClauseContext): Unit = {
    ctx.ast = ctx.variable().ast()
  }

  final override def exitWithClause(
    ctx: Cypher25Parser.WithClauseContext
  ): Unit = {
    val r = ctx.returnBody().ast[Return]()
    val where = astOpt(ctx.whereClause())
    ctx.ast = With(r.distinct, r.returnItems, r.groupBy, r.orderBy, r.skip, r.limit, where)(pos(ctx))
  }

  final override def exitCreateClause(ctx: Cypher25Parser.CreateClauseContext): Unit = {
    val patternList = ctx.patternList()
    val nonPrefixedPatternPartList = patternList.ast[ArraySeq[PatternPart]]().map {
      case p: NonPrefixedPatternPart => p
      case p: PrefixedPatternPart =>
        val inputPosition = pos(patternList)
        val (selectorOrExplicitPathMode: String, errorMessage: String) = p.pathMode match {
          case pathMode: PathMode if !pathMode.implicitlyCreated =>
            (
              pathMode.prettified,
              s"Explicit path modes such as `${pathMode.prettified}` cannot be used in a CREATE clause, but only in a MATCH clause."
            )
          case _ =>
            (
              p.selector.prettified,
              s"Path selectors such as `${p.selector.prettified}` cannot be used in a CREATE clause, but only in a MATCH clause."
            )
        }
        val gql = GqlHelper.getGql42001_42I04(
          selectorOrExplicitPathMode,
          "CREATE",
          inputPosition.offset,
          inputPosition.line,
          inputPosition.column
        )
        throw exceptionFactory.syntaxException(
          gql,
          errorMessage,
          inputPosition
        )
    }
    ctx.ast = Create(Pattern.ForUpdate(nonPrefixedPatternPartList)(pos(patternList)))(pos(ctx))
  }

  final override def exitInsertClause(
    ctx: Cypher25Parser.InsertClauseContext
  ): Unit = {
    val insertPatternList = ctx.insertPatternList()
    ctx.ast = Insert(
      Pattern.ForUpdate(insertPatternList.ast[ArraySeq[NonPrefixedPatternPart]]())(pos(insertPatternList))
    )(pos(ctx))
  }

  final override def exitSetClause(ctx: Cypher25Parser.SetClauseContext): Unit = {
    ctx.ast = SetClause(astSeq(ctx.children, offset = 1, step = 2))(pos(ctx))
  }

  final override def exitSetItem(ctx: Cypher25Parser.SetItemContext): Unit = {
    ctx.ast = ctx match {
      case _: Cypher25Parser.SetPropsContext =>
        SetExactPropertiesFromMapItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast(), rhsMustBeMap = true)(pos(ctx))
      case _: Cypher25Parser.AddPropContext =>
        SetIncludingPropertiesFromMapItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast(), rhsMustBeMap = true)(pos(ctx))
      case _: Cypher25Parser.SetLabelsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        SetLabelItem(ctxChild(ctx, 0).ast(), labels, dynamicLabels, containsIs = false)(pos(ctx))
      case _: Cypher25Parser.SetLabelsIsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        SetLabelItem(ctxChild(ctx, 0).ast(), labels, dynamicLabels, containsIs = true)(pos(ctx))
      case _: Cypher25Parser.SetPropContext =>
        ctxChild(ctx, 0).ast[Expression]() match {
          case lp: LogicalProperty => SetPropertyItem(lp, ctxChild(ctx, 2).ast())(pos(ctx))
          case dp: ContainerIndex  => SetDynamicPropertyItem(dp, ctxChild(ctx, 2).ast())(pos(ctx))
          case ex =>
            val inputPosition = ex.position
            val gql = GqlHelper.getGql42001_42I06(
              "none property access expression",
              List("static property access", "dynamic property access").asJava,
              inputPosition.offset,
              inputPosition.line,
              inputPosition.column
            )
            throw exceptionFactory.syntaxException(
              gql,
              s"Invalid input none property access expression, expected: static property access or dynamic property access.",
              inputPosition
            )
        }
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitRemoveClause(
    ctx: Cypher25Parser.RemoveClauseContext
  ): Unit = {
    ctx.ast = Remove(astSeq(ctx.removeItem()))(pos(ctx))
  }

  final override def exitRemoveItem(ctx: Cypher25Parser.RemoveItemContext): Unit = {
    ctx.ast = ctx match {
      case _: Cypher25Parser.RemoveLabelsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        RemoveLabelItem(ctxChild(ctx, 0).ast(), labels, dynamicLabels, containsIs = false)(pos(ctx))
      case _: Cypher25Parser.RemoveLabelsIsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        RemoveLabelItem(ctxChild(ctx, 0).ast(), labels, dynamicLabels, containsIs = true)(pos(ctx))
      case _: Cypher25Parser.RemovePropContext =>
        ctxChild(ctx, 0).ast[Expression]() match {
          case lp: LogicalProperty => RemovePropertyItem(lp)(pos(ctx))
          case dp: ContainerIndex  => RemoveDynamicPropertyItem(dp)(pos(ctx))
          case ex =>
            val inputPosition = ex.position
            val gql = GqlHelper.getGql42001_42I06(
              "none property access expression",
              List("static property access", "dynamic property access").asJava,
              inputPosition.offset,
              inputPosition.line,
              inputPosition.column
            )
            throw exceptionFactory.syntaxException(
              gql,
              s"Invalid input none property access expression, expected: static property access or dynamic property access.",
              inputPosition
            )
        }
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitDeleteClause(
    ctx: Cypher25Parser.DeleteClauseContext
  ): Unit = {
    val detach = ctx.DETACH() != null
    ctx.ast = Delete(astSeq(ctx.expression()), detach)(pos(ctx))
  }

  final override def exitMatchClause(ctx: Cypher25Parser.MatchClauseContext): Unit = {
    val patternParts = ctx.patternList()
    val patternPartsWithSelector = patternParts.ast[ArraySeq[PatternPart]]().map {
      case part: PrefixedPatternPart    => part
      case part: NonPrefixedPatternPart => PrefixedPatternPart(part)(part.position)
    }

    val position = pos(ctx)
    val patternPos = if (ctx.OPTIONAL() != null) ctxChild(ctx, 2) else ctxChild(ctx, 1)

    ctx.ast = Match(
      optional = nodeChild(ctx, 0).getSymbol.getType == Cypher25Parser.OPTIONAL,
      matchMode = astOpt(ctx.matchMode(), MatchMode.default(position)),
      pattern =
        Pattern.ForMatch(patternPartsWithSelector)(pos(patternPos)),
      hints = astSeq(ctx.hint()).toList,
      where = astOpt(ctx.whereClause()),
      search = astOpt(ctx.searchClause())
    )(position)
  }

  final override def exitMatchMode(ctx: Cypher25Parser.MatchModeContext): Unit = {
    val firstToken = nodeChild(ctx, 0).getSymbol
    ctx.ast = firstToken.getType match {
      case Cypher25Parser.REPEATABLE => MatchMode.RepeatableElements()(pos(firstToken))
      case Cypher25Parser.DIFFERENT  => MatchMode.DifferentRelationships()(pos(firstToken))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token $firstToken)")
    }
  }

  final override def exitHint(ctx: Cypher25Parser.HintContext): Unit = {
    val secondToken = nodeChild(ctx, 1).getSymbol
    ctx.ast = secondToken.getType match {
      case Cypher25Parser.INDEX => indexHint(ctx, UsingAnyIndexType)
      case Cypher25Parser.TEXT  => indexHint(ctx, UsingTextIndexType)
      case Cypher25Parser.RANGE => indexHint(ctx, UsingRangeIndexType)
      case Cypher25Parser.POINT => indexHint(ctx, UsingPointIndexType)
      case Cypher25Parser.JOIN  => UsingJoinHint(nonEmptyVariables(ctx.nonEmptyNameList()))(pos(ctx))
      case Cypher25Parser.SCAN  => UsingScanHint(ctx.variable().ast(), ctx.labelOrRelType().ast())(pos(ctx))
      case Cypher25Parser.EXPAND =>
        val steps = NonEmptyList.from(astSeq[ExpandStep](ctx.expandHintStep()))
        UsingExpandHint(steps)(pos(ctx))
      case _ => throw new IllegalStateException(s"Unexpected token $secondToken")
    }
  }

  final override def exitExpandHintStep(ctx: Cypher25Parser.ExpandHintStepContext): Unit = {
    val from: Option[Variable] = astOpt[Variable](ctx.from)
    val to: Option[Variable] = astOpt[Variable](ctx.to)
    val via: Option[Variable] = astOpt[Variable](ctx.via)
    val mode =
      if (ctx.ALL() != null) Some(ExpandHintAll)
      else if (ctx.INTO() != null) Some(ExpandHintInto)
      else None
    ctx.ast = ExpandStep(from, to, via, mode)(pos(ctx))
  }

  final override def exitNonEmptyNameList(ctx: Cypher25Parser.NonEmptyNameListContext): Unit = {
    ctx.ast = astSeqPositioned[PropertyKeyName, String](ctx.symbolicNameString(), PropertyKeyName.apply)
  }

  private def nonEmptyVariables(list: Cypher25Parser.NonEmptyNameListContext): NonEmptyList[Variable] = {
    NonEmptyList.from(
      list.children.asScala.collect {
        case nameCtx: Cypher25Parser.SymbolicNameStringContext =>
          Variable(nameCtx.ast())(pos(nameCtx), Variable.isIsolatedDefault)
      }
    )
  }

  private def indexHint(ctx: Cypher25Parser.HintContext, hintType: UsingIndexHintType): UsingIndexHint = {
    checkOnlyWhenAssertionsAreEnabled(
      ctx.INDEX() != null && ctx.LPAREN() != null && ctx.LPAREN() != null && ctx.getChildCount <= 9
    )
    val spec = if (ctx.SEEK() != null) SeekOnly else SeekOrScan
    UsingIndexHint(
      ctx.variable().ast(),
      ctx.labelOrRelType().ast(),
      nonEmptyPropertyKeyName(ctx.nonEmptyNameList()),
      spec,
      hintType
    )(pos(ctx))
  }

  final override def exitMergeClause(ctx: Cypher25Parser.MergeClauseContext): Unit = {
    val patternPart = ctxChild(ctx, 1)
    val nonPrefixedPatternPart = patternPart.ast[PatternPart]() match {
      case p: NonPrefixedPatternPart => p
      case p: PrefixedPatternPart =>
        val inputPosition = pos(patternPart)
        val (selectorOrExplicitPathMode: String, errorMessage: String) = p.pathMode match {
          case pathMode: PathMode if !pathMode.implicitlyCreated =>
            (
              pathMode.prettified,
              s"Explicit path modes such as `${pathMode.prettified}` cannot be used in a MERGE clause, but only in a MATCH clause."
            )
          case _ => (
              p.selector.prettified,
              s"Path selectors such as `${p.selector.prettified}` cannot be used in a MERGE clause, but only in a MATCH clause."
            )
        }
        val gql = GqlHelper.getGql42001_42I04(
          selectorOrExplicitPathMode,
          "MERGE",
          inputPosition.offset,
          inputPosition.line,
          inputPosition.column
        );
        throw exceptionFactory.syntaxException(
          gql,
          errorMessage,
          inputPosition
        )
    }

    ctx.ast = Merge(nonPrefixedPatternPart, astSeq(ctx.children, 2))(pos(ctx))
  }

  final override def exitMergeAction(ctx: Cypher25Parser.MergeActionContext): Unit = {
    ctx.ast = nodeChild(ctx, 1).getSymbol.getType match {
      case Cypher25Parser.MATCH  => OnMatch(ctxChild(ctx, 2).ast())(pos(ctx))
      case Cypher25Parser.CREATE => OnCreate(ctxChild(ctx, 2).ast())(pos(ctx))
    }
  }

  final override def exitFilterClause(
    ctx: Cypher25Parser.FilterClauseContext
  ): Unit = {
    ctx.ast = With(
      distinct = false,
      ReturnItems(AdditiveProjection, Seq.empty)(pos(ctx)),
      None,
      None,
      None,
      None,
      Some(Where(ctx.expression().ast())(pos(ctx))),
      ParsedAsFilter
    )(pos(ctx))
  }

  final override def exitUnwindClause(
    ctx: Cypher25Parser.UnwindClauseContext
  ): Unit = {
    ctx.ast = Unwind(ctxChild(ctx, 1).ast(), ctxChild(ctx, 3).ast())(pos(ctx), useForInSyntax = false)
  }

  final override def exitForListClause(
    ctx: Cypher25Parser.ForListClauseContext
  ): Unit = {
    ctx.ast = Unwind(ctx.expression().ast(), ctx.variable().ast())(pos(ctx), useForInSyntax = true)
  }

  final override def exitLetClause(
    ctx: Cypher25Parser.LetClauseContext
  ): Unit = {
    ctx.ast = With(
      distinct = false,
      ReturnItems(
        StrictlyAdditiveProjection,
        astSeq(ctx.children, offset = 1, step = 2)
      )(pos(ctx)),
      None,
      None,
      None,
      None,
      None,
      ParsedAsLet
    )(pos(ctx))
  }

  final override def exitLetItem(
    ctx: Cypher25Parser.LetItemContext
  ): Unit = {
    ctx.ast = AliasedReturnItem(ctx.expression().ast(), ctx.variable().ast())(pos(ctx))
  }

  final override def exitCallClause(
    ctx: Cypher25Parser.CallClauseContext
  ): Unit = {
    val procedureName = ctx.procedureName.ast[ProcedureName]()
    val procedureArguments =
      if (ctx.RPAREN() == null) None
      else
        Some(
          astSeq[Expression](ctx.procedureArgument.stream().map(arg => arg.expression()).collect(Collectors.toList()))
        )
    val yieldAll = ctx.TIMES() != null
    val procedureResults = {
      if (ctx.YIELD() == null || yieldAll) None
      else {
        val procRes = astSeq[ProcedureResultItem](ctx.procedureResultItem())
        Some(ProcedureResult(procRes, astOpt(ctx.whereClause()))(pos(ctx.YIELD().getSymbol)))
      }
    }
    ctx.ast = UnresolvedCall(
      procedureName,
      procedureArguments,
      procedureResults,
      isStandalone = false,
      yieldAll,
      if (ctx.OPTIONAL() != null) Optional else NonOptional
    )(pos(ctx))
  }

  final override def exitProcedureName(
    ctx: Cypher25Parser.ProcedureNameContext
  ): Unit = {
    val namespace = ctx.namespace().ast[Namespace]()
    val procedureName = ctx.symbolicNameString().ast[String]()
    ctx.ast = ProcedureName(namespace, procedureName)(pos(ctx.namespace()))
  }

  final override def exitProcedureArgument(
    ctx: Cypher25Parser.ProcedureArgumentContext
  ): Unit = {
    ctx.ast = ctx.expression()
  }

  final override def exitProcedureResultItem(
    ctx: Cypher25Parser.ProcedureResultItemContext
  ): Unit = {
    val str = ctx.yieldItemName.ast[Variable]().name
    ctx.ast = if (ctx.yieldItemAlias == null)
      ProcedureResultItem(Variable(str)(pos(ctx), Variable.isIsolatedDefault))(pos(ctx))
    else {
      val v = ctx.yieldItemAlias.ast[Variable]()
      ProcedureResultItem(ProcedureOutput(str)(v.position), v)(pos(ctx))
    }
  }

  final override def exitLoadCSVClause(
    ctx: Cypher25Parser.LoadCSVClauseContext
  ): Unit = {
    val withHeaders = ctx.HEADERS() != null
    ctx.ast =
      LoadCSV.fromUrl(withHeaders, ctx.expression().ast(), ctx.variable().ast(), astOpt(ctx.stringLiteral()))(pos(ctx))
  }

  final override def exitForeachClause(
    ctx: Cypher25Parser.ForeachClauseContext
  ): Unit = {
    ctx.ast = Foreach(ctxChild(ctx, 2).ast(), ctxChild(ctx, 4).ast(), astSeq(ctx.clause()))(pos(ctx))
  }

  final override def exitSubqueryClause(
    ctx: Cypher25Parser.SubqueryClauseContext
  ): Unit = {
    val scope = ctx.subqueryScope()

    ctx.ast = if (scope != null) {
      val (isImportingAll, importedVariables) = scope.ast[(Boolean, Seq[Variable])]()
      ScopeClauseSubqueryCall(
        ctx.queryWithLocalDefinitions().ast(),
        isImportingAll,
        importedVariables,
        astOpt(ctx.subqueryInTransactionsParameters()),
        ctx.OPTIONAL() != null
      )(pos(ctx))
    } else {
      ImportingWithSubqueryCall(
        ctx.queryWithLocalDefinitions().ast(),
        astOpt(ctx.subqueryInTransactionsParameters()),
        ctx.OPTIONAL() != null
      )(pos(ctx))
    }
  }

  override def exitSubqueryScope(ctx: Cypher25Parser.SubqueryScopeContext): Unit = {
    ctx.ast = (ctx.TIMES() != null, astSeq[Variable](ctx.variable()))
  }

  final override def exitSubqueryInTransactionsParameters(
    ctx: Cypher25Parser.SubqueryInTransactionsParametersContext
  ): Unit = {
    val batch = ctx.subqueryInTransactionsBatchParameters()
    val error = ctx.subqueryInTransactionsErrorParameters()
    val report = ctx.subqueryInTransactionsReportParameters()
    val disjointBy = ctx.subqueryInTransactionsDisjointByParameters()
    val batchParam = if (batch.isEmpty) None else Some(batch.get(0).ast[SubqueryCall.InTransactionsBatchParameters]())
    val concurrencyParam =
      if (ctx.CONCURRENT() != null)
        Some(InTransactionsConcurrencyParameters(astOpt[Expression](ctx.expression()))(pos(ctx.IN().getSymbol)))
      else None
    val errorParam = if (error.isEmpty) None else Some(error.get(0).ast[SubqueryCall.InTransactionsErrorParameters]())
    val reportParam =
      if (report.isEmpty) None else Some(report.get(0).ast[SubqueryCall.InTransactionsReportParameters]())
    val disjointByParam =
      if (disjointBy.isEmpty) None
      else Some(disjointBy.get(0).ast[SubqueryCall.InTransactionsDisjointByParameters]())
    ctx.ast =
      SubqueryCall.InTransactionsParameters(batchParam, concurrencyParam, errorParam, reportParam, disjointByParam)(
        pos(ctx.TRANSACTIONS().getSymbol)
      )
  }

  final override def exitSubqueryInTransactionsBatchParameters(
    ctx: Cypher25Parser.SubqueryInTransactionsBatchParametersContext
  ): Unit = {
    ctx.ast = SubqueryCall.InTransactionsBatchParameters(ctxChild(ctx, 1).ast())(pos(ctx))
  }

  final override def exitSubqueryInTransactionsDisjointByParameters(
    ctx: Cypher25Parser.SubqueryInTransactionsDisjointByParametersContext
  ): Unit = {
    val mode: SubqueryCall.InTransactionsDisjointByMode =
      if (ctx.AUTO() != null) {
        InTransactionsDisjointByMode.DisjointByAuto
      } else if (ctx.NONE() != null) {
        InTransactionsDisjointByMode.DisjointByNone
      } else {
        InTransactionsDisjointByMode.DisjointByExpressions(
          ctx.subqueryInTransactionsDisjointByExpressions().ast[Seq[Expression]]()
        )
      }
    ctx.ast = SubqueryCall.InTransactionsDisjointByParameters(mode)(pos(ctx))
  }

  final override def exitSubqueryInTransactionsDisjointByExpressions(
    ctx: Cypher25Parser.SubqueryInTransactionsDisjointByExpressionsContext
  ): Unit = {
    ctx.ast = astSeq[Expression](ctx.expression())
  }

  final override def exitSubqueryInTransactionsErrorParameters(
    ctx: Cypher25Parser.SubqueryInTransactionsErrorParametersContext
  ): Unit = {
    val behaviour = nodeChild(ctx, 2).getSymbol.getType match {
      case Cypher25Parser.RETRY =>
        if (ctx.THEN() != null) {
          if (ctx.CONTINUE() != null) {
            OnErrorRetryThenContinue
          } else if (ctx.BREAK() != null) {
            OnErrorRetryThenBreak
          } else {
            OnErrorRetryThenFail
          }
        } else {
          OnErrorRetryThenFail
        }
      case Cypher25Parser.CONTINUE => OnErrorContinue
      case Cypher25Parser.BREAK    => OnErrorBreak
      case Cypher25Parser.FAIL     => OnErrorFail
    }

    ctx.ast = SubqueryCall.InTransactionsErrorParameters(
      behaviour,
      astOpt(ctx.subqueryInTransactionsRetryParameters())
    )(pos(ctx))
  }

  final override def exitSubqueryInTransactionsRetryParameters(
    ctx: Cypher25Parser.SubqueryInTransactionsRetryParametersContext
  ): Unit = {
    ctx.ast = SubqueryCall.InTransactionsRetryParameters(astOpt(ctx.expression()))(pos(ctx))
  }

  final override def exitSubqueryInTransactionsReportParameters(
    ctx: Cypher25Parser.SubqueryInTransactionsReportParametersContext
  ): Unit = {
    ctx.ast = SubqueryCall.InTransactionsReportParameters(ctxChild(ctx, 3).ast())(pos(ctx))
  }

  override def exitOrderBySkipLimitClause(ctx: Cypher25Parser.OrderBySkipLimitClauseContext): Unit = {
    val orderBy = astOpt[OrderBy](ctx.orderBy())
    val skip = astOpt[Skip](ctx.skip())
    val limit = astOpt[Limit](ctx.limit())
    ctx.ast = With(
      distinct = false,
      ReturnItems(AdditiveProjection, Seq.empty)(pos(ctx)),
      None,
      orderBy,
      skip,
      limit,
      where = None,
      withType = orderBy.orElse(skip).orElse(limit).map {
        case _: OrderBy => ParsedAsOrderBy
        case _: Skip    => ParsedAsSkip
        case _: Limit   => ParsedAsLimit
        case _          => DefaultWith // to make the match exhaustive and the compiler happy
      }.getOrElse(DefaultWith)
    )(pos(ctx))
  }

  final override def exitPatternList(ctx: Cypher25Parser.PatternListContext): Unit = {
    ctx.ast = astSeq[PatternPart](ctx.pattern())
  }

  final override def exitPattern(ctx: Cypher25Parser.PatternContext): Unit = {
    val variable = ctx.variable()
    var pattern = ctx.anonymousPattern().ast[PatternPart]()
    if (variable != null) {
      val astVariable = variable.ast[Variable]()
      pattern = NamedPatternPart(astVariable, pattern.asInstanceOf[AnonymousPatternPart])(astVariable.position)
    }
    val pathPatternPrefix = ctx.pathPatternPrefix()
    if (pathPatternPrefix != null) {
      val (selectorAst, pathModeAst) = pathPatternPrefix.ast[(Selector, PathMode)]()
      pattern = PrefixedPatternPart(selectorAst, pathModeAst, pattern.asInstanceOf[NonPrefixedPatternPart])
    }
    ctx.ast = pattern
  }

  override def exitInsertPatternList(ctx: Cypher25Parser.InsertPatternListContext): Unit = {
    ctx.ast = astSeq[PathPatternPart](ctx.insertPattern())
  }

  override def exitInsertPattern(ctx: Cypher25Parser.InsertPatternContext): Unit = {
    if (ctx.EQ == null) {
      val size = ctx.children.size()
      if (size == 1) {
        ctx.ast = PathPatternPart(ctxChild(ctx, 0).ast[NodePattern]())
      } else {
        val p = pos(ctx)
        var part: SimplePattern = null
        var relPattern: RelationshipPattern = null
        var i = 0
        while (i < size) {
          ctx.children.get(i) match {
            case nCtx: Cypher25Parser.InsertNodePatternContext =>
              val nodePattern = nCtx.ast[NodePattern]()
              if (relPattern != null) {
                part = RelationshipChain(part, relPattern, nodePattern)(p)
                relPattern = null
              } else {
                part = nodePattern
              }
            case relCtx: Cypher25Parser.InsertRelationshipPatternContext =>
              relPattern = relCtx.ast[RelationshipPattern]()
            case other => throw new IllegalStateException(s"Unexpected child $other")
          }
          i += 1
        }
        ctx.ast = PathPatternPart(part)
      }
    } else {
      // Case is invalid, caught but SyntaxChecker.scala
      ctx.ast = null
    }
  }
}
