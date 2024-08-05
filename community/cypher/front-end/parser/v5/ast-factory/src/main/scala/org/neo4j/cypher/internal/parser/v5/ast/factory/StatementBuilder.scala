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

import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
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
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.ProcedureOutput
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
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
import org.neo4j.cypher.internal.parser.common.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.parser.common.ast.factory.HintIndexType
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser
import org.neo4j.cypher.internal.parser.v5.Cypher5ParserListener
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstUtil.nonEmptyPropertyKeyName
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList

import java.util.stream.Collectors

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.IterableHasAsScala

trait StatementBuilder extends Cypher5ParserListener {

  protected def exceptionFactory: CypherExceptionFactory

  final override def exitStatements(ctx: Cypher5Parser.StatementsContext): Unit = {
    ctx.ast = Statements(astSeq(ctx.statement()))
  }

  final override def exitStatement(ctx: Cypher5Parser.StatementContext): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx).ast
  }

  final override def exitRegularQuery(ctx: Cypher5Parser.RegularQueryContext): Unit = {
    var result: Query = ctxChild(ctx, 0).ast[SingleQuery]()
    val size = ctx.children.size()
    if (size != 1) {
      var i = 1; var all = false; var p: InputPosition = null
      while (i < size) {
        ctx.children.get(i) match {
          case sqCtx: Cypher5Parser.SingleQueryContext =>
            val rhs = sqCtx.ast[SingleQuery]()
            result = if (all) UnionAll(result, rhs)(p) else UnionDistinct(result, rhs)(p)
            all = false
          case node: TerminalNode => node.getSymbol.getType match {
              case Cypher5Parser.ALL      => all = true
              case Cypher5Parser.DISTINCT => all = false
              case Cypher5Parser.UNION    => p = pos(node)
              case _                      => throw new IllegalStateException(s"Unexpected token $node")
            }
          case _ => throw new IllegalStateException(s"Unexpected ctx $ctx")
        }
        i += 1
      }
    }
    ctx.ast = result
  }

  final override def exitSingleQuery(ctx: Cypher5Parser.SingleQueryContext): Unit = {
    ctx.ast = SingleQuery(astSeq[Clause](ctx.children))(pos(ctx))
  }

  final override def exitClause(ctx: Cypher5Parser.ClauseContext): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitUseClause(ctx: Cypher5Parser.UseClauseContext): Unit = {
    ctx.ast = UseGraph(ctx.graphReference().ast())(pos(ctx))
  }

  final override def exitGraphReference(ctx: Cypher5Parser.GraphReferenceContext): Unit = {
    ctx.ast =
      if (ctx.graphReference() != null) ctx.graphReference().ast
      else if (ctx.functionInvocation() != null) GraphFunctionReference(ctx.functionInvocation().ast())(pos(ctx))
      else GraphDirectReference(CatalogName(ctx.symbolicAliasName().ast[ArraySeq[String]](): _*))(pos(ctx))
  }

  final override def exitSymbolicAliasName(ctx: Cypher5Parser.SymbolicAliasNameContext): Unit = {
    ctx.ast = astSeq[String](ctx.symbolicNameString())
  }

  final override def exitReturnClause(ctx: Cypher5Parser.ReturnClauseContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast[Return]().copy()(pos(ctx))
  }

  final override def exitFinishClause(ctx: Cypher5Parser.FinishClauseContext): Unit = {
    ctx.ast = Finish()(pos(ctx))
  }

  final override def exitReturnBody(ctx: Cypher5Parser.ReturnBodyContext): Unit = {
    ctx.ast = Return(
      ctx.DISTINCT() != null,
      ctx.returnItems().ast[ReturnItems](),
      astOpt(ctx.orderBy()),
      astOpt(ctx.skip()),
      astOpt(ctx.limit())
    )(pos(ctx))
  }

  final override def exitReturnItems(ctx: Cypher5Parser.ReturnItemsContext): Unit = {
    ctx.ast = ReturnItems(
      includeExisting = ctx.TIMES() != null,
      items = astSeq(ctx.returnItem())
    )(pos(ctx))
  }

  final override def exitReturnItem(ctx: Cypher5Parser.ReturnItemContext): Unit = {
    val position = pos(ctx)
    val expression = ctx.expression()
    val variable = ctx.variable()
    ctx.ast =
      if (variable != null) AliasedReturnItem(expression.ast(), variable.ast())(position)
      else UnaliasedReturnItem(expression.ast(), inputText(expression))(position)
  }

  final override def exitOrderItem(ctx: Cypher5Parser.OrderItemContext): Unit = {
    ctx.ast = if (ctx.children.size() == 1 || ctx.ascToken() != null) {
      AscSortItem(astChild(ctx, 0))(pos(ctx))
    } else {
      DescSortItem(astChild(ctx, 0))(pos(ctx))
    }
  }

  final override def exitSkip(ctx: Cypher5Parser.SkipContext): Unit = ctx.ast = Skip(astChild(ctx, 1))(pos(ctx))

  final override def exitLimit(ctx: Cypher5Parser.LimitContext): Unit = ctx.ast = Limit(astChild(ctx, 1))(pos(ctx))

  final override def exitWhereClause(ctx: Cypher5Parser.WhereClauseContext): Unit = {
    ctx.ast = Where(astChild(ctx, 1))(pos(ctx))
  }

  final override def exitWithClause(
    ctx: Cypher5Parser.WithClauseContext
  ): Unit = {
    val r = ctx.returnBody().ast[Return]()
    val where = astOpt(ctx.whereClause())
    ctx.ast = With(r.distinct, r.returnItems, r.orderBy, r.skip, r.limit, where)(pos(ctx))
  }

  final override def exitCreateClause(ctx: Cypher5Parser.CreateClauseContext): Unit = {
    val patternList = ctx.patternList()
    val nonPrefixedPatternPartList = patternList.ast[ArraySeq[PatternPart]]().map {
      case p: NonPrefixedPatternPart => p
      case p: PatternPartWithSelector => throw exceptionFactory.syntaxException(
          s"Path selectors such as `${p.selector.prettified}` cannot be used in a CREATE clause, but only in a MATCH clause.",
          pos(patternList)
        )
    }
    ctx.ast = Create(Pattern.ForUpdate(nonPrefixedPatternPartList)(pos(patternList)))(pos(ctx))
  }

  final override def exitInsertClause(
    ctx: Cypher5Parser.InsertClauseContext
  ): Unit = {
    val insertPatternList = ctx.insertPatternList()
    ctx.ast = Insert(
      Pattern.ForUpdate(insertPatternList.ast[ArraySeq[NonPrefixedPatternPart]]())(pos(insertPatternList))
    )(pos(ctx))
  }

  final override def exitSetClause(ctx: Cypher5Parser.SetClauseContext): Unit = {
    ctx.ast = SetClause(astSeq(ctx.children, offset = 1, step = 2))(pos(ctx))
  }

  final override def exitSetItem(ctx: Cypher5Parser.SetItemContext): Unit = {
    ctx.ast = ctx match {
      case _: Cypher5Parser.SetPropContext =>
        SetPropertyItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast())(pos(ctx))
      case _: Cypher5Parser.SetDynamicPropContext =>
        val dynamicProp = ctxChild(ctx, 0).ast[ContainerIndex]()
        SetDynamicPropertyItem(dynamicProp, ctxChild(ctx, 2).ast())(dynamicProp.position)
      case _: Cypher5Parser.SetPropsContext =>
        SetExactPropertiesFromMapItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast())(pos(ctx))
      case _: Cypher5Parser.AddPropContext =>
        SetIncludingPropertiesFromMapItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast())(pos(ctx))
      case _: Cypher5Parser.SetLabelsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        SetLabelItem(ctxChild(ctx, 0).ast(), labels, dynamicLabels, containsIs = false)(pos(ctx))
      case _: Cypher5Parser.SetLabelsIsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        SetLabelItem(ctxChild(ctx, 0).ast(), labels, dynamicLabels, containsIs = true)(pos(ctx))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitRemoveClause(
    ctx: Cypher5Parser.RemoveClauseContext
  ): Unit = {
    ctx.ast = Remove(astSeq(ctx.removeItem()))(pos(ctx))
  }

  final override def exitRemoveItem(ctx: Cypher5Parser.RemoveItemContext): Unit = {
    ctx.ast = ctx match {
      case r: Cypher5Parser.RemovePropContext =>
        RemovePropertyItem(ctxChild(r, 0).ast())
      case r: Cypher5Parser.RemoveDynamicPropContext =>
        RemoveDynamicPropertyItem(ctxChild(r, 0).ast())
      case r: Cypher5Parser.RemoveLabelsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        RemoveLabelItem(ctxChild(r, 0).ast(), labels, dynamicLabels, containsIs = false)(pos(ctx))
      case r: Cypher5Parser.RemoveLabelsIsContext =>
        val (labels, dynamicLabels) = astChild[(Seq[LabelName], Seq[Expression])](ctx, 1)
        RemoveLabelItem(ctxChild(r, 0).ast(), labels, dynamicLabels, containsIs = true)(pos(ctx))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitDeleteClause(
    ctx: Cypher5Parser.DeleteClauseContext
  ): Unit = {
    val detach = ctx.DETACH() != null
    ctx.ast = Delete(astSeq(ctx.expression()), detach)(pos(ctx))
  }

  final override def exitMatchClause(ctx: Cypher5Parser.MatchClauseContext): Unit = {
    val patternParts = ctx.patternList()
    val patternPartsWithSelector = patternParts.ast[ArraySeq[PatternPart]]().map {
      case part: PatternPartWithSelector => part
      case part: NonPrefixedPatternPart  => PatternPartWithSelector(PatternPart.AllPaths()(part.position), part)
      case other => throw new IllegalStateException(s"Expected pattern part but was ${other.getClass}")
    }

    val position = pos(ctx)
    val patternPos = if (ctx.OPTIONAL() != null) ctxChild(ctx, 2) else ctxChild(ctx, 1)

    ctx.ast = Match(
      optional = nodeChild(ctx, 0).getSymbol.getType == Cypher5Parser.OPTIONAL,
      matchMode = astOpt(ctx.matchMode(), MatchMode.default(position)),
      pattern =
        Pattern.ForMatch(patternPartsWithSelector)(pos(patternPos)),
      hints = astSeq(ctx.hint()).toList,
      where = astOpt(ctx.whereClause())
    )(position)
  }

  final override def exitMatchMode(ctx: Cypher5Parser.MatchModeContext): Unit = {
    val firstToken = nodeChild(ctx, 0).getSymbol
    ctx.ast = firstToken.getType match {
      case Cypher5Parser.REPEATABLE => MatchMode.RepeatableElements()(pos(firstToken))
      case Cypher5Parser.DIFFERENT  => MatchMode.DifferentRelationships()(pos(firstToken))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token $firstToken)")
    }
  }

  final override def exitHint(ctx: Cypher5Parser.HintContext): Unit = {
    val secondToken = nodeChild(ctx, 1).getSymbol
    ctx.ast = secondToken.getType match {
      case Cypher5Parser.INDEX => indexHint(ctx, UsingAnyIndexType)
      case Cypher5Parser.BTREE => throw exceptionFactory.syntaxException(
          ASTExceptionFactory.invalidHintIndexType(HintIndexType.BTREE),
          pos(secondToken)
        )
      case Cypher5Parser.TEXT  => indexHint(ctx, UsingTextIndexType)
      case Cypher5Parser.RANGE => indexHint(ctx, UsingRangeIndexType)
      case Cypher5Parser.POINT => indexHint(ctx, UsingPointIndexType)
      case Cypher5Parser.JOIN  => UsingJoinHint(nonEmptyVariables(ctx.nonEmptyNameList()))(pos(ctx))
      case Cypher5Parser.SCAN  => UsingScanHint(ctx.variable().ast(), ctx.labelOrRelType().ast())(pos(ctx))
      case _                   => throw new IllegalStateException(s"Unexpected token $secondToken")
    }
  }

  final override def exitNonEmptyNameList(ctx: Cypher5Parser.NonEmptyNameListContext): Unit = {
    ctx.ast = astSeqPositioned[PropertyKeyName, String](ctx.symbolicNameString(), PropertyKeyName.apply)
  }

  private def nonEmptyVariables(list: Cypher5Parser.NonEmptyNameListContext): NonEmptyList[Variable] = {
    NonEmptyList.from(
      list.children.asScala.collect {
        case nameCtx: Cypher5Parser.SymbolicNameStringContext => Variable(nameCtx.ast())(pos(nameCtx))
      }
    )
  }

  private def indexHint(ctx: Cypher5Parser.HintContext, hintType: UsingIndexHintType): UsingIndexHint = {
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

  final override def exitMergeClause(ctx: Cypher5Parser.MergeClauseContext): Unit = {
    val patternPart = ctxChild(ctx, 1)
    val nonPrefixedPatternPart = patternPart.ast[PatternPart]() match {
      case p: NonPrefixedPatternPart => p
      case p: PatternPartWithSelector => throw exceptionFactory.syntaxException(
          s"Path selectors such as `${p.selector.prettified}` cannot be used in a MERGE clause, but only in a MATCH clause.",
          pos(patternPart)
        )
    }

    ctx.ast = Merge(nonPrefixedPatternPart, astSeq(ctx.children, 2))(pos(ctx))
  }

  final override def exitMergeAction(ctx: Cypher5Parser.MergeActionContext): Unit = {
    ctx.ast = nodeChild(ctx, 1).getSymbol.getType match {
      case Cypher5Parser.MATCH  => OnMatch(ctxChild(ctx, 2).ast())(pos(ctx))
      case Cypher5Parser.CREATE => OnCreate(ctxChild(ctx, 2).ast())(pos(ctx))
    }
  }

  final override def exitUnwindClause(
    ctx: Cypher5Parser.UnwindClauseContext
  ): Unit = {
    ctx.ast = Unwind(ctxChild(ctx, 1).ast(), ctxChild(ctx, 3).ast())(pos(ctx))
  }

  final override def exitCallClause(
    ctx: Cypher5Parser.CallClauseContext
  ): Unit = {
    val (namespace, procedureName) = ctx.procedureName.ast[(Namespace, ProcedureName)]()
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
    ctx.ast = UnresolvedCall(namespace, procedureName, procedureArguments, procedureResults, yieldAll)(pos(ctx))
  }

  final override def exitProcedureName(
    ctx: Cypher5Parser.ProcedureNameContext
  ): Unit = {
    val namespace = ctx.namespace().ast[Namespace]()
    val procedureName = ProcedureName(ctx.symbolicNameString().ast())(pos(ctx.symbolicNameString()))
    ctx.ast = (namespace, procedureName)
  }

  final override def exitProcedureArgument(
    ctx: Cypher5Parser.ProcedureArgumentContext
  ): Unit = {
    ctx.ast = ctx.expression()
  }

  final override def exitProcedureResultItem(
    ctx: Cypher5Parser.ProcedureResultItemContext
  ): Unit = {
    val str = ctx.symbolicNameString().ast[String]()
    ctx.ast = if (ctx.variable() == null) ProcedureResultItem(Variable(str)(pos(ctx)))(pos(ctx))
    else {
      val v = ctx.variable().ast[Variable]()
      ProcedureResultItem(ProcedureOutput(str)(v.position), v)(pos(ctx))
    }
  }

  final override def exitLoadCSVClause(
    ctx: Cypher5Parser.LoadCSVClauseContext
  ): Unit = {
    val withHeaders = ctx.HEADERS() != null
    ctx.ast =
      LoadCSV.fromUrl(withHeaders, ctx.expression().ast(), ctx.variable().ast(), astOpt(ctx.stringLiteral()))(pos(ctx))
  }

  final override def exitForeachClause(
    ctx: Cypher5Parser.ForeachClauseContext
  ): Unit = {
    ctx.ast = Foreach(ctxChild(ctx, 2).ast(), ctxChild(ctx, 4).ast(), astSeq(ctx.clause()))(pos(ctx))
  }

  final override def exitSubqueryClause(
    ctx: Cypher5Parser.SubqueryClauseContext
  ): Unit = {
    val scope = ctx.subqueryScope()

    ctx.ast = if (scope != null) {
      val (isImportingAll, importedVariables) = scope.ast[(Boolean, Seq[Variable])]()
      ScopeClauseSubqueryCall(
        ctx.regularQuery().ast(),
        isImportingAll,
        importedVariables,
        astOpt(ctx.subqueryInTransactionsParameters())
      )(pos(ctx))
    } else {
      ImportingWithSubqueryCall(
        ctx.regularQuery().ast(),
        astOpt(ctx.subqueryInTransactionsParameters())
      )(pos(ctx))
    }
  }

  override def exitSubqueryScope(ctx: Cypher5Parser.SubqueryScopeContext): Unit = {
    ctx.ast = (ctx.TIMES() != null, astSeq[Variable](ctx.variable()))
  }

  final override def exitSubqueryInTransactionsParameters(
    ctx: Cypher5Parser.SubqueryInTransactionsParametersContext
  ): Unit = {
    val batch = ctx.subqueryInTransactionsBatchParameters()
    val error = ctx.subqueryInTransactionsErrorParameters()
    val report = ctx.subqueryInTransactionsReportParameters()
    val batchParam = if (batch.isEmpty) None else Some(batch.get(0).ast[SubqueryCall.InTransactionsBatchParameters]())
    val concurrencyParam =
      if (ctx.CONCURRENT() != null)
        Some(InTransactionsConcurrencyParameters(astOpt[Expression](ctx.expression()))(pos(ctx.IN().getSymbol)))
      else None
    val errorParam = if (error.isEmpty) None else Some(error.get(0).ast[SubqueryCall.InTransactionsErrorParameters]())
    val reportParam =
      if (report.isEmpty) None else Some(report.get(0).ast[SubqueryCall.InTransactionsReportParameters]())
    ctx.ast = SubqueryCall.InTransactionsParameters(batchParam, concurrencyParam, errorParam, reportParam)(
      pos(ctx.TRANSACTIONS().getSymbol)
    )
  }

  final override def exitSubqueryInTransactionsBatchParameters(
    ctx: Cypher5Parser.SubqueryInTransactionsBatchParametersContext
  ): Unit = {
    ctx.ast = SubqueryCall.InTransactionsBatchParameters(ctxChild(ctx, 1).ast())(pos(ctx))
  }

  final override def exitSubqueryInTransactionsErrorParameters(
    ctx: Cypher5Parser.SubqueryInTransactionsErrorParametersContext
  ): Unit = {
    val behaviour = nodeChild(ctx, 2).getSymbol.getType match {
      case Cypher5Parser.CONTINUE => OnErrorContinue
      case Cypher5Parser.BREAK    => OnErrorBreak
      case Cypher5Parser.FAIL     => OnErrorFail
    }
    ctx.ast = SubqueryCall.InTransactionsErrorParameters(behaviour)(pos(ctx))
  }

  final override def exitSubqueryInTransactionsReportParameters(
    ctx: Cypher5Parser.SubqueryInTransactionsReportParametersContext
  ): Unit = {
    ctx.ast = SubqueryCall.InTransactionsReportParameters(ctxChild(ctx, 3).ast())(pos(ctx))
  }

  final override def exitPeriodicCommitQueryHintFailure(
    ctx: Cypher5Parser.PeriodicCommitQueryHintFailureContext
  ): Unit = {}

  final override def exitPatternList(ctx: Cypher5Parser.PatternListContext): Unit = {
    ctx.ast = astSeq[PatternPart](ctx.pattern())
  }

  final override def exitPattern(ctx: Cypher5Parser.PatternContext): Unit = {
    val variable = ctx.variable()
    val selector = ctx.selector()
    var pattern = ctx.anonymousPattern().ast[PatternPart]()
    if (variable != null) {
      val astVariable = variable.ast[Variable]()
      pattern = NamedPatternPart(astVariable, pattern.asInstanceOf[AnonymousPatternPart])(astVariable.position)
    }
    if (selector != null) {
      pattern = PatternPartWithSelector(selector.ast(), pattern.asInstanceOf[NonPrefixedPatternPart])
    }

    ctx.ast = pattern
  }

  override def exitInsertPatternList(ctx: Cypher5Parser.InsertPatternListContext): Unit = {
    ctx.ast = astSeq[PathPatternPart](ctx.insertPattern())
  }

  override def exitInsertPattern(ctx: Cypher5Parser.InsertPatternContext): Unit = {
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
            case nCtx: Cypher5Parser.InsertNodePatternContext =>
              val nodePattern = nCtx.ast[NodePattern]()
              if (relPattern != null) {
                part = RelationshipChain(part, relPattern, nodePattern)(p)
                relPattern = null
              } else {
                part = nodePattern
              }
            case relCtx: Cypher5Parser.InsertRelationshipPatternContext =>
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
