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

import org.antlr.v4.runtime.misc.Interval
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
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SetClause
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
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOpt
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeq
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeqPositioned
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.ctxChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.lastChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.Expression
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
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.IterableHasAsScala

trait StatementBuilder extends CypherParserListener {

  protected def exceptionFactory: CypherExceptionFactory

  final override def exitStatements(ctx: CypherParser.StatementsContext): Unit = {
    ctx.ast = Statements(astSeq(ctx.statement()))
  }

  final override def exitStatement(ctx: CypherParser.StatementContext): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx).ast
  }

  final override def exitRegularQuery(ctx: CypherParser.RegularQueryContext): Unit = {
    var result: Query = ctxChild(ctx, 0).ast[SingleQuery]()
    val size = ctx.children.size()
    if (size != 1) {
      var i = 1; var all = false; var p: InputPosition = null
      while (i < size) {
        ctx.children.get(i) match {
          case sqCtx: CypherParser.SingleQueryContext =>
            val rhs = sqCtx.ast[SingleQuery]()
            result = if (all) UnionAll(result, rhs)(p) else UnionDistinct(result, rhs)(p)
            all = false
          case node: TerminalNode => node.getSymbol.getType match {
              case CypherParser.ALL      => all = true
              case CypherParser.DISTINCT => all = false
              case CypherParser.UNION    => p = pos(node)
              case _                     => throw new IllegalStateException(s"Unexpected token $node")
            }
          case _ => throw new IllegalStateException(s"Unexpected ctx $ctx")
        }
        i += 1
      }
    }
    ctx.ast = result
  }

  final override def exitSingleQuery(ctx: CypherParser.SingleQueryContext): Unit = {
    ctx.ast = SingleQuery(astSeq[Clause](ctx.children))(pos(ctx))
  }

  final override def exitClause(ctx: CypherParser.ClauseContext): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitUseClause(ctx: CypherParser.UseClauseContext): Unit = {
    ctx.ast = UseGraph(ctx.graphReference().ast())(pos(ctx))
  }

  final override def exitGraphReference(ctx: CypherParser.GraphReferenceContext): Unit = {
    ctx.ast =
      if (ctx.graphReference() != null) ctx.graphReference().ast
      else if (ctx.functionInvocation() != null) GraphFunctionReference(ctx.functionInvocation().ast())(pos(ctx))
      else GraphDirectReference(CatalogName(ctx.symbolicAliasName().ast[ArraySeq[String]](): _*))(pos(ctx))
  }

  final override def exitSymbolicAliasName(ctx: CypherParser.SymbolicAliasNameContext): Unit = {
    ctx.ast = astSeq[String](ctx.symbolicNameString())
  }

  final override def exitReturnClause(ctx: CypherParser.ReturnClauseContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast[Return]().copy()(pos(ctx))
  }

  final override def exitFinishClause(ctx: CypherParser.FinishClauseContext): Unit = {
    ctx.ast = Finish()(pos(ctx))
  }

  final override def exitReturnBody(ctx: CypherParser.ReturnBodyContext): Unit = {
    val distinct = ctx.DISTINCT() != null
    val returnItems = ctx.returnItems().ast[ReturnItems]()
    val orderBy = if (ctx.ORDER() == null) None
    else {
      Some(
        OrderBy(
          astSeq(ctx.orderItem())
        )(pos(ctx.ORDER()))
      )
    }
    val skip = astOpt(ctx.skip())
    val limit = astOpt(ctx.limit())
    ctx.ast = Return(distinct, returnItems, orderBy, skip, limit)(pos(ctx))
  }

  final override def exitReturnItems(ctx: CypherParser.ReturnItemsContext): Unit = {
    ctx.ast = ReturnItems(
      includeExisting = ctx.TIMES() != null,
      items = astSeq(ctx.returnItem())
    )(pos(ctx))
  }

  final override def exitReturnItem(ctx: CypherParser.ReturnItemContext): Unit = {
    val position = pos(ctx)
    val expression = ctx.expression()
    val variable = ctx.variable()
    ctx.ast =
      if (variable != null) AliasedReturnItem(expression.ast(), variable.ast())(position)
      else {
        val interval = Interval.of(expression.start.getStartIndex, expression.stop.getStopIndex)
        UnaliasedReturnItem(expression.ast(), expression.start.getInputStream.getText(interval))(
          position
        )
      }
  }

  final override def exitOrderItem(ctx: CypherParser.OrderItemContext): Unit = {
    ctx.ast = if (ctx.children.size() == 1 || nodeChild(ctx, 1).getSymbol.getType == CypherParser.ASC) {
      AscSortItem(astChild(ctx, 0))(pos(ctx))
    } else {
      DescSortItem(astChild(ctx, 0))(pos(ctx))
    }
  }

  final override def exitSkip(ctx: CypherParser.SkipContext): Unit = ctx.ast = Skip(astChild(ctx, 1))(pos(ctx))

  final override def exitLimit(ctx: CypherParser.LimitContext): Unit = ctx.ast = Limit(astChild(ctx, 1))(pos(ctx))

  final override def exitWhereClause(ctx: CypherParser.WhereClauseContext): Unit = {
    ctx.ast = Where(astChild(ctx, 1))(pos(ctx))
  }

  final override def exitWithClause(
    ctx: CypherParser.WithClauseContext
  ): Unit = {
    val r = ctx.returnBody().ast[Return]()
    val where = astOpt(ctx.whereClause())
    ctx.ast = With(r.distinct, r.returnItems, r.orderBy, r.skip, r.limit, where)(pos(ctx))
  }

  final override def exitCreateClause(ctx: CypherParser.CreateClauseContext): Unit = {
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
    ctx: CypherParser.InsertClauseContext
  ): Unit = {
    val insertPatternList = ctx.insertPatternList()
    ctx.ast = Insert(
      Pattern.ForUpdate(insertPatternList.ast[ArraySeq[NonPrefixedPatternPart]]())(pos(insertPatternList))
    )(pos(ctx))
  }

  final override def exitSetClause(ctx: CypherParser.SetClauseContext): Unit = {
    ctx.ast = SetClause(astSeq(ctx.children, offset = 1, step = 2))(pos(ctx))
  }

  final override def exitSetItem(ctx: CypherParser.SetItemContext): Unit = {
    ctx.ast = ctx match {
      case _: CypherParser.SetPropContext =>
        SetPropertyItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast())(pos(ctx))
      case _: CypherParser.SetPropsContext =>
        SetExactPropertiesFromMapItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast())(pos(ctx))
      case _: CypherParser.AddPropContext =>
        SetIncludingPropertiesFromMapItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 2).ast())(pos(ctx))
      case _: CypherParser.SetLabelsContext =>
        SetLabelItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 1).ast(), containsIs = false)(pos(ctx))
      case _: CypherParser.SetLabelsIsContext =>
        SetLabelItem(ctxChild(ctx, 0).ast(), ctxChild(ctx, 1).ast(), containsIs = true)(pos(ctx))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitRemoveClause(
    ctx: CypherParser.RemoveClauseContext
  ): Unit = {
    ctx.ast = Remove(astSeq(ctx.removeItem()))(pos(ctx))
  }

  final override def exitRemoveItem(ctx: CypherParser.RemoveItemContext): Unit = {
    ctx.ast = ctx match {
      case r: CypherParser.RemovePropContext =>
        RemovePropertyItem(ctxChild(r, 0).ast())
      case r: CypherParser.RemoveLabelsContext =>
        RemoveLabelItem(ctxChild(r, 0).ast(), ctxChild(r, 1).ast(), containsIs = false)(pos(ctx))
      case r: CypherParser.RemoveLabelsIsContext =>
        RemoveLabelItem(ctxChild(r, 0).ast(), ctxChild(r, 1).ast(), containsIs = true)(pos(ctx))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitDeleteClause(
    ctx: CypherParser.DeleteClauseContext
  ): Unit = {
    val detach = ctx.DETACH() != null
    ctx.ast = Delete(astSeq(ctx.expression()), detach)(pos(ctx))
  }

  final override def exitMatchClause(ctx: CypherParser.MatchClauseContext): Unit = {
    val patternParts = ctx.patternList()
    val patternPartsWithSelector = patternParts.ast[ArraySeq[PatternPart]]().map {
      case part: PatternPartWithSelector => part
      case part: NonPrefixedPatternPart  => PatternPartWithSelector(PatternPart.AllPaths()(part.position), part)
      case other => throw new IllegalStateException(s"Expected pattern part but was ${other.getClass}")
    }

    val position = pos(ctx)
    val patternPos = if (ctx.OPTIONAL() != null) ctxChild(ctx, 2) else ctxChild(ctx, 1)

    ctx.ast = Match(
      optional = nodeChild(ctx, 0).getSymbol.getType == CypherParser.OPTIONAL,
      matchMode = astOpt(ctx.matchMode(), MatchMode.default(position)),
      pattern =
        Pattern.ForMatch(patternPartsWithSelector)(pos(patternPos)),
      hints = astSeq(ctx.hint()).toList,
      where = astOpt(ctx.whereClause())
    )(position)
  }

  final override def exitMatchMode(ctx: CypherParser.MatchModeContext): Unit = {
    val firstToken = nodeChild(ctx, 0).getSymbol
    ctx.ast = firstToken.getType match {
      case CypherParser.REPEATABLE => MatchMode.RepeatableElements()(pos(firstToken))
      case CypherParser.DIFFERENT  => MatchMode.DifferentRelationships()(pos(firstToken))
      case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token $firstToken)")
    }
  }

  final override def exitHint(ctx: CypherParser.HintContext): Unit = {
    val secondToken = nodeChild(ctx, 1).getSymbol
    ctx.ast = secondToken.getType match {
      case CypherParser.INDEX => indexHint(ctx, UsingAnyIndexType)
      case CypherParser.BTREE => throw new IllegalStateException("TODO") // TODO Correct error
      case CypherParser.TEXT  => indexHint(ctx, UsingTextIndexType)
      case CypherParser.RANGE => indexHint(ctx, UsingRangeIndexType)
      case CypherParser.POINT => indexHint(ctx, UsingPointIndexType)
      case CypherParser.JOIN  => UsingJoinHint(nonEmptyVariables(ctx.nonEmptyNameList()))(pos(ctx))
      case CypherParser.SCAN  => UsingScanHint(ctx.variable().ast(), ctx.labelOrRelType().ast())(pos(ctx))
    }
  }

  final override def exitNonEmptyNameList(ctx: CypherParser.NonEmptyNameListContext): Unit = {
    ctx.ast = astSeqPositioned[PropertyKeyName, String](ctx.symbolicNameString(), PropertyKeyName.apply)
  }

  private def nonEmptyPropertyKeyName(list: CypherParser.NonEmptyNameListContext): ArraySeq[PropertyKeyName] = {
    ArraySeq.from(list.symbolicNameString().asScala.collect {
      case s: CypherParser.SymbolicNameStringContext => PropertyKeyName(s.ast())(pos(s))
    })
  }

  private def nonEmptyVariables(list: CypherParser.NonEmptyNameListContext): NonEmptyList[Variable] = {
    NonEmptyList.from(
      list.children.asScala.collect {
        case nameCtx: CypherParser.SymbolicNameStringContext => Variable(nameCtx.ast())(pos(nameCtx))
      }
    )
  }

  private def indexHint(ctx: CypherParser.HintContext, hintType: UsingIndexHintType): UsingIndexHint = {
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

  final override def exitMergeClause(ctx: CypherParser.MergeClauseContext): Unit = {
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

  final override def exitMergeAction(ctx: CypherParser.MergeActionContext): Unit = {
    ctx.ast = nodeChild(ctx, 1).getSymbol.getType match {
      case CypherParser.MATCH  => OnMatch(ctxChild(ctx, 2).ast())(pos(ctx))
      case CypherParser.CREATE => OnCreate(ctxChild(ctx, 2).ast())(pos(ctx))
    }
  }

  final override def exitUnwindClause(
    ctx: CypherParser.UnwindClauseContext
  ): Unit = {
    ctx.ast = Unwind(ctxChild(ctx, 1).ast(), ctxChild(ctx, 3).ast())(pos(ctx))
  }

  final override def exitCallClause(
    ctx: CypherParser.CallClauseContext
  ): Unit = {
    val namespace = ctx.namespace().ast[Namespace]()
    val procedureName = ProcedureName(ctx.symbolicNameString().ast())(pos(ctx.symbolicNameString()))
    val procedureArguments =
      if (ctx.RPAREN() == null) None
      else
        Some(astSeq[Expression](ctx.procedureArgument()))
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

  final override def exitProcedureArgument(
    ctx: CypherParser.ProcedureArgumentContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitProcedureResultItem(
    ctx: CypherParser.ProcedureResultItemContext
  ): Unit = {
    val str = ctx.symbolicNameString().ast[String]()
    ctx.ast = if (ctx.variable() == null) ProcedureResultItem(Variable(str)(pos(ctx)))(pos(ctx))
    else {
      val v = ctx.variable().ast[Variable]()
      ProcedureResultItem(ProcedureOutput(str)(v.position), v)(pos(ctx))
    }
  }

  final override def exitLoadCSVClause(
    ctx: CypherParser.LoadCSVClauseContext
  ): Unit = {
    val withHeaders = ctx.HEADERS() != null
    ctx.ast = LoadCSV(withHeaders, ctx.expression().ast(), ctx.variable().ast(), astOpt(ctx.stringLiteral()))(pos(ctx))
  }

  final override def exitForeachClause(
    ctx: CypherParser.ForeachClauseContext
  ): Unit = {
    ctx.ast = Foreach(ctxChild(ctx, 2).ast(), ctxChild(ctx, 4).ast(), astSeq(ctx.clause()))(pos(ctx))
  }

  final override def exitSubqueryClause(
    ctx: CypherParser.SubqueryClauseContext
  ): Unit = {
    ctx.ast = SubqueryCall(ctxChild(ctx, 2).ast(), astOpt(ctx.subqueryInTransactionsParameters()))(pos(ctx))
  }

  final override def exitSubqueryInTransactionsParameters(
    ctx: CypherParser.SubqueryInTransactionsParametersContext
  ): Unit = {
    val batch = ctx.subqueryInTransactionsBatchParameters()
    val error = ctx.subqueryInTransactionsErrorParameters()
    val report = ctx.subqueryInTransactionsReportParameters()
    val batchParam = if (batch.isEmpty) None else Some(batch.get(0).ast[SubqueryCall.InTransactionsBatchParameters]())
    val concurrencyParam =
      if (ctx.CONCURRENT() != null)
        Some(InTransactionsConcurrencyParameters(astOpt[Expression](ctx.expression()))(pos(ctx.CONCURRENT().getSymbol)))
      else None
    val errorParam = if (error.isEmpty) None else Some(error.get(0).ast[SubqueryCall.InTransactionsErrorParameters]())
    val reportParam =
      if (report.isEmpty) None else Some(report.get(0).ast[SubqueryCall.InTransactionsReportParameters]())
    ctx.ast = SubqueryCall.InTransactionsParameters(batchParam, concurrencyParam, errorParam, reportParam)(
      pos(ctx.TRANSACTIONS().getSymbol)
    )
  }

  final override def exitSubqueryInTransactionsBatchParameters(
    ctx: CypherParser.SubqueryInTransactionsBatchParametersContext
  ): Unit = {
    ctx.ast = SubqueryCall.InTransactionsBatchParameters(ctxChild(ctx, 1).ast())(pos(ctx))
  }

  final override def exitSubqueryInTransactionsErrorParameters(
    ctx: CypherParser.SubqueryInTransactionsErrorParametersContext
  ): Unit = {
    val behaviour = nodeChild(ctx, 2).getSymbol.getType match {
      case CypherParser.CONTINUE => OnErrorContinue
      case CypherParser.BREAK    => OnErrorBreak
      case CypherParser.FAIL     => OnErrorFail
    }
    ctx.ast = SubqueryCall.InTransactionsErrorParameters(behaviour)(pos(ctx))
  }

  final override def exitSubqueryInTransactionsReportParameters(
    ctx: CypherParser.SubqueryInTransactionsReportParametersContext
  ): Unit = {
    ctx.ast = SubqueryCall.InTransactionsReportParameters(ctxChild(ctx, 3).ast())(pos(ctx))
  }

  final override def exitPeriodicCommitQueryHintFailure(
    ctx: CypherParser.PeriodicCommitQueryHintFailureContext
  ): Unit = {}

  final override def exitPatternList(ctx: CypherParser.PatternListContext): Unit = {
    ctx.ast = astSeq[PatternPart](ctx.pattern())
  }

  final override def exitPattern(ctx: CypherParser.PatternContext): Unit = {
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

  override def exitInsertPatternList(ctx: CypherParser.InsertPatternListContext): Unit = {
    ctx.ast = astSeq[PathPatternPart](ctx.insertPattern())
  }

  override def exitInsertPattern(ctx: CypherParser.InsertPatternContext): Unit = {
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
            case nCtx: CypherParser.InsertNodePatternContext =>
              val nodePattern = nCtx.ast[NodePattern]()
              if (relPattern != null) {
                part = RelationshipChain(part, relPattern, nodePattern)(p)
                relPattern = null
              } else {
                part = nodePattern
              }
            case relCtx: CypherParser.InsertRelationshipPatternContext =>
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
