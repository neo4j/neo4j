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

import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener

trait ExpressionBuilder extends CypherParserListener {

  final override def exitProcedureName(
    ctx: CypherParser.ProcedureNameContext
  ): Unit = {}

  final override def exitProcedureArgument(
    ctx: CypherParser.ProcedureArgumentContext
  ): Unit = {}

  final override def exitProcedureResultItem(
    ctx: CypherParser.ProcedureResultItemContext
  ): Unit = {}

  final override def exitSubqueryInTransactionsParameters(
    ctx: CypherParser.SubqueryInTransactionsParametersContext
  ): Unit = {}

  final override def exitSubqueryInTransactionsBatchParameters(
    ctx: CypherParser.SubqueryInTransactionsBatchParametersContext
  ): Unit = {}

  final override def exitSubqueryInTransactionsErrorParameters(
    ctx: CypherParser.SubqueryInTransactionsErrorParametersContext
  ): Unit = {}

  final override def exitSubqueryInTransactionsReportParameters(
    ctx: CypherParser.SubqueryInTransactionsReportParametersContext
  ): Unit = {}

  final override def exitPatternList(
    ctx: CypherParser.PatternListContext
  ): Unit = {}

  final override def exitPattern(
    ctx: CypherParser.PatternContext
  ): Unit = {}

  final override def exitQuantifier(
    ctx: CypherParser.QuantifierContext
  ): Unit = {}

  final override def exitAnonymousPattern(
    ctx: CypherParser.AnonymousPatternContext
  ): Unit = {}

  final override def exitShortestPathPattern(
    ctx: CypherParser.ShortestPathPatternContext
  ): Unit = {}

  final override def exitMaybeQuantifiedRelationshipPattern(
    ctx: CypherParser.MaybeQuantifiedRelationshipPatternContext
  ): Unit = {}

  final override def exitPatternElement(
    ctx: CypherParser.PatternElementContext
  ): Unit = {}

  final override def exitPathPatternAtoms(
    ctx: CypherParser.PathPatternAtomsContext
  ): Unit = {}

  final override def exitSelector(
    ctx: CypherParser.SelectorContext
  ): Unit = {}

  final override def exitPathPatternNonEmpty(
    ctx: CypherParser.PathPatternNonEmptyContext
  ): Unit = {}

  final override def exitNodePattern(
    ctx: CypherParser.NodePatternContext
  ): Unit = {}

  final override def exitParenthesizedPath(
    ctx: CypherParser.ParenthesizedPathContext
  ): Unit = {}

  final override def exitProperties(
    ctx: CypherParser.PropertiesContext
  ): Unit = {}

  final override def exitRelationshipPattern(
    ctx: CypherParser.RelationshipPatternContext
  ): Unit = {}

  final override def exitLeftArrow(
    ctx: CypherParser.LeftArrowContext
  ): Unit = {}

  final override def exitArrowLine(
    ctx: CypherParser.ArrowLineContext
  ): Unit = {}

  final override def exitRightArrow(
    ctx: CypherParser.RightArrowContext
  ): Unit = {}

  final override def exitPathLength(
    ctx: CypherParser.PathLengthContext
  ): Unit = {}

  final override def exitExpression(
    ctx: CypherParser.ExpressionContext
  ): Unit = {}

  final override def exitExpression12(
    ctx: CypherParser.Expression12Context
  ): Unit = {}

  final override def exitExpression11(
    ctx: CypherParser.Expression11Context
  ): Unit = {}

  final override def exitExpression10(
    ctx: CypherParser.Expression10Context
  ): Unit = {}

  final override def exitExpression9(
    ctx: CypherParser.Expression9Context
  ): Unit = {}

  final override def exitExpression8(
    ctx: CypherParser.Expression8Context
  ): Unit = {}

  final override def exitExpression7(
    ctx: CypherParser.Expression7Context
  ): Unit = {}

  final override def exitComparisonExpression6(
    ctx: CypherParser.ComparisonExpression6Context
  ): Unit = {}

  final override def exitExpression6(
    ctx: CypherParser.Expression6Context
  ): Unit = {}

  final override def exitExpression5(
    ctx: CypherParser.Expression5Context
  ): Unit = {}

  final override def exitExpression4(
    ctx: CypherParser.Expression4Context
  ): Unit = {}

  final override def exitExpression3(
    ctx: CypherParser.Expression3Context
  ): Unit = {}

  final override def exitExpression2(
    ctx: CypherParser.Expression2Context
  ): Unit = {}

  final override def exitPostFix1(
    ctx: CypherParser.PostFix1Context
  ): Unit = {}

  final override def exitProperty(
    ctx: CypherParser.PropertyContext
  ): Unit = {}

  final override def exitPropertyExpression(
    ctx: CypherParser.PropertyExpressionContext
  ): Unit = {}

  final override def exitExpression1(
    ctx: CypherParser.Expression1Context
  ): Unit = {}

  final override def exitCaseExpression(
    ctx: CypherParser.CaseExpressionContext
  ): Unit = {}

  final override def exitListComprehension(
    ctx: CypherParser.ListComprehensionContext
  ): Unit = {}

  final override def exitListComprehensionWhereAndBar(
    ctx: CypherParser.ListComprehensionWhereAndBarContext
  ): Unit = {}

  final override def exitPatternComprehension(
    ctx: CypherParser.PatternComprehensionContext
  ): Unit = {}

  final override def exitReduceExpression(
    ctx: CypherParser.ReduceExpressionContext
  ): Unit = {}

  final override def exitAllExpression(
    ctx: CypherParser.AllExpressionContext
  ): Unit = {}

  final override def exitAnyExpression(
    ctx: CypherParser.AnyExpressionContext
  ): Unit = {}

  final override def exitNoneExpression(
    ctx: CypherParser.NoneExpressionContext
  ): Unit = {}

  final override def exitSingleExpression(
    ctx: CypherParser.SingleExpressionContext
  ): Unit = {}

  final override def exitPatternExpression(
    ctx: CypherParser.PatternExpressionContext
  ): Unit = {}

  final override def exitShortestPathExpression(
    ctx: CypherParser.ShortestPathExpressionContext
  ): Unit = {}

  final override def exitMapProjection(
    ctx: CypherParser.MapProjectionContext
  ): Unit = {}

  final override def exitMapProjectionItem(
    ctx: CypherParser.MapProjectionItemContext
  ): Unit = {}

  final override def exitExistsExpression(
    ctx: CypherParser.ExistsExpressionContext
  ): Unit = {}

  final override def exitCountExpression(
    ctx: CypherParser.CountExpressionContext
  ): Unit = {}

  final override def exitCollectExpression(
    ctx: CypherParser.CollectExpressionContext
  ): Unit = {}

  final override def exitPropertyKeyName(
    ctx: CypherParser.PropertyKeyNameContext
  ): Unit = {}

  final override def exitParameter(
    ctx: CypherParser.ParameterContext
  ): Unit = {}

  final override def exitFunctionInvocation(
    ctx: CypherParser.FunctionInvocationContext
  ): Unit = {}

  final override def exitNamespace(
    ctx: CypherParser.NamespaceContext
  ): Unit = {}

  final override def exitVariableList1(
    ctx: CypherParser.VariableList1Context
  ): Unit = {}

  final override def exitVariable(
    ctx: CypherParser.VariableContext
  ): Unit = {}

  final override def exitSymbolicNameList1(
    ctx: CypherParser.SymbolicNameList1Context
  ): Unit = {}

  final override def exitYieldItem(
    ctx: CypherParser.YieldItemContext
  ): Unit = {}

  final override def exitYieldClause(
    ctx: CypherParser.YieldClauseContext
  ): Unit = {}

  final override def exitShowIndexesAllowBrief(
    ctx: CypherParser.ShowIndexesAllowBriefContext
  ): Unit = {}

  final override def exitShowIndexesNoBrief(
    ctx: CypherParser.ShowIndexesNoBriefContext
  ): Unit = {}

  final override def exitShowConstraintsAllowBriefAndYield(
    ctx: CypherParser.ShowConstraintsAllowBriefAndYieldContext
  ): Unit = {}

  final override def exitShowConstraintsAllowBrief(
    ctx: CypherParser.ShowConstraintsAllowBriefContext
  ): Unit = {}

  final override def exitShowConstraintsAllowYield(
    ctx: CypherParser.ShowConstraintsAllowYieldContext
  ): Unit = {}

  final override def exitShowProcedures(
    ctx: CypherParser.ShowProceduresContext
  ): Unit = {}

  final override def exitShowFunctions(
    ctx: CypherParser.ShowFunctionsContext
  ): Unit = {}

  final override def exitShowTransactions(
    ctx: CypherParser.ShowTransactionsContext
  ): Unit = {}

  final override def exitTerminateTransactions(
    ctx: CypherParser.TerminateTransactionsContext
  ): Unit = {}

  final override def exitShowSettings(
    ctx: CypherParser.ShowSettingsContext
  ): Unit = {}

  final override def exitStringsOrExpression(
    ctx: CypherParser.StringsOrExpressionContext
  ): Unit = {}

  final override def exitCreateConstraint(
    ctx: CypherParser.CreateConstraintContext
  ): Unit = {}

  final override def exitCypherTypeName(
    ctx: CypherParser.CypherTypeNameContext
  ): Unit = {}

  final override def exitCypherTypeNameList(
    ctx: CypherParser.CypherTypeNameListContext
  ): Unit = {}

  final override def exitCypherTypeNamePart(
    ctx: CypherParser.CypherTypeNamePartContext
  ): Unit = {}

  final override def exitLabelResource(
    ctx: CypherParser.LabelResourceContext
  ): Unit = {}

  final override def exitPropertyResource(
    ctx: CypherParser.PropertyResourceContext
  ): Unit = {}

  final override def exitGraphQualifier(
    ctx: CypherParser.GraphQualifierContext
  ): Unit = {}

  final override def exitSymbolicNameOrStringParameterList(
    ctx: CypherParser.SymbolicNameOrStringParameterListContext
  ): Unit = {}

  final override def exitSymbolicNameOrStringParameter(
    ctx: CypherParser.SymbolicNameOrStringParameterContext
  ): Unit = {}

  final override def exitStringList(
    ctx: CypherParser.StringListContext
  ): Unit = {}

  final override def exitStringOrParameter(
    ctx: CypherParser.StringOrParameterContext
  ): Unit = {}

  final override def exitMapOrParameter(
    ctx: CypherParser.MapOrParameterContext
  ): Unit = {}

  final override def exitMap(
    ctx: CypherParser.MapContext
  ): Unit = {}

  final override def exitSymbolicNamePositions(
    ctx: CypherParser.SymbolicNamePositionsContext
  ): Unit = {}

  final override def exitSymbolicNameString(
    ctx: CypherParser.SymbolicNameStringContext
  ): Unit = {}

  final override def exitEscapedSymbolicNameString(
    ctx: CypherParser.EscapedSymbolicNameStringContext
  ): Unit = {}

  final override def exitUnescapedSymbolicNameString(
    ctx: CypherParser.UnescapedSymbolicNameStringContext
  ): Unit = {}

  final override def exitSymbolicLabelNameString(
    ctx: CypherParser.SymbolicLabelNameStringContext
  ): Unit = {}

  final override def exitUnescapedLabelSymbolicNameString(
    ctx: CypherParser.UnescapedLabelSymbolicNameStringContext
  ): Unit = {}

  override def exitNormalizeExpression(
    ctx: CypherParser.NormalizeExpressionContext
  ): Unit = {}

  override def exitPatternComprehensionPrefix(
    ctx: CypherParser.PatternComprehensionPrefixContext
  ): Unit = {}

  override def exitParameterName(
    ctx: CypherParser.ParameterNameContext
  ): Unit = {}

  override def exitFunctionName(
    ctx: CypherParser.FunctionNameContext
  ): Unit = {}

  override def exitFunctionArgument(
    ctx: CypherParser.FunctionArgumentContext
  ): Unit = {}

  override def exitNormalForm(ctx: CypherParser.NormalFormContext): Unit = {}
}
