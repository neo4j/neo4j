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
package org.neo4j.cypher.internal.parser.v6.ast.factory

import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsNotNormalized
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.Concatenate
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NFCNormalForm
import org.neo4j.cypher.internal.expressions.NFDNormalForm
import org.neo4j.cypher.internal.expressions.NFKCNormalForm
import org.neo4j.cypher.internal.expressions.NFKDNormalForm
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.NormalForm
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableSelector
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.expressions.functions.Trim
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.util.Util.astBinaryFold
import org.neo4j.cypher.internal.parser.ast.util.Util.astChild
import org.neo4j.cypher.internal.parser.ast.util.Util.astCtxReduce
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astPairs
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.child
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.lastChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChildType
import org.neo4j.cypher.internal.parser.ast.util.Util.optUnsignedDecimalInt
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.ast.util.Util.unsignedDecimalInt
import org.neo4j.cypher.internal.parser.common.ast.factory.ParserTrimSpecification
import org.neo4j.cypher.internal.parser.common.deprecation.DeprecatedChars
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser
import org.neo4j.cypher.internal.parser.v6.Cypher6ParserListener
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.DeprecatedIdentifierUnicode
import org.neo4j.cypher.internal.util.DeprecatedIdentifierWhitespaceUnicode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.NullType
import org.neo4j.cypher.internal.util.symbols.PathType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType

import java.util.stream.Collectors

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala

trait ExpressionBuilder extends Cypher6ParserListener {

  protected def exceptionFactory: CypherExceptionFactory

  final override def exitQuantifier(ctx: Cypher6Parser.QuantifierContext): Unit = {
    val firstToken = nodeChild(ctx, 0).getSymbol
    ctx.ast = firstToken.getType match {
      case Cypher6Parser.LCURLY =>
        if (ctx.from != null || ctx.to != null || ctx.COMMA() != null) {
          IntervalQuantifier(optUnsignedDecimalInt(ctx.from), optUnsignedDecimalInt(ctx.to))(pos(ctx))
        } else {
          FixedQuantifier(unsignedDecimalInt(nodeChild(ctx, 1).getSymbol))(pos(firstToken))
        }
      case Cypher6Parser.PLUS  => PlusQuantifier()(pos(firstToken))
      case Cypher6Parser.TIMES => StarQuantifier()(pos(firstToken))
    }
  }

  final override def exitAnonymousPattern(ctx: Cypher6Parser.AnonymousPatternContext): Unit = {
    ctx.ast = ctxChild(ctx, 0) match {
      case peCtx: Cypher6Parser.PatternElementContext      => PathPatternPart(peCtx.ast())
      case spCtx: Cypher6Parser.ShortestPathPatternContext => spCtx.ast
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitShortestPathPattern(ctx: Cypher6Parser.ShortestPathPatternContext): Unit = {
    val first = nodeChild(ctx, 0).getSymbol
    ctx.ast =
      ShortestPathsPatternPart(ctx.patternElement().ast(), first.getType != Cypher6Parser.ALL_SHORTEST_PATHS)(pos(
        first
      ))
  }

  final override def exitPatternElement(ctx: Cypher6Parser.PatternElementContext): Unit = {
    val size = ctx.children.size()
    if (size == 1) {
      ctx.ast = ctxChild(ctx, 0).ast[PathFactor]()
    } else {
      val p = pos(ctx)
      val parts = new mutable.ArrayDeque[PathFactor](1)
      var relPattern: RelationshipPattern = null
      var i = 0
      while (i < size) {
        ctx.children.get(i) match {
          case nCtx: Cypher6Parser.NodePatternContext =>
            val nodePattern = nCtx.ast[NodePattern]()
            if (relPattern != null) {
              val lhs = parts.removeLast().asInstanceOf[SimplePattern]
              parts.addOne(RelationshipChain(lhs, relPattern, nodePattern)(p))
              relPattern = null
            } else {
              parts.addOne(nodePattern)
            }
          case relCtx: Cypher6Parser.RelationshipPatternContext =>
            relPattern = relCtx.ast[RelationshipPattern]()
          case qCtx: Cypher6Parser.QuantifierContext =>
            val emptyNodePattern = NodePattern(None, None, None, None)(relPattern.position)
            val part =
              PathPatternPart(RelationshipChain(emptyNodePattern, relPattern, emptyNodePattern)(relPattern.position))
            parts.addOne(QuantifiedPath(part, qCtx.ast(), None)(relPattern.position))
            relPattern = null
          case parenCtx: Cypher6Parser.ParenthesizedPathContext =>
            parts.addOne(parenCtx.ast[ParenthesizedPath]())
          case other => throw new IllegalStateException(s"Unexpected child $other")
        }
        i += 1
      }
      ctx.ast =
        if (parts.size == 1) parts.head
        else PathConcatenation(ArraySeq.unsafeWrapArray(parts.toArray))(pos(ctx))
    }
  }

  private def selectorCount(node: TerminalNode, p: InputPosition): UnsignedDecimalIntegerLiteral =
    if (node == null) UnsignedDecimalIntegerLiteral("1")(p)
    else UnsignedDecimalIntegerLiteral(node.getText)(pos(node))

  final override def exitSelector(ctx: Cypher6Parser.SelectorContext): Unit = {
    val p = pos(ctx)
    ctx.ast = ctx match {
      case anyShortestCtx: Cypher6Parser.AnyShortestPathContext =>
        PatternPart.AnyShortestPath(selectorCount(anyShortestCtx.UNSIGNED_DECIMAL_INTEGER(), p))(p)
      case allShortestCtx: Cypher6Parser.AllShortestPathContext =>
        PatternPart.AllShortestPaths()(pos(allShortestCtx))
      case anyCtx: Cypher6Parser.AnyPathContext =>
        PatternPart.AnyPath(selectorCount(anyCtx.UNSIGNED_DECIMAL_INTEGER(), p))(p)
      case shortestGrpCtx: Cypher6Parser.ShortestGroupContext =>
        PatternPart.ShortestGroups(selectorCount(shortestGrpCtx.UNSIGNED_DECIMAL_INTEGER(), p))(p)
      case allPathCtx: Cypher6Parser.AllPathContext =>
        PatternPart.AllPaths()(p)
      case _ => throw new IllegalStateException(s"Unexpected context $ctx")
    }
  }

  final override def exitParenthesizedPath(ctx: Cypher6Parser.ParenthesizedPathContext): Unit = {
    val p = pos(ctx)
    val pattern = astChild[PatternPart](ctx, 1) match {
      case p: NonPrefixedPatternPart => p
      case p: PatternPartWithSelector =>
        val pathPatternKind = if (ctx.quantifier() == null) "parenthesized" else "quantified"
        throw exceptionFactory.syntaxException(
          s"Path selectors such as `${p.selector.prettified}` are not supported within $pathPatternKind path patterns.",
          p.position
        )
    }
    val quantifier = ctx.quantifier()
    ctx.ast = if (quantifier != null) QuantifiedPath(pattern, quantifier.ast(), astOpt(ctx.expression()))(p)
    else ParenthesizedPath(pattern, astOpt(ctx.expression()))(p)
  }

  final override def exitProperties(
    ctx: Cypher6Parser.PropertiesContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitPathLength(
    ctx: Cypher6Parser.PathLengthContext
  ): Unit = {
    // This is weird, we should refactor range to be more sensible and not use nested options
    ctx.ast = if (ctx.DOTDOT() != null) {
      val from = optUnsignedDecimalInt(ctx.from)
      val to = optUnsignedDecimalInt(ctx.to)
      Some(org.neo4j.cypher.internal.expressions.Range(from, to)(from.map(_.position).getOrElse(pos(ctx))))
    } else if (ctx.single != null) {
      val single = Some(UnsignedDecimalIntegerLiteral(ctx.single.getText)(pos(ctx.single)))
      Some(org.neo4j.cypher.internal.expressions.Range(single, single)(pos(ctx)))
    } else None
  }

  final override def exitExpression(ctx: Cypher6Parser.ExpressionContext): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold[Expression](ctx, (lhs, token, rhs) => Or(lhs, rhs)(pos(token)))
  }

  final override def exitExpression11(ctx: Cypher6Parser.Expression11Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold[Expression](ctx, (lhs, token, rhs) => Xor(lhs, rhs)(pos(token)))
  }

  final override def exitExpression10(ctx: Cypher6Parser.Expression10Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold[Expression](ctx, (lhs, token, rhs) => And(lhs, rhs)(pos(token)))
  }

  final override def exitExpression9(ctx: Cypher6Parser.Expression9Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.expression8() == lastChild(ctx))
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case 2 => Not(astChild(ctx, 1))(pos(ctx))
      case _ => ctx.NOT().asScala.foldRight(lastChild[AstRuleCtx](ctx).ast[Expression]()) { case (not, acc) =>
          Not(acc)(pos(not.getSymbol))
        }
    }
  }

  final override def exitExpression8(ctx: Cypher6Parser.Expression8Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case 3 => binaryPredicate(ctxChild(ctx, 0).ast(), child(ctx, 1), child(ctx, 2))
      case _ =>
        Ands(ctx.children.asScala.toSeq.sliding(3, 2).map {
          case Seq(lhs: AstRuleCtx, token: TerminalNode, rhs: AstRuleCtx) => binaryPredicate(lhs.ast(), token, rhs)
          case _ => throw new IllegalStateException(s"Unexpected parse results $ctx")
        })(pos(nodeChild(ctx, 1)))
    }
  }

  private def binaryPredicate(lhs: Expression, token: TerminalNode, rhs: AstRuleCtx): Expression = {
    token.getSymbol.getType match {
      case Cypher6Parser.EQ          => Equals(lhs, rhs.ast())(pos(token.getSymbol))
      case Cypher6Parser.INVALID_NEQ => InvalidNotEquals(lhs, rhs.ast())(pos(token.getSymbol))
      case Cypher6Parser.NEQ         => NotEquals(lhs, rhs.ast())(pos(token.getSymbol))
      case Cypher6Parser.LE          => LessThanOrEqual(lhs, rhs.ast())(pos(token.getSymbol))
      case Cypher6Parser.GE          => GreaterThanOrEqual(lhs, rhs.ast())(pos(token.getSymbol))
      case Cypher6Parser.LT          => LessThan(lhs, rhs.ast())(pos(token.getSymbol))
      case Cypher6Parser.GT          => GreaterThan(lhs, rhs.ast())(pos(token.getSymbol))
    }
  }

  final override def exitExpression7(ctx: Cypher6Parser.Expression7Context): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case _ =>
        val lhs = ctxChild(ctx, 0).ast[Expression]()
        ctxChild(ctx, 1) match {
          case strCtx: Cypher6Parser.StringAndListComparisonContext =>
            stringAndListComparisonExpression(lhs, strCtx)
          case nullCtx: Cypher6Parser.NullComparisonContext =>
            nullComparisonExpression(lhs, nullCtx)
          case typeCtx: Cypher6Parser.TypeComparisonContext =>
            typeComparisonExpression(lhs, typeCtx)
          case nfCtx: Cypher6Parser.NormalFormComparisonContext =>
            normalFormComparisonExpression(lhs, nfCtx.normalForm(), nfCtx.NOT() != null, pos(nfCtx))
          case _ => throw new IllegalStateException(s"Unexpected parse result $ctx")
        }
    }
  }

  private def stringAndListComparisonExpression(lhs: Expression, ctx: AstRuleCtx): Expression = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      ctx.isInstanceOf[Cypher6Parser.StringAndListComparisonContext] ||
        ctx.isInstanceOf[Cypher6Parser.WhenStringOrListContext]
    )
    val token = child[TerminalNode](ctx, 0).getSymbol
    val rhs = lastChild[AstRuleCtx](ctx).ast[Expression]()
    token.getType match {
      case Cypher6Parser.REGEQ    => RegexMatch(lhs, rhs)(pos(token))
      case Cypher6Parser.STARTS   => StartsWith(lhs, rhs)(pos(token))
      case Cypher6Parser.ENDS     => EndsWith(lhs, rhs)(pos(token))
      case Cypher6Parser.CONTAINS => Contains(lhs, rhs)(pos(token))
      case Cypher6Parser.IN       => In(lhs, rhs)(pos(token))
    }
  }

  private def nullComparisonExpression(lhs: Expression, ctx: AstRuleCtx): Expression = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      ctx.isInstanceOf[Cypher6Parser.NullComparisonContext] ||
        ctx.isInstanceOf[Cypher6Parser.WhenNullContext]
    )
    if (nodeChildType(ctx, 1) != Cypher6Parser.NOT) IsNull(lhs)(pos(ctx))
    else IsNotNull(lhs)(pos(ctx))
  }

  private def typeComparisonExpression(lhs: Expression, ctx: AstRuleCtx): Expression = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      ctx.isInstanceOf[Cypher6Parser.TypeComparisonContext] ||
        ctx.isInstanceOf[Cypher6Parser.WhenTypeContext]
    )
    val cypherType = lastChild[AstRuleCtx](ctx).ast[CypherType]()
    val not = child[ParseTree](ctx, 1) match {
      case n: TerminalNode => n.getSymbol.getType == Cypher6Parser.NOT
      case _               => false
    }
    if (not) IsNotTyped(lhs, cypherType)(pos(ctx))
    else IsTyped(lhs, cypherType)(pos(ctx))
  }

  private def normalFormComparisonExpression(
    lhs: Expression,
    nfCtx: Cypher6Parser.NormalFormContext,
    not: Boolean,
    p: InputPosition
  ): Expression = {
    val nf = astOpt[NormalForm](nfCtx, NFCNormalForm)
    if (not) IsNotNormalized(lhs, nf)(p)
    else IsNormalized(lhs, nf)(p)
  }

  final override def exitComparisonExpression6(ctx: Cypher6Parser.ComparisonExpression6Context): Unit = {}

  final override def exitNormalForm(ctx: Cypher6Parser.NormalFormContext): Unit = {
    ctx.ast = child[TerminalNode](ctx, 0).getSymbol.getType match {
      case Cypher6Parser.NFC  => NFCNormalForm
      case Cypher6Parser.NFD  => NFDNormalForm
      case Cypher6Parser.NFKC => NFKCNormalForm
      case Cypher6Parser.NFKD => NFKDNormalForm
    }

  }

  final override def exitExpression6(ctx: Cypher6Parser.Expression6Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold(ctx, binaryAdditive)
  }

  private def binaryAdditive(lhs: Expression, token: TerminalNode, rhs: Expression): Expression = {
    token.getSymbol.getType match {
      case Cypher6Parser.PLUS      => Add(lhs, rhs)(pos(token.getSymbol))
      case Cypher6Parser.MINUS     => Subtract(lhs, rhs)(pos(token.getSymbol))
      case Cypher6Parser.DOUBLEBAR => Concatenate(lhs, rhs)(pos(token.getSymbol))
    }
  }

  final override def exitExpression5(ctx: Cypher6Parser.Expression5Context): Unit = {
    ctx.ast = astBinaryFold(ctx, binaryMultiplicative)
  }

  private def binaryMultiplicative(lhs: Expression, token: TerminalNode, rhs: Expression): Expression = {
    token.getSymbol.getType match {
      case Cypher6Parser.TIMES   => Multiply(lhs, rhs)(pos(token.getSymbol))
      case Cypher6Parser.DIVIDE  => Divide(lhs, rhs)(pos(token.getSymbol))
      case Cypher6Parser.PERCENT => Modulo(lhs, rhs)(pos(token.getSymbol))
    }
  }

  final override def exitExpression4(ctx: Cypher6Parser.Expression4Context): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.getChildCount % 2 == 1)
    ctx.ast = astBinaryFold[Expression](ctx, (lhs, token, rhs) => Pow(lhs, rhs)(pos(token.getSymbol)))
  }

  final override def exitExpression3(ctx: Cypher6Parser.Expression3Context): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast()
      case _ =>
        if (ctx.PLUS() != null) UnaryAdd(lastChild[AstRuleCtx](ctx).ast())(pos(ctx))
        else UnarySubtract(lastChild[AstRuleCtx](ctx).ast())(pos(ctx))
    }
  }

  final override def exitExpression2(
    ctx: Cypher6Parser.Expression2Context
  ): Unit = {
    ctx.ast = ctx.children.size() match {
      case 1 => ctxChild(ctx, 0).ast
      case _ => astCtxReduce(ctx, postFix)
    }
  }

  // TODO All postfix should probably have positions that work in the same manner
  private def postFix(lhs: Expression, rhs: Cypher6Parser.PostFixContext): Expression = {
    val p = lhs.position
    rhs match {
      case propCtx: Cypher6Parser.PropertyPostfixContext => Property(lhs, ctxChild(propCtx, 0).ast())(p)
      case indexCtx: Cypher6Parser.IndexPostfixContext =>
        ContainerIndex(lhs, ctxChild(indexCtx, 1).ast())(pos(ctxChild(indexCtx, 1)))
      case labelCtx: Cypher6Parser.LabelPostfixContext => LabelExpressionPredicate(lhs, ctxChild(labelCtx, 0).ast())(p)
      case rangeCtx: Cypher6Parser.RangePostfixContext =>
        ListSlice(lhs, astOpt(rangeCtx.fromExp), astOpt(rangeCtx.toExp))(pos(rhs))
      case _ => throw new IllegalStateException(s"Unexpected rhs $rhs")
    }
  }

  final override def exitPostFix(ctx: Cypher6Parser.PostFixContext): Unit = {}

  final override def exitProperty(ctx: Cypher6Parser.PropertyContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast[PropertyKeyName]()
  }

  def exitDynamicProperty(ctx: Cypher6Parser.DynamicPropertyContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast()
  }

  final override def exitPropertyExpression(ctx: Cypher6Parser.PropertyExpressionContext): Unit = {
    var result = Property(ctxChild(ctx, 0).ast(), ctxChild(ctx, 1).ast())(pos(ctx))
    val size = ctx.children.size(); var i = 2
    while (i < size) {
      val key = ctxChild(ctx, i).ast[PropertyKeyName]()
      result = Property(result, key)(key.position)
      i += 1
    }
    ctx.ast = result
  }

  final override def exitDynamicPropertyExpression(ctx: Cypher6Parser.DynamicPropertyExpressionContext): Unit = {
    val index = ctxChild(ctx, 1).ast[Expression]()
    ctx.ast = ContainerIndex(ctxChild(ctx, 0).ast(), index)(index.position)
  }

  final override def exitExpression1(ctx: Cypher6Parser.Expression1Context): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast()
      case _ => throw new IllegalStateException("Unexpected expression")
    }
  }

  final override def exitCaseExpression(
    ctx: Cypher6Parser.CaseExpressionContext
  ): Unit = {
    ctx.ast = CaseExpression(
      expression = None,
      alternatives = astSeq(ctx.caseAlternative()),
      default = astOpt(ctx.expression())
    )(pos(ctx))
  }

  final override def exitCaseAlternative(ctx: Cypher6Parser.CaseAlternativeContext): Unit = {
    ctx.ast = (ctxChild(ctx, 1).ast, ctxChild(ctx, 3).ast)
  }

  final override def exitExtendedCaseExpression(ctx: Cypher6Parser.ExtendedCaseExpressionContext): Unit = {
    val caseExp = astChild[Expression](ctx, 1)
    ctx.ast = CaseExpression(
      expression = Some(caseExp),
      alternatives = extendedCaseAlts(caseExp, ctx.extendedCaseAlternative()),
      default = astOpt(ctx.elseExp)
    )(pos(ctx))
  }

  private def extendedCaseAlts(
    lhs: Expression,
    ctxs: java.util.List[Cypher6Parser.ExtendedCaseAlternativeContext]
  ): ArraySeq[(Expression, Expression)] = {
    val size = ctxs.size()
    val resultBuffer = new ArrayBuffer[(Expression, Expression)](size)
    var i = 0
    while (i < size) {
      extendedCaseAlt(resultBuffer, lhs, ctxs.get(i))
      i += 1
    }
    ArraySeq.unsafeWrapArray(resultBuffer.toArray)
  }

  private def extendedCaseAlt(
    buffer: ArrayBuffer[(Expression, Expression)],
    lhs: Expression,
    ctx: Cypher6Parser.ExtendedCaseAlternativeContext
  ): Unit = {
    val size = ctx.children.size()
    var i = 1
    val thenExp = lastChild[AstRuleCtx](ctx).ast[Expression]()
    while (i < size) {
      ctx.children.get(i) match {
        case whenCtx: Cypher6Parser.ExtendedWhenContext =>
          val newWhen = whenCtx match {
            case _: Cypher6Parser.WhenEqualsContext =>
              Equals(lhs, astChild(whenCtx, 0))(pos(nodeChild(ctx, i - 1)))
            case _: Cypher6Parser.WhenComparatorContext =>
              binaryPredicate(lhs, nodeChild(whenCtx, 0), ctxChild(whenCtx, 1))
            case _: Cypher6Parser.WhenStringOrListContext =>
              stringAndListComparisonExpression(lhs, whenCtx)
            case _: Cypher6Parser.WhenNullContext =>
              nullComparisonExpression(lhs, whenCtx)
            case _: Cypher6Parser.WhenTypeContext =>
              typeComparisonExpression(lhs, whenCtx)
            case formCtx: Cypher6Parser.WhenFormContext =>
              normalFormComparisonExpression(lhs, formCtx.normalForm(), formCtx.NOT() != null, pos(formCtx))
            case _ => throw new IllegalStateException(s"Unexpected context $whenCtx")
          }
          buffer.addOne(newWhen -> thenExp)
        case _ =>
      }
      i += 1
    }
  }

  final override def exitExtendedCaseAlternative(ctx: Cypher6Parser.ExtendedCaseAlternativeContext): Unit = {}

  final override def exitExtendedWhen(ctx: Cypher6Parser.ExtendedWhenContext): Unit = {}

  final override def exitListComprehension(ctx: Cypher6Parser.ListComprehensionContext): Unit = {
    ctx.ast = ListComprehension(
      variable = ctxChild(ctx, 1).ast(),
      expression = ctxChild(ctx, 3).ast(),
      innerPredicate = if (ctx.whereExp != null) Some(ctx.whereExp.ast()) else None,
      extractExpression = if (ctx.barExp != null) Some(ctx.barExp.ast()) else None
    )(pos(ctx))
  }

  final override def exitPatternComprehension(
    ctx: Cypher6Parser.PatternComprehensionContext
  ): Unit = {
    val variable = if (ctx.variable() != null) Some(ctx.variable().ast[LogicalVariable]()) else None
    val pathPatternNonEmpty = ctx.pathPatternNonEmpty().ast[RelationshipsPattern]()
    val whereExp = if (ctx.whereExp != null) Some(ctx.whereExp.ast[PatternExpression]()) else None
    val barExp = ctx.barExp.ast[Expression]()
    ctx.ast = PatternComprehension(variable, pathPatternNonEmpty, whereExp, barExp)(pos(ctx), None, None)
  }

  final override def exitPathPatternNonEmpty(
    ctx: Cypher6Parser.PathPatternNonEmptyContext
  ): Unit = {
    val first = ctxChild(ctx, 0).ast[PathFactor]()
    val size = ctx.children.size()
    var part = first
    var i = 1
    var relPattern: RelationshipPattern = null
    while (i < size) {
      ctxChild(ctx, i) match {
        case relCtx: Cypher6Parser.RelationshipPatternContext => relPattern = relCtx.ast()
        case nodeCtx: Cypher6Parser.NodePatternContext =>
          part = RelationshipChain(
            part.asInstanceOf[SimplePattern],
            relPattern,
            nodeCtx.ast()
          )(pos(nodeCtx))
        case other => throw new IllegalStateException(s"Unexpected child $other")
      }
      i += 1
    }
    ctx.ast = RelationshipsPattern(part.asInstanceOf[RelationshipChain])(pos(ctx))
  }

  final override def exitPatternExpression(
    ctx: Cypher6Parser.PatternExpressionContext
  ): Unit = {
    ctx.ast = PatternExpression(ctxChild(ctx, 0).ast())(None, None)
  }

  final override def exitReduceExpression(
    ctx: Cypher6Parser.ReduceExpressionContext
  ): Unit = {
    val accumulator = ctxChild(ctx, 2).ast[LogicalVariable]()
    val variable = ctxChild(ctx, 6).ast[LogicalVariable]()
    val expression = ctxChild(ctx, 10).ast[Expression]()
    val init = ctxChild(ctx, 4).ast[Expression]()
    val collection = ctxChild(ctx, 8).ast[Expression]()
    ctx.ast = ReduceExpression(ReduceScope(accumulator, variable, expression)(pos(ctx)), init, collection)(pos(ctx))
  }

  final override def exitListItemsPredicate(ctx: Cypher6Parser.ListItemsPredicateContext): Unit = {
    val p = pos(ctx)
    val variable = ctx.variable().ast[Variable]()
    val inExp = ctx.inExp.ast[Expression]()
    val where = astOpt[Expression](ctx.whereExp)
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case Cypher6Parser.ALL    => AllIterablePredicate(variable, inExp, where)(p)
      case Cypher6Parser.ANY    => AnyIterablePredicate(variable, inExp, where)(p)
      case Cypher6Parser.NONE   => NoneIterablePredicate(variable, inExp, where)(p)
      case Cypher6Parser.SINGLE => SingleIterablePredicate(variable, inExp, where)(p)
    }
  }

  final override def exitShortestPathExpression(ctx: Cypher6Parser.ShortestPathExpressionContext): Unit = {
    ctx.ast = ShortestPathExpression(astChild(ctx, 0))
  }

  final override def exitParenthesizedExpression(
    ctx: Cypher6Parser.ParenthesizedExpressionContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitMapProjection(
    ctx: Cypher6Parser.MapProjectionContext
  ): Unit = {
    ctx.ast = MapProjection(ctx.variable().ast[Variable](), astSeq(ctx.mapProjectionElement()))(pos(ctx.LCURLY()))
  }

  final override def exitMapProjectionElement(
    ctx: Cypher6Parser.MapProjectionElementContext
  ): Unit = {
    val colon = ctx.COLON()
    val variable = ctx.variable()
    val propertyKeyName = ctx.propertyKeyName()
    val property = ctx.property()
    ctx.ast =
      if (colon != null) {
        val expr = ctx.expression().ast[Expression]()
        LiteralEntry(propertyKeyName.ast(), expr)(expr.position)
      } else if (variable != null) {
        VariableSelector(variable.ast())(pos(ctx))
      } else if (property != null) {
        val prop = property.ast[PropertyKeyName]()
        PropertySelector(prop)(prop.position)
      } else {
        AllPropertiesSelector()(pos(ctx.TIMES().getSymbol))
      }
  }

  final override def exitCountStar(
    ctx: Cypher6Parser.CountStarContext
  ): Unit = {
    ctx.ast = CountStar()(pos(ctx))
  }

  final override def exitExistsExpression(
    ctx: Cypher6Parser.ExistsExpressionContext
  ): Unit = {
    ctx.ast = ExistsExpression(subqueryBuilder(
      ctx.regularQuery(),
      ctx.matchMode(),
      ctx.whereClause(),
      ctx.patternList()
    ))(pos(ctx), None, None)
  }

  final override def exitCountExpression(
    ctx: Cypher6Parser.CountExpressionContext
  ): Unit = {
    ctx.ast = CountExpression(subqueryBuilder(
      ctx.regularQuery(),
      ctx.matchMode(),
      ctx.whereClause(),
      ctx.patternList()
    ))(pos(ctx), None, None)
  }

  private def subqueryBuilder(
    regQuery: Cypher6Parser.RegularQueryContext,
    matchMode: Cypher6Parser.MatchModeContext,
    whereClause: Cypher6Parser.WhereClauseContext,
    patternList: Cypher6Parser.PatternListContext
  ): Query = {
    if (regQuery != null) regQuery.ast[Query]()
    else {
      val patternParts = patternList.ast[ArraySeq[PatternPart]]().map {
        case p: PatternPartWithSelector => p
        case p: NonPrefixedPatternPart  => PatternPartWithSelector(PatternPart.AllPaths()(p.position), p)
      }
      val patternPos = patternParts.head.position
      val where = astOpt[Where](whereClause)
      val finalMatchMode = astOpt(matchMode, MatchMode.default(patternPos))
      SingleQuery(
        ArraySeq(
          Match(optional = false, finalMatchMode, Pattern.ForMatch(patternParts)(patternPos), Seq.empty, where)(
            patternPos
          )
        )
      )(patternPos)
    }

  }

  final override def exitCollectExpression(
    ctx: Cypher6Parser.CollectExpressionContext
  ): Unit = {
    ctx.ast = CollectExpression(ctx.regularQuery().ast[Query]())(pos(ctx), None, None)
  }

  final override def exitPropertyKeyName(ctx: Cypher6Parser.PropertyKeyNameContext): Unit = {
    ctx.ast = PropertyKeyName(ctxChild(ctx, 0).ast())(pos(ctx))
  }

  final override def exitParameter(ctx: Cypher6Parser.ParameterContext): Unit = {
    val ast = ctx.parameterName().ast[ExplicitParameter]()
    ctx.ast = ast.copy()(position = pos(ctx))
  }

  final override def exitParameterName(ctx: Cypher6Parser.ParameterNameContext): Unit = {
    val parameterType = ctx.paramType match {
      case "STRING" => CTString
      case "MAP"    => CTMap
      case _        => CTAny
    }
    val name: String = child[ParseTree](ctx, 0) match {
      case strCtx: Cypher6Parser.SymbolicNameStringContext => strCtx.ast()
      case node: TerminalNode                              => node.getText
      case _                                               => throw new IllegalStateException(s"Unexpected ctx $ctx")
    }
    ctx.ast = ExplicitParameter(name, parameterType)(pos(ctx))
  }

  final override def exitFunctionInvocation(
    ctx: Cypher6Parser.FunctionInvocationContext
  ): Unit = {
    val functionName = ctx.functionName().ast[FunctionName]
    val distinct = if (ctx.DISTINCT() != null) true else false
    val expressions =
      astSeq[Expression](ctx.functionArgument().stream().map(arg => arg.expression()).collect(Collectors.toList()))
    ctx.ast = FunctionInvocation(
      functionName,
      distinct,
      expressions,
      ArgumentUnordered,
      ctx.parent.isInstanceOf[Cypher6Parser.GraphReferenceContext]
    )(functionName.namespace.position)
  }

  final override def exitFunctionName(
    ctx: Cypher6Parser.FunctionNameContext
  ): Unit = {
    val namespace = ctx.namespace().ast[Namespace]
    val functionNameCtx = ctx.symbolicNameString()
    val functionName: String = functionNameCtx.ast()
    ctx.ast = FunctionName(namespace, functionName)(pos(functionNameCtx))
  }

  final override def exitFunctionArgument(
    ctx: Cypher6Parser.FunctionArgumentContext
  ): Unit = {
    ctx.ast = ctx.expression().ast[Expression]()
  }

  final override def exitNamespace(
    ctx: Cypher6Parser.NamespaceContext
  ): Unit = {
    ctx.ast = Namespace(astSeq[String](ctx.symbolicNameString()).toList)(pos(ctx))
  }

  final override def exitVariable(
    ctx: Cypher6Parser.VariableContext
  ): Unit = {
    ctx.ast = Variable(name = ctx.symbolicNameString().ast())(pos(ctx))
  }

  final override def exitType(ctx: Cypher6Parser.TypeContext): Unit = {
    val cypherType = ctx.children.size() match {
      case 1 => ctxChild(ctx, 0).ast[CypherType]
      case _ =>
        val types = astSeq[CypherType](ctx.typePart()).toSet
        if (types.size == 1) types.head else ClosedDynamicUnionType(types)(pos(ctx))
    }
    ctx.ast = cypherType.simplify
  }

  final override def exitTypePart(ctx: Cypher6Parser.TypePartContext): Unit = {
    var cypherType = ctx.typeName().ast[CypherType]()
    if (ctx.typeNullability() != null) cypherType = cypherType.withIsNullable(false)
    ctx.typeListSuffix().forEach { list =>
      cypherType = ListType(cypherType, list.ast())(pos(ctx))
    }
    ctx.ast = cypherType
  }

  final override def exitTypeName(ctx: Cypher6Parser.TypeNameContext): Unit = {
    val size = ctx.children.size
    val p = pos(ctx)
    val firstToken = nodeChild(ctx, 0).getSymbol.getType
    ctx.ast = size match {
      case 1 => firstToken match {
          case Cypher6Parser.NOTHING                           => NothingType()(p)
          case Cypher6Parser.NULL                              => NullType()(p)
          case Cypher6Parser.BOOL | Cypher6Parser.BOOLEAN      => BooleanType(true)(p)
          case Cypher6Parser.STRING | Cypher6Parser.VARCHAR    => StringType(true)(p)
          case Cypher6Parser.INT | Cypher6Parser.INTEGER       => IntegerType(true)(p)
          case Cypher6Parser.FLOAT                             => FloatType(true)(p)
          case Cypher6Parser.DATE                              => DateType(true)(p)
          case Cypher6Parser.DURATION                          => DurationType(true)(p)
          case Cypher6Parser.POINT                             => PointType(true)(p)
          case Cypher6Parser.NODE | Cypher6Parser.VERTEX       => NodeType(true)(p)
          case Cypher6Parser.RELATIONSHIP | Cypher6Parser.EDGE => RelationshipType(true)(p)
          case Cypher6Parser.MAP                               => MapType(true)(p)
          case Cypher6Parser.PATH | Cypher6Parser.PATHS        => PathType(true)(p)
          case Cypher6Parser.ANY                               => AnyType(true)(p)
          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
      case 2 => firstToken match {
          case Cypher6Parser.SIGNED   => IntegerType(true)(p)
          case Cypher6Parser.PROPERTY => PropertyValueType(true)(p)
          case Cypher6Parser.LOCAL => nodeChild(ctx, 1).getSymbol.getType match {
              case Cypher6Parser.TIME     => LocalTimeType(true)(p)
              case Cypher6Parser.DATETIME => LocalDateTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case Cypher6Parser.ZONED => nodeChild(ctx, 1).getSymbol.getType match {
              case Cypher6Parser.TIME     => ZonedTimeType(true)(p)
              case Cypher6Parser.DATETIME => ZonedDateTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case Cypher6Parser.ANY => nodeChild(ctx, 1).getSymbol.getType match {
              case Cypher6Parser.NODE | Cypher6Parser.VERTEX       => NodeType(true)(p)
              case Cypher6Parser.RELATIONSHIP | Cypher6Parser.EDGE => RelationshipType(true)(p)
              case Cypher6Parser.MAP                               => MapType(true)(p)
              case Cypher6Parser.VALUE                             => AnyType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
      case 3 => firstToken match {
          case Cypher6Parser.TIME => nodeChild(ctx, 1).getSymbol.getType match {
              case Cypher6Parser.WITH    => ZonedTimeType(true)(p)
              case Cypher6Parser.WITHOUT => LocalTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case Cypher6Parser.TIMESTAMP => nodeChild(ctx, 1).getSymbol.getType match {
              case Cypher6Parser.WITH    => ZonedDateTimeType(true)(p)
              case Cypher6Parser.WITHOUT => LocalDateTimeType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case Cypher6Parser.ANY => nodeChild(ctx, 1).getSymbol.getType match {
              case Cypher6Parser.PROPERTY => PropertyValueType(true)(p)
              case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
            }
          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
      case _ => firstToken match {
          case Cypher6Parser.LIST | Cypher6Parser.ARRAY => ListType(ctx.`type`().ast(), true)(p)
          case Cypher6Parser.ANY =>
            AssertMacros.checkOnlyWhenAssertionsAreEnabled(ctx.LT() != null && ctx.GT() != null)
            ctx.`type`().ast[CypherType]() match {
              case du: ClosedDynamicUnionType => du
              case other                      => ClosedDynamicUnionType(Set(other))(other.position)
            }

          case _ => throw new IllegalStateException(s"Unexpected context $ctx (first token type $firstToken)")
        }
    }
  }

  final override def exitTypeNullability(ctx: Cypher6Parser.TypeNullabilityContext): Unit = {}

  final override def exitTypeListSuffix(ctx: Cypher6Parser.TypeListSuffixContext): Unit = {
    ctx.ast = ctx.typeNullability() == null
  }

  final override def exitMap(ctx: Cypher6Parser.MapContext): Unit =
    ctx.ast = MapExpression(astPairs(ctx.propertyKeyName(), ctx.expression()))(pos(ctx))

  final override def exitSymbolicNameString(
    ctx: Cypher6Parser.SymbolicNameStringContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitEscapedSymbolicNameString(ctx: Cypher6Parser.EscapedSymbolicNameStringContext): Unit = {
    ctx.ast = ctx.start.getInputStream
      .getText(new Interval(ctx.start.getStartIndex + 1, ctx.stop.getStopIndex - 1)).replace("``", "`")
  }

  protected def notificationLogger: Option[InternalNotificationLogger]

  private def reportDeprecatedChars(text: String, p: InputPosition): Unit = {
    for {
      logger <- notificationLogger
      deprecatedChar <- DeprecatedChars.deprecatedChars(text).asScala
    } {
      if (deprecatedChar == '\u0085') logger.log(DeprecatedIdentifierWhitespaceUnicode(p, deprecatedChar, text))
      else logger.log(DeprecatedIdentifierUnicode(p, deprecatedChar, text))
    }
  }

  final override def exitUnescapedSymbolicNameString(ctx: Cypher6Parser.UnescapedSymbolicNameStringContext): Unit = {
    val text = ctx.getText
    if (DeprecatedChars.containsDeprecatedChar(text)) reportDeprecatedChars(text, pos(ctx))
    ctx.ast = text
  }

  final override def exitSymbolicLabelNameString(
    ctx: Cypher6Parser.SymbolicLabelNameStringContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitUnescapedLabelSymbolicNameString(
    ctx: Cypher6Parser.UnescapedLabelSymbolicNameStringContext
  ): Unit = {
    ctx.ast = ctx.getText
  }

  final override def exitNormalizeFunction(
    ctx: Cypher6Parser.NormalizeFunctionContext
  ): Unit = {
    val expression = ctx.expression().ast[Expression]()
    val normalFormCtx = ctx.normalForm()
    val normalForm = astOpt[NormalForm](normalFormCtx, NFCNormalForm).formName

    ctx.ast =
      FunctionInvocation(
        FunctionName("normalize")(pos(ctx)),
        distinct = false,
        IndexedSeq(expression, StringLiteral(normalForm)(pos(ctx).withInputLength(0)))
      )(pos(ctx))
  }

  final override def exitTrimFunction(
    ctx: Cypher6Parser.TrimFunctionContext
  ): Unit = {
    val trimSource = ctx.trimSource.ast[Expression]()
    val trimCharacterString = astOpt[Expression](ctx.trimCharacterString)
    var trimSpecification = ParserTrimSpecification.BOTH.description()
    if (ctx.LEADING() != null) trimSpecification = ParserTrimSpecification.LEADING.description()
    if (ctx.TRAILING() != null) trimSpecification = ParserTrimSpecification.TRAILING.description()

    ctx.ast = if (trimCharacterString.isEmpty) {
      FunctionInvocation(
        FunctionName(Trim.name)(pos(ctx)),
        distinct = false,
        args = IndexedSeq(
          StringLiteral(trimSpecification)(pos(ctx).withInputLength(0)),
          trimSource
        )
      )(
        pos(ctx)
      )
    } else {
      FunctionInvocation(
        FunctionName(Trim.name)(pos(ctx)),
        distinct = false,
        args = IndexedSeq(
          StringLiteral(trimSpecification)(pos(ctx).withInputLength(0)),
          trimCharacterString.get,
          trimSource
        )
      )(
        pos(ctx)
      )
    }
  }
}
