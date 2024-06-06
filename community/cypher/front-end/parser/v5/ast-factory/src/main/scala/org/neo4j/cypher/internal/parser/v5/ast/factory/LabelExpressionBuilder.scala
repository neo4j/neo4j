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
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.util.Util.astChild
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.lastChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.AnyLabelContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.AnyLabelIsContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.LabelNameContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.LabelNameIsContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.ParenthesizedLabelExpressionContext
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser.ParenthesizedLabelExpressionIsContext
import org.neo4j.cypher.internal.parser.v5.Cypher5ParserListener

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait LabelExpressionBuilder extends Cypher5ParserListener {

  final override def exitNodePattern(
    ctx: Cypher5Parser.NodePatternContext
  ): Unit = {
    val variable = astOpt[LogicalVariable](ctx.variable())
    val labelExpression = astOpt[LabelExpression](ctx.labelExpression())
    val properties = astOpt[Expression](ctx.properties())
    val expression = astOpt[Expression](ctx.expression())
    ctx.ast = NodePattern(variable, labelExpression, properties, expression)(pos(ctx))
  }

  final override def exitRelationshipPattern(
    ctx: Cypher5Parser.RelationshipPatternContext
  ): Unit = {
    val variable = astOpt[LogicalVariable](ctx.variable())
    val labelExpression = astOpt[LabelExpression](ctx.labelExpression())
    val pathLength = astOpt[Option[expressions.Range]](ctx.pathLength())
    val properties = astOpt[Expression](ctx.properties())
    val expression = astOpt[Expression](ctx.expression())

    val direction = (ctx.leftArrow() != null, ctx.rightArrow() != null) match {
      case (true, false) => SemanticDirection.INCOMING
      case (false, true) => SemanticDirection.OUTGOING
      case _             => SemanticDirection.BOTH
    }

    ctx.ast = RelationshipPattern(variable, labelExpression, pathLength, properties, expression, direction)(pos(ctx))
  }

  final override def exitNodeLabels(ctx: Cypher5Parser.NodeLabelsContext): Unit = {
    ctx.ast = astSeq[LabelName](ctx.labelType())
  }

  final override def exitNodeLabelsIs(ctx: Cypher5Parser.NodeLabelsIsContext): Unit = {
    val symString = ctx.symbolicNameString()
    ctx.ast =
      ArraySeq(LabelName(symString.ast[String]())(pos(symString))) ++
        astSeq[LabelName](ctx.labelType())
  }

  final override def exitLabelType(ctx: Cypher5Parser.LabelTypeContext): Unit = {
    val child = ctxChild(ctx, 1)
    ctx.ast = LabelName(child.ast())(pos(child))
  }

  override def exitRelType(ctx: Cypher5Parser.RelTypeContext): Unit = {
    val child = ctxChild(ctx, 1)
    ctx.ast = RelTypeName(child.ast())(pos(child))
  }

  final override def exitLabelOrRelType(ctx: Cypher5Parser.LabelOrRelTypeContext): Unit = {
    val child = ctxChild(ctx, 1)
    ctx.ast = LabelOrRelTypeName(child.ast())(pos(child))
  }

  final override def exitLabelExpression(ctx: Cypher5Parser.LabelExpressionContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitLabelExpression4(ctx: Cypher5Parser.LabelExpression4Context): Unit = {
    val children = ctx.children; val size = children.size()
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher5Parser.COLON => colon = true
        case lblCtx: Cypher5Parser.LabelExpression3Context =>
          val rhs = lblCtx.ast[LabelExpression]()
          if (colon) {
            result = ColonDisjunction(result, rhs)(pos(nodeChild(ctx, i - 2)))
            colon = false
          } else {
            result = Disjunctions.flat(result, rhs, pos(nodeChild(ctx, i - 1)), containsIs = false)
          }
        case _ =>
      }
      i += 1
    }
    ctx.ast = result
  }

  final override def exitLabelExpression4Is(
    ctx: Cypher5Parser.LabelExpression4IsContext
  ): Unit = {
    val children = ctx.children; val size = children.size()
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher5Parser.COLON => colon = true
        case lblCtx: Cypher5Parser.LabelExpression3IsContext =>
          val rhs = lblCtx.ast[LabelExpression]()
          if (colon) {
            result = ColonDisjunction(result, rhs, containsIs = true)(pos(nodeChild(ctx, i - 2)))
            colon = false
          } else {
            result = Disjunctions.flat(result, rhs, pos(nodeChild(ctx, i - 1)), containsIs = true)
          }
        case _ =>
      }
      i += 1
    }
    ctx.ast = result
  }

  final override def exitLabelExpression3(ctx: Cypher5Parser.LabelExpression3Context): Unit = {
    val children = ctx.children; val size = children.size()
    // Left most LE2
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher5Parser.COLON => colon = true
        case lblCtx: Cypher5Parser.LabelExpression2Context =>
          val rhs = lblCtx.ast[LabelExpression]()
          if (colon) {
            result = ColonConjunction(result, rhs)(pos(nodeChild(ctx, i - 1)))
            colon = false
          } else {
            result = Conjunctions.flat(result, rhs, pos(nodeChild(ctx, i - 1)), containsIs = false)
          }
        case _ =>
      }
      i += 1
    }
    ctx.ast = result
  }

  final override def exitLabelExpression3Is(
    ctx: Cypher5Parser.LabelExpression3IsContext
  ): Unit = {
    val children = ctx.children; val size = children.size()
    // Left most LE2
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher5Parser.COLON => colon = true
        case lblCtx: Cypher5Parser.LabelExpression2IsContext =>
          val rhs = lblCtx.ast[LabelExpression]()
          if (colon) {
            result = ColonConjunction(result, rhs, containsIs = true)(pos(nodeChild(ctx, i - 1)))
            colon = false
          } else {
            result = Conjunctions.flat(result, rhs, pos(nodeChild(ctx, i - 1)), containsIs = true)
          }
        case _ =>
      }
      i += 1
    }
    ctx.ast = result
  }

  final override def exitLabelExpression2(ctx: Cypher5Parser.LabelExpression2Context): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case 2 => Negation(astChild(ctx, 1))(pos(ctx))
      case _ => ctx.EXCLAMATION_MARK().asScala.foldRight(lastChild[AstRuleCtx](ctx).ast[LabelExpression]()) {
          case (exclamation, acc) =>
            Negation(acc)(pos(exclamation.getSymbol))
        }
    }
  }

  final override def exitLabelExpression2Is(
    ctx: Cypher5Parser.LabelExpression2IsContext
  ): Unit = {
    ctx.ast = ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast
      case 2 => Negation(astChild(ctx, 1), containsIs = true)(pos(ctx))
      case _ => ctx.EXCLAMATION_MARK().asScala.foldRight(lastChild[AstRuleCtx](ctx).ast[LabelExpression]()) {
          case (exclamation, acc) =>
            Negation(acc, containsIs = true)(pos(exclamation.getSymbol))
        }
    }
  }

  final override def exitLabelExpression1(ctx: Cypher5Parser.LabelExpression1Context): Unit = {
    ctx.ast = ctx match {
      case ctx: ParenthesizedLabelExpressionContext =>
        ctx.labelExpression4().ast
      case ctx: AnyLabelContext =>
        Wildcard()(pos(ctx))
      case ctx: LabelNameContext =>
        var parent = ctx.parent
        var isLabel = 0
        while (isLabel == 0) {
          if (parent == null || parent.getRuleIndex == Cypher5Parser.RULE_postFix) isLabel = 3
          else if (parent.getRuleIndex == Cypher5Parser.RULE_nodePattern) isLabel = 1
          else if (parent.getRuleIndex == Cypher5Parser.RULE_relationshipPattern) isLabel = 2
          else parent = parent.getParent
        }
        isLabel match {
          case 1 =>
            LabelExpression.Leaf(
              LabelName(ctx.symbolicNameString().ast())(pos(ctx))
            )
          case 2 =>
            LabelExpression.Leaf(
              RelTypeName(ctx.symbolicNameString().ast())(pos(ctx))
            )
          case 3 =>
            LabelExpression.Leaf(
              LabelOrRelTypeName(ctx.symbolicNameString().ast())(pos(ctx))
            )
        }
      case _ =>
        throw new IllegalStateException("Parsed an unknown LabelExpression1 type")
    }
  }

  final override def exitLabelExpression1Is(
    ctx: Cypher5Parser.LabelExpression1IsContext
  ): Unit = {
    ctx.ast = ctx match {
      case ctx: ParenthesizedLabelExpressionIsContext =>
        ctx.labelExpression4Is().ast
      case ctx: AnyLabelIsContext =>
        Wildcard(containsIs = true)(pos(ctx))
      case ctx: LabelNameIsContext =>
        var parent = ctx.parent
        var isLabel = 0
        while (isLabel == 0) {
          if (parent == null || parent.getRuleIndex == Cypher5Parser.RULE_postFix) isLabel = 3
          else if (parent.getRuleIndex == Cypher5Parser.RULE_nodePattern) isLabel = 1
          else if (parent.getRuleIndex == Cypher5Parser.RULE_relationshipPattern) isLabel = 2
          else parent = parent.getParent
        }
        isLabel match {
          case 1 =>
            LabelExpression.Leaf(
              LabelName(ctx.symbolicLabelNameString().ast())(pos(ctx)),
              containsIs = true
            )
          case 2 =>
            LabelExpression.Leaf(
              RelTypeName(ctx.symbolicLabelNameString().ast())(pos(ctx)),
              containsIs = true
            )
          case 3 =>
            LabelExpression.Leaf(
              LabelOrRelTypeName(ctx.symbolicLabelNameString().ast())(pos(ctx)),
              containsIs = true
            )
        }
      case _ =>
        throw new IllegalStateException("Parsed an unknown LabelExpression1Is type")
    }
  }

  final override def exitInsertNodePattern(
    ctx: Cypher5Parser.InsertNodePatternContext
  ): Unit = {
    val variable =
      if (ctx.variable() != null) Some(ctx.variable().ast[LogicalVariable]()) else None
    val labelExpression =
      if (ctx.insertNodeLabelExpression() != null) Some(ctx.insertNodeLabelExpression().ast[LabelExpression]())
      else None
    val properties =
      if (ctx.properties() != null) Some(ctx.properties().ast[Expression]()) else None
    ctx.ast = NodePattern(variable, labelExpression, properties, None)(pos(ctx))

  }

  final override def exitInsertNodeLabelExpression(
    ctx: Cypher5Parser.InsertNodeLabelExpressionContext
  ): Unit = {
//    ctx.ast = Leaf(ctx.insertLabelConjunction().ast(), containsIs = ctx.IS != null)
    val containsIs = ctx.IS != null
    val children = ctx.children; val size = children.size()
    // Left most LE2
    val firstChild = ctxChild(ctx, 1)
    var result: LabelExpression = Leaf(LabelName(firstChild.ast())(pos(firstChild)), containsIs)
    if (size > 2) {
      var colon = false
      var i = 2
      while (i < size) {
        children.get(i) match {
          case node: TerminalNode if node.getSymbol.getType == Cypher5Parser.COLON => colon = true
          case lblCtx: Cypher5Parser.SymbolicNameStringContext =>
            val rhs = Leaf(LabelName(lblCtx.ast())(pos(lblCtx)), containsIs = ctx.IS != null)
            if (colon) {
              result = ColonConjunction(result, rhs, containsIs)(pos(nodeChild(ctx, i - 1)))
              colon = false
            } else {
              result = Conjunctions.flat(result, rhs, pos(nodeChild(ctx, i - 1)), containsIs)
            }
          case _ =>
        }
        i += 1
      }

    }
    ctx.ast = result
  }

  final override def exitInsertRelationshipPattern(
    ctx: Cypher5Parser.InsertRelationshipPatternContext
  ): Unit = {
    val hasRightArrow = ctx.rightArrow() != null
    val hasLeftArrow = ctx.leftArrow() != null
    val variable =
      if (ctx.variable() != null) Some(ctx.variable().ast[LogicalVariable]()) else None
    val labelExpression =
      if (ctx.insertRelationshipLabelExpression() != null)
        Some(ctx.insertRelationshipLabelExpression().ast[LabelExpression]())
      else None
    val properties =
      if (ctx.properties() != null) Some(ctx.properties().ast[Expression]()) else None
    val direction =
      if (hasLeftArrow && hasRightArrow) SemanticDirection.BOTH
      else if (!hasLeftArrow && !hasRightArrow) SemanticDirection.BOTH
      else if (hasLeftArrow) SemanticDirection.INCOMING
      else SemanticDirection.OUTGOING

    ctx.ast = RelationshipPattern(variable, labelExpression, None, properties, None, direction)(pos(ctx))

  }

  final override def exitInsertRelationshipLabelExpression(
    ctx: Cypher5Parser.InsertRelationshipLabelExpressionContext
  ): Unit = {
    val symbolicNameString = ctx.symbolicNameString()
    ctx.ast = Leaf(RelTypeName(symbolicNameString.ast())(pos(symbolicNameString)), containsIs = ctx.IS != null)
  }

  final override def exitLeftArrow(
    ctx: Cypher5Parser.LeftArrowContext
  ): Unit = {}

  final override def exitArrowLine(
    ctx: Cypher5Parser.ArrowLineContext
  ): Unit = {}

  final override def exitRightArrow(
    ctx: Cypher5Parser.RightArrowContext
  ): Unit = {}

}
