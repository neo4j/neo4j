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
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser.AnyLabelContext
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser.AnyLabelIsContext
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser.LabelNameContext
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser.LabelNameIsContext
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser.ParenthesizedLabelExpressionContext
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser.ParenthesizedLabelExpressionIsContext
import org.neo4j.cypher.internal.parser.v6.Cypher6ParserListener

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait LabelExpressionBuilder extends Cypher6ParserListener {

  final override def exitNodePattern(
    ctx: Cypher6Parser.NodePatternContext
  ): Unit = {
    val variable = astOpt[LogicalVariable](ctx.variable())
    val labelExpression = astOpt[LabelExpression](ctx.labelExpression())
    val properties = astOpt[Expression](ctx.properties())
    val expression = astOpt[Expression](ctx.expression())
    ctx.ast = NodePattern(variable, labelExpression, properties, expression)(pos(ctx))
  }

  final override def exitRelationshipPattern(
    ctx: Cypher6Parser.RelationshipPatternContext
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

  final override def exitNodeLabels(ctx: Cypher6Parser.NodeLabelsContext): Unit = {
    ctx.ast = (astSeq[LabelName](ctx.labelType()), astSeq[Expression](ctx.dynamicLabelType()))
  }

  final override def exitNodeLabelsIs(ctx: Cypher6Parser.NodeLabelsIsContext): Unit = {
    val labelTypes = ArraySeq.newBuilder[LabelName]
      .addAll(Option(ctx.symbolicNameString()).map(c => LabelName(c.ast())(pos(c))))
      .addAll(astSeq(ctx.labelType()))
      .result()
    val dynamicLabels = ArraySeq.newBuilder[Expression]
      .addAll(astOpt(ctx.dynamicExpression()))
      .addAll(astSeq(ctx.dynamicLabelType()))
      .result()
    ctx.ast = (labelTypes, dynamicLabels)
  }

  final override def exitDynamicExpression(ctx: Cypher6Parser.DynamicExpressionContext): Unit = {
    ctx.ast = ctxChild(ctx, 2).ast
  }

  final override def exitDynamicLabelType(ctx: Cypher6Parser.DynamicLabelTypeContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitLabelType(ctx: Cypher6Parser.LabelTypeContext): Unit = {
    val child = ctxChild(ctx, 1)
    ctx.ast = LabelName(child.ast())(pos(child))
  }

  override def exitRelType(ctx: Cypher6Parser.RelTypeContext): Unit = {
    val child = ctxChild(ctx, 1)
    ctx.ast = RelTypeName(child.ast())(pos(child))
  }

  final override def exitLabelOrRelType(ctx: Cypher6Parser.LabelOrRelTypeContext): Unit = {
    val child = ctxChild(ctx, 1)
    ctx.ast = LabelOrRelTypeName(child.ast())(pos(child))
  }

  final override def exitLabelExpression(ctx: Cypher6Parser.LabelExpressionContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitLabelExpression4(ctx: Cypher6Parser.LabelExpression4Context): Unit = {
    val children = ctx.children; val size = children.size()
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher6Parser.COLON => colon = true
        case lblCtx: Cypher6Parser.LabelExpression3Context =>
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
    ctx: Cypher6Parser.LabelExpression4IsContext
  ): Unit = {
    val children = ctx.children; val size = children.size()
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher6Parser.COLON => colon = true
        case lblCtx: Cypher6Parser.LabelExpression3IsContext =>
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

  final override def exitLabelExpression3(ctx: Cypher6Parser.LabelExpression3Context): Unit = {
    val children = ctx.children; val size = children.size()
    // Left most LE2
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher6Parser.COLON => colon = true
        case lblCtx: Cypher6Parser.LabelExpression2Context =>
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
    ctx: Cypher6Parser.LabelExpression3IsContext
  ): Unit = {
    val children = ctx.children; val size = children.size()
    // Left most LE2
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[LabelExpression]()
    var colon = false
    var i = 1
    while (i < size) {
      children.get(i) match {
        case node: TerminalNode if node.getSymbol.getType == Cypher6Parser.COLON => colon = true
        case lblCtx: Cypher6Parser.LabelExpression2IsContext =>
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

  final override def exitLabelExpression2(ctx: Cypher6Parser.LabelExpression2Context): Unit = {
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
    ctx: Cypher6Parser.LabelExpression2IsContext
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

  final override def exitLabelExpression1(ctx: Cypher6Parser.LabelExpression1Context): Unit = {
    ctx.ast = ctx match {
      case ctx: ParenthesizedLabelExpressionContext =>
        ctx.labelExpression4().ast
      case ctx: AnyLabelContext =>
        Wildcard()(pos(ctx))
      case ctx: LabelNameContext =>
        var parent = ctx.parent
        var isLabel = 0
        while (isLabel == 0) {
          if (parent == null || parent.getRuleIndex == Cypher6Parser.RULE_postFix) isLabel = 3
          else if (parent.getRuleIndex == Cypher6Parser.RULE_nodePattern) isLabel = 1
          else if (parent.getRuleIndex == Cypher6Parser.RULE_relationshipPattern) isLabel = 2
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
    ctx: Cypher6Parser.LabelExpression1IsContext
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
          if (parent == null || parent.getRuleIndex == Cypher6Parser.RULE_postFix) isLabel = 3
          else if (parent.getRuleIndex == Cypher6Parser.RULE_nodePattern) isLabel = 1
          else if (parent.getRuleIndex == Cypher6Parser.RULE_relationshipPattern) isLabel = 2
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
    ctx: Cypher6Parser.InsertNodePatternContext
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
    ctx: Cypher6Parser.InsertNodeLabelExpressionContext
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
          case node: TerminalNode if node.getSymbol.getType == Cypher6Parser.COLON => colon = true
          case lblCtx: Cypher6Parser.SymbolicNameStringContext =>
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
    ctx: Cypher6Parser.InsertRelationshipPatternContext
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
    ctx: Cypher6Parser.InsertRelationshipLabelExpressionContext
  ): Unit = {
    val symbolicNameString = ctx.symbolicNameString()
    ctx.ast = Leaf(RelTypeName(symbolicNameString.ast())(pos(symbolicNameString)), containsIs = ctx.IS != null)
  }

  final override def exitLeftArrow(
    ctx: Cypher6Parser.LeftArrowContext
  ): Unit = {}

  final override def exitArrowLine(
    ctx: Cypher6Parser.ArrowLineContext
  ): Unit = {}

  final override def exitRightArrow(
    ctx: Cypher6Parser.RightArrowContext
  ): Unit = {}

}
