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

trait LabelExpressionBuilder extends CypherParserListener {

  final override def exitNodeLabels(
    ctx: CypherParser.NodeLabelsContext
  ): Unit = {}

  final override def exitNodeLabelsIs(
    ctx: CypherParser.NodeLabelsIsContext
  ): Unit = {}

  final override def exitLabelExpressionPredicate(
    ctx: CypherParser.LabelExpressionPredicateContext
  ): Unit = {}

  final override def exitLabelOrRelType(
    ctx: CypherParser.LabelOrRelTypeContext
  ): Unit = {}

  final override def exitLabelOrRelTypes(
    ctx: CypherParser.LabelOrRelTypesContext
  ): Unit = {}

  final override def exitLabelExpression(
    ctx: CypherParser.LabelExpressionContext
  ): Unit = {}

  final override def exitLabelExpression4(
    ctx: CypherParser.LabelExpression4Context
  ): Unit = {}

  final override def exitLabelExpression4Is(
    ctx: CypherParser.LabelExpression4IsContext
  ): Unit = {}

  final override def exitLabelExpression3(
    ctx: CypherParser.LabelExpression3Context
  ): Unit = {}

  final override def exitLabelExpression3Is(
    ctx: CypherParser.LabelExpression3IsContext
  ): Unit = {}

  final override def exitLabelExpression2(
    ctx: CypherParser.LabelExpression2Context
  ): Unit = {}

  final override def exitLabelExpression2Is(
    ctx: CypherParser.LabelExpression2IsContext
  ): Unit = {}

  final override def exitLabelExpression1(
    ctx: CypherParser.LabelExpression1Context
  ): Unit = {}

  final override def exitLabelExpression1Is(
    ctx: CypherParser.LabelExpression1IsContext
  ): Unit = {}

  final override def exitInsertNodeLabelExpression(
    ctx: CypherParser.InsertNodeLabelExpressionContext
  ): Unit = {}

  final override def exitInsertRelationshipLabelExpression(
    ctx: CypherParser.InsertRelationshipLabelExpressionContext
  ): Unit = {}

  final override def exitInsertLabelConjunction(
    ctx: CypherParser.InsertLabelConjunctionContext
  ): Unit = {}
}
