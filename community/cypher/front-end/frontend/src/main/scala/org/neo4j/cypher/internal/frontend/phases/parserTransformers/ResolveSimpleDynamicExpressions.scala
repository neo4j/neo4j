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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.expressions.DynamicLabelExpression
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.DynamicLeaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUpWithRecorder
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * ResolveSimpleDynamicExpressions resolves dynamic expressions that are either a List of StringLiterals
 * or explicit parameters referencing a single String.
 *
 * Queries with $param = "Label" and $listParam = ["Label1", "Label2"] get converted as follows:
 * (:$($param))-[]->() => (:Label)-[]->()
 * (:$($listParam))-[]->() => (:Label1&Label2)-[]->()
 *
 * Queries with a list of StringLiterals get converted as follows:
 * (:all$(["Label1", "Label2"]))-[]->() => (:Label1&Label2)-[]->()
 * (:any$(["Label1", "Label2"]))-[]->() => (:Label1|Label2)-[]->()
 *
 * Note: By resolving the parameters before runtime we cannot cache the resolved query plan since it would then reference
 * the resolved parameters. This means that the query plan will be recompiled for each new parameter value.
 */
case class ResolveSimpleDynamicExpressions(parameters: MapValue)
    extends StepSequencer.Step
    with ParsePipelineTransformerFactory
    with Transformer[BaseContext, BaseState, BaseState] {

  /**
   * Turns an iterable into a List of label expressions if `f` is defined for all input values.
   */
  private def asLabelExpressions[A](iterable: Iterable[A])(f: PartialFunction[A, LabelExpression])
    : Option[List[LabelExpression]] = {
    val iterator = iterable.iterator
    val labelExpressionsBuilder = List.newBuilder[LabelExpression]
    var isDefined = true
    while (isDefined && iterator.hasNext) {
      val expression = iterator.next()
      isDefined = f.runWith(labelExpressionsBuilder.addOne)(expression)
    }
    Option.when(isDefined)(labelExpressionsBuilder.result())
  }

  private def rewrite(expression: LabelExpression): LabelExpression = expression match {
    case dynamicLeaf @ DynamicLeaf(DynamicLabelExpression(parameter: ExplicitParameter, all), containsIs) =>
      parameters.get(parameter.name) match {
        case textValue: TextValue =>
          Leaf(LabelName(textValue.stringValue())(dynamicLeaf.position), containsIs)
        case listValue: ListValue =>
          asLabelExpressions(listValue.asScala) {
            case textValue: TextValue => Leaf(LabelName(textValue.stringValue())(dynamicLeaf.position), containsIs)
          } match {
            case None                         => dynamicLeaf
            case Some(Nil)                    => dynamicLeaf
            case Some(labelExpression :: Nil) => labelExpression
            case Some(labelExpressions) =>
              if (all) {
                Conjunctions(labelExpressions, containsIs)(dynamicLeaf.position)
              } else {
                Disjunctions(labelExpressions, containsIs)(dynamicLeaf.position)
              }
          }
        case _ => dynamicLeaf
      }
    case dynamicLeaf @ DynamicLeaf(DynamicRelTypeExpression(parameter: ExplicitParameter, all), containsIs) =>
      parameters.get(parameter.name) match {
        case textValue: TextValue =>
          Leaf(RelTypeName(textValue.stringValue())(dynamicLeaf.position), containsIs)
        case listValue: ListValue =>
          asLabelExpressions(listValue.asScala) {
            case textValue: TextValue => Leaf(RelTypeName(textValue.stringValue())(dynamicLeaf.position), containsIs)
          } match {
            case None                         => dynamicLeaf
            case Some(Nil)                    => dynamicLeaf
            case Some(labelExpression :: Nil) => labelExpression
            case Some(labelExpressions) =>
              if (all) {
                dynamicLeaf
              } else {
                Disjunctions(labelExpressions, containsIs)(dynamicLeaf.position)
              }
          }
        case _ => dynamicLeaf
      }
    case dynamicLeaf @ DynamicLeaf(DynamicLabelExpression(StringLiteral(name), _), containsIs) =>
      Leaf(LabelName(name)(dynamicLeaf.position), containsIs)
    case dynamicLeaf @ DynamicLeaf(DynamicRelTypeExpression(StringLiteral(name), _), containsIs) =>
      Leaf(RelTypeName(name)(dynamicLeaf.position), containsIs)
    case dynamicLeaf @ DynamicLeaf(DynamicLabelExpression(ListLiteral(list), all), containsIs) =>
      asLabelExpressions(list) {
        case StringLiteral(labelName) => Leaf(LabelName(labelName)(dynamicLeaf.position), containsIs)
      } match {
        case None                         => dynamicLeaf
        case Some(Nil)                    => dynamicLeaf
        case Some(labelExpression :: Nil) => labelExpression
        case Some(labelExpressions) =>
          if (all) {
            Conjunctions(labelExpressions, containsIs)(dynamicLeaf.position)
          } else {
            Disjunctions(labelExpressions, containsIs)(dynamicLeaf.position)
          }
      }
    case dynamicLeaf @ DynamicLeaf(DynamicRelTypeExpression(ListLiteral(list), all), containsIs) =>
      asLabelExpressions(list) {
        case StringLiteral(labelName) => Leaf(RelTypeName(labelName)(dynamicLeaf.position), containsIs)
      } match {
        case None                         => dynamicLeaf
        case Some(Nil)                    => dynamicLeaf
        case Some(labelExpression :: Nil) => labelExpression
        case Some(labelExpressions) =>
          if (all) {
            dynamicLeaf
          } else {
            Disjunctions(labelExpressions, containsIs)(dynamicLeaf.position)
          }
      }
    case e => e
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement]())

  override def postConditions: Set[StepSequencer.Condition] = Set()

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  /**
   * @return the conditions that this step invalidates as a side-effect of its work.
   */
  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def transform(from: BaseState, context: BaseContext): BaseState = {
    val resolvedParameters = Set.newBuilder[String]
    val rewriter = bottomUpWithRecorder.onRewriter(
      Rewriter.lift {
        case e: LabelExpression => rewrite(e)
      },
      stopper = _.isInstanceOf[Create], // stopper actually works in a top-down way, even when called with bottomUp
      recorder = {
        case (DynamicLeaf(expression, _), _) =>
          expression.expression match {
            case parameter: ExplicitParameter => resolvedParameters.addOne(parameter.name)
            case _: StringLiteral             => ()
            case _: ListLiteral               => ()
            case _ => throw new IllegalStateException("Accidentally rewrote an unexpected expression")
          }
        case _ => throw new IllegalStateException("Accidentally rewrote an unexpected label expression")
      },
      cancellation = CancellationChecker.NeverCancelled
    )

    val rewritten = from.statement().endoRewrite(rewriter)
    val resolvedParamResult = resolvedParameters.result()
    if (resolvedParamResult.isEmpty)
      from.withStatement(rewritten)
    else
      from.withStatement(rewritten)
        .withResolvedParamsAdded(resolvedParameters.result())
  }

  override def name: String = "ResolveSimpleDynamicExpressions"
}
