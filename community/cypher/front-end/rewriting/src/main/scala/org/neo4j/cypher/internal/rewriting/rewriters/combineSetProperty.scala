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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetItem
import org.neo4j.cypher.internal.ast.SetProperty
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SetPropertyItems
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case object combineSetProperty extends StepSequencer.Step with DefaultPostCondition with ASTRewriterFactory {
  override def preConditions: Set[StepSequencer.Condition] = Set()
  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = instance

  private def onSameEntity(setItem: SetItem, entity: Expression) = setItem match {
    case SetPropertyItem(Property(map, _), _) => map == entity
    case _                                    => false
  }

  private def combine(entity: Expression, ops: Seq[SetPropertyItem]): SetProperty =
    if (ops.size == 1) ops.head
    else SetPropertyItems(entity, ops.map(o => (o.property.propertyKey, o.expression)))(ops.head.position)

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case s @ SetClause(items) =>
      val newItems = ArrayBuffer.empty[SetItem]
      val itemsArray = items.toArray

      // find all contiguous blocks of SetPropertyItem on the same item, e.g., SET n.p1 = 1, n.p2 = 2
      // we are not allowed to change the order of the SET operations so it is only safe to combine
      // contiguous blocks.
      var i = 0
      while (i < itemsArray.length) {
        val item = itemsArray(i)
        item match {
          case s @ SetPropertyItem(Property(map, _), _) if i < itemsArray.length - 1 =>
            val itemsToCombine: mutable.ArrayBuffer[SetPropertyItem] = ArrayBuffer(s)
            while (i + 1 < itemsArray.length && onSameEntity(itemsArray(i + 1), map)) {
              itemsToCombine += itemsArray(i + 1).asInstanceOf[SetPropertyItem]
              i += 1
            }
            newItems += combine(map, itemsToCombine.toSeq)

          case item =>
            newItems += item
        }
        i += 1
      }
      s.copy(items = newItems.toSeq)(s.position)
  })
}
