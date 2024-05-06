/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.evaluator

import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator.CONVERTERS
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator.NULL_CURSOR_FACTORY
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QuerySelectivityTrackers
import org.neo4j.cypher.internal.runtime.SelectivityTracker
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.SelectivityTrackerStorage
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

class SimpleInternalExpressionEvaluator extends InternalExpressionEvaluator {

  override def evaluate(expression: String): AnyValue =
    errorContext(expression) {
      val parsedExpression = SimpleInternalExpressionEvaluator.ExpressionParser.parse(expression)
      doEvaluate(parsedExpression, MapValue.EMPTY, CypherRow.empty)
    }

  override def evaluate(
    expression: Expression,
    params: MapValue = MapValue.EMPTY,
    context: CypherRow = CypherRow.empty
  ): AnyValue = errorContext(expression.toString) {
    doEvaluate(expression, params, context)
  }

  def errorContext[T](expr: String)(block: => T): T =
    try block
    catch {
      case e: Exception =>
        throw new EvaluationException(s"Failed to evaluate expression $expr", e)
    }

  def doEvaluate(expression: Expression, params: MapValue, context: CypherRow): AnyValue = {
    val (expr, paramArray) = withSlottedParams(expression, params)
    val allocated = expressionVariableAllocation.allocate(expr)
    val state = queryState(allocated.nExpressionSlots, paramArray)
    val commandExpr = CONVERTERS.toCommandExpression(Id.INVALID_ID, allocated.rewritten)
    commandExpr(context, state)
  }

  def queryState(nExpressionSlots: Int, slottedParams: Array[AnyValue]) =
    new QueryState(
      query = null,
      resources = null,
      params = slottedParams,
      cursors = new ExpressionCursors(NULL_CURSOR_FACTORY, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE),
      queryIndexes = Array.empty[IndexReadSession],
      selectivityTrackerStorage = SimpleInternalExpressionEvaluator.alwaysNewSelectivityTrackerStorage,
      nodeLabelTokenReadSession = None,
      relTypeTokenReadSession = None,
      expressionVariables = new Array(nExpressionSlots),
      subscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
      queryMemoryTracker = null,
      memoryTrackerForOperatorProvider = null
    )

  private def withSlottedParams(input: Expression, params: MapValue): (Expression, Array[AnyValue]) = {
    val mapping: ParameterMapping = input.folder.treeFold(ParameterMapping.empty) {
      case Parameter(name, _, _) => acc => TraverseChildren(acc.updated(name))
    }

    val rewritten = input.endoRewrite(bottomUp(Rewriter.lift {
      case Parameter(name, typ, _) => ParameterFromSlot(mapping.offsetFor(name), name, typ)
    }))

    val paramArray = createParameterArray(params, mapping)
    (rewritten, paramArray)
  }
}

object SimpleInternalExpressionEvaluator {

  // to avoid growing tracker count indefinitely in `CONVERTERS`
  private val noopSelectivityTrackerRegistrator = new SelectivityTrackerRegistrator {
    override def register(): Int = 0
    override def result(): QuerySelectivityTrackers = noopQuerySelectivityTrackers
  }

  private val noopQuerySelectivityTrackers = new QuerySelectivityTrackers(0) {
    override def initializeTrackers(): SelectivityTrackerStorage = alwaysNewSelectivityTrackerStorage
  }

  val alwaysNewSelectivityTrackerStorage: SelectivityTrackerStorage = new SelectivityTrackerStorage(0) {

    override def get(trackerIndex: Int, predicatesCount: Int): SelectivityTracker =
      new SelectivityTracker(predicatesCount)
  }

  private val CONVERTERS =
    new ExpressionConverters(
      None,
      CommunityExpressionConverter(
        ReadTokenContext.EMPTY,
        new AnonymousVariableNameGenerator(),
        noopSelectivityTrackerRegistrator,
        CypherRuntimeConfiguration.defaultConfiguration,
        SemanticTable()
      )
    )

  object ExpressionParser {

    def parse(text: String): Expression = JavaccRule.Expression.apply(text)
  }

  private val NULL_CURSOR_FACTORY = new CursorFactory {
    override def allocateNodeCursor(cursorContext: CursorContext, memoryTracker: MemoryTracker): NodeCursor = null

    override def allocateFullAccessNodeCursor(cursorContext: CursorContext): NodeCursor = null

    override def allocateRelationshipScanCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): RelationshipScanCursor = null

    override def allocateFullAccessRelationshipScanCursor(cursorContext: CursorContext): RelationshipScanCursor = null

    override def allocateRelationshipTraversalCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): RelationshipTraversalCursor = null

    override def allocateFullAccessRelationshipTraversalCursor(cursorContext: CursorContext)
      : RelationshipTraversalCursor = null

    override def allocatePropertyCursor(cursorContext: CursorContext, memoryTracker: MemoryTracker): PropertyCursor =
      null

    override def allocateFullAccessPropertyCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): PropertyCursor = null

    override def allocateNodeValueIndexCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): NodeValueIndexCursor = null

    override def allocateFullAccessNodeValueIndexCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): NodeValueIndexCursor = null

    override def allocateFullAccessRelationshipValueIndexCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): RelationshipValueIndexCursor = null

    override def allocateNodeLabelIndexCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): NodeLabelIndexCursor = null

    override def allocateFullAccessNodeLabelIndexCursor(cursorContext: CursorContext): NodeLabelIndexCursor = null

    override def allocateRelationshipValueIndexCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): RelationshipValueIndexCursor = null

    override def allocateRelationshipTypeIndexCursor(
      cursorContext: CursorContext,
      memoryTracker: MemoryTracker
    ): RelationshipTypeIndexCursor = null

    override def allocateFullAccessRelationshipTypeIndexCursor(cursorContext: CursorContext)
      : RelationshipTypeIndexCursor = null
  }
}
