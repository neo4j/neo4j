/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.codegen.api.CodeGeneration.CodeGenerationMode
import org.neo4j.cypher.internal.Assertion.assertionsEnabled
import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.compiler.CodeGenerationFailedNotification
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.AbstractExpressionCompilerFront.isWhitelisted
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledExpressionContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledGroupingExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledProjection
import org.neo4j.cypher.internal.runtime.compiled.expressions.StandaloneExpressionCompiler
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConversionLogger
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExtendedExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.InequalitySeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointBoundingBoxSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointDistanceSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PrefixSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.RandFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.expressions.CompiledExpressionConverter.COMPILE_LIMIT
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.logging.InternalLog
import org.neo4j.values.AnyValue

import scala.collection.mutable

class CompiledExpressionConversionLogger extends ExpressionConversionLogger {
  private val _warnings: mutable.Set[InternalNotification] = mutable.Set.empty

  override def failedToConvertExpression(expression: expressions.Expression): Unit = {
    if (!containsWhitelisted(expression)) {
      _warnings += CodeGenerationFailedNotification(s"Failed to compile expression: $expression")
    }
  }

  override def failedToConvertProjection(projection: Map[String, expressions.Expression]): Unit = {
    if (!projection.values.exists(containsWhitelisted)) {
      _warnings += CodeGenerationFailedNotification(s"Failed to compile projection: $projection")
    }
  }

  private def containsWhitelisted(expression: expressions.Expression): Boolean = {
    isWhitelisted(expression) || expression.subExpressions.exists(isWhitelisted)
  }

  override def warnings: Set[InternalNotification] = _warnings.toSet
}

class CompiledExpressionConverter(
  log: InternalLog,
  physicalPlan: PhysicalPlan,
  tokenContext: ReadTokenContext,
  readOnly: Boolean,
  parallelExecution: Boolean,
  codeGenerationMode: CodeGenerationMode,
  compiledExpressionsContext: CompiledExpressionContext,
  logger: ExpressionConversionLogger,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  neverFail: Boolean = false
) extends ExpressionConverter {

  // uses an inner converter to simplify compliance with Expression trait
  private val inner = new ExpressionConverters(
    SlottedExpressionConverters(physicalPlan),
    CommunityExpressionConverter(tokenContext, anonymousVariableNameGenerator)
  )

  override def toCommandExpression(
    id: Id,
    expression: expressions.Expression,
    self: ExpressionConverters
  ): Option[Expression] = expression match {
    // we don't deal with aggregations
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None
    case _: CountStar                                                          => None

    case e: PrefixSeekRangeWrapper =>
      Some(PrefixSeekRangeExpression(e.range.map(self.toCommandExpression(id, _))))
    case e: InequalitySeekRangeWrapper =>
      Some(InequalitySeekRangeExpression(e.range.mapBounds(self.toCommandExpression(id, _))))
    case e: PointDistanceSeekRangeWrapper =>
      Some(PointDistanceSeekRangeExpression(e.range.map(self.toCommandExpression(id, _))))
    case e: PointBoundingBoxSeekRangeWrapper =>
      Some(PointBoundingBoxSeekRangeExpression(e.range.map(self.toCommandExpression(id, _))))

    case e if sizeOf(e) > COMPILE_LIMIT =>
      try {
        log.debug(s"Compiling expression: $expression")
        val maybeCompiledExpression = StandaloneExpressionCompiler.default(
          physicalPlan.slotConfigurations(id),
          readOnly,
          parallelExecution,
          codeGenerationMode,
          compiledExpressionsContext,
          tokenContext
        )
          .compileExpression(e, id)
        maybeCompiledExpression match {
          case Some(compiledExpression) =>
            Some(CompileWrappingExpression(compiledExpression, inner.toCommandExpression(id, expression)))
          case None =>
            logger.failedToConvertExpression(expression)
            None
        }
      } catch {
        case NonFatalCypherError(t) =>
          // Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
          // to load invalid bytecode, whatever is the case we should silently fallback to the next expression
          // converter
          if (shouldThrow) throw t
          else log.debug(s"Failed to compile expression: $e", t)
          logger.failedToConvertExpression(expression)
          None
      }

    case _ => None
  }

  private def sizeOf(expression: expressions.Expression) = expression.folder.treeCount {
    case _: expressions.Expression => true
  }

  override def toCommandProjection(
    id: Id,
    projections: Map[String, expressions.Expression],
    self: ExpressionConverters
  ): Option[CommandProjection] = {
    try {
      val totalSize = projections.values.foldLeft(0)((acc, current) => acc + sizeOf(current))
      if (totalSize > COMPILE_LIMIT) {
        log.debug(s" Compiling projection: $projections")
        val maybeCompiledExpression = StandaloneExpressionCompiler.default(
          physicalPlan.slotConfigurations(id),
          readOnly,
          parallelExecution,
          codeGenerationMode,
          compiledExpressionsContext,
          tokenContext
        )
          .compileProjection(projections, id)
        maybeCompiledExpression match {
          case Some(compiledProjection) =>
            Some(CompileWrappingProjection(compiledProjection, projections.isEmpty))
          case None =>
            logger.failedToConvertProjection(projections)
            None
        }
      } else None
    } catch {
      case NonFatalCypherError(t) =>
        // Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        // to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        // converter
        if (shouldThrow) throw t
        else log.debug(s"Failed to compile projection: $projections", t)
        logger.failedToConvertProjection(projections)
        None
    }
  }

  override def toGroupingExpression(
    id: Id,
    projections: Map[String, expressions.Expression],
    orderToLeverage: collection.Seq[expressions.Expression],
    self: ExpressionConverters
  ): Option[GroupingExpression] = {
    try {
      if (orderToLeverage.nonEmpty) {
        // TODO Support compiled ordered GroupingExpression
        // UPDATE: In theory this should now be supported...
        // REMINDER: once code generation for this case is supported, remember to log compilation failures in ExpressionConversionLogger
        None
      } else {
        val totalSize = projections.values.foldLeft(0)((acc, current) => acc + sizeOf(current))
        if (totalSize > COMPILE_LIMIT) {
          log.debug(s" Compiling grouping expression: $projections")
          val maybeCompiledExpression = StandaloneExpressionCompiler.default(
            physicalPlan.slotConfigurations(id),
            readOnly,
            parallelExecution,
            codeGenerationMode,
            compiledExpressionsContext,
            tokenContext
          )
            .compileGrouping(orderGroupingKeyExpressions(projections, orderToLeverage), id)
          maybeCompiledExpression match {
            case Some(compiledExpression) =>
              Some(CompileWrappingDistinctGroupingExpression(compiledExpression, projections.isEmpty))
            case None =>
              logger.failedToConvertProjection(projections)
              None
          }
        } else None
      }
    } catch {
      case NonFatalCypherError(t) =>
        // Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        // to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        // converter
        if (shouldThrow) throw t
        else log.debug(s"Failed to compile grouping expression: $projections", t)
        logger.failedToConvertProjection(projections)
        None
    }
  }

  private def shouldThrow = !neverFail && assertionsEnabled()
}

object CompiledExpressionConverter {
  private val COMPILE_LIMIT: Int = 2
}

case class CompileWrappingDistinctGroupingExpression(grouping: CompiledGroupingExpression, isEmpty: Boolean)
    extends GroupingExpression {

  override type KeyType = AnyValue

  override def computeGroupingKey(context: ReadableRow, state: QueryState): AnyValue =
    grouping.computeGroupingKey(context, state.query, state.params, state.cursors, state.expressionVariables)

  override def computeOrderedGroupingKey(groupingKey: AnyValue): AnyValue =
    throw new IllegalStateException("Compiled expressions do not support this yet.")

  override def getGroupingKey(context: CypherRow): AnyValue = grouping.getGroupingKey(context)

  override def project(context: WritableRow, groupingKey: AnyValue): Unit =
    grouping.projectGroupingKey(context, groupingKey)
}

case class CompileWrappingProjection(projection: CompiledProjection, isEmpty: Boolean) extends CommandProjection {

  override def project(ctx: ReadWriteRow, state: QueryState): Unit =
    projection.project(ctx, state.query, state.params, state.cursors, state.expressionVariables)
}

case class CompileWrappingExpression(ce: CompiledExpression, legacy: Expression) extends ExtendedExpression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq(legacy)

  override def children: Seq[AstNode[_]] = Seq(legacy)

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    ce.evaluate(row, state.query, state.params, state.cursors, state.expressionVariables)

  override def toString: String = legacy.toString

  override val isDeterministic: Boolean = !legacy.exists {
    case RandFunction() => true
    case _              => false
  }
}
