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

import org.neo4j.codegen.api.CodeGeneration.ByteCodeGeneration
import org.neo4j.codegen.api.CodeGeneration.CodeSaver
import org.neo4j.codegen.api.CodeGeneration.SourceCodeGeneration
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.NestedPlanArgumentConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.compiled.expressions.CachingExpressionCompilerCache
import org.neo4j.cypher.internal.runtime.compiled.expressions.CachingExpressionCompilerTracer
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledExpressionContext
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.NullExpressionConversionLogger
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.logging.AssertableLogProvider

class CompiledExpressionConverterTest extends CypherFunSuite with AstConstructionTestSupport {

  val compiledExpressionsContext = CompiledExpressionContext(
    new CachingExpressionCompilerCache(TestExecutorCaffeineCacheFactory),
    CachingExpressionCompilerTracer.NONE
  )

  test("should log unexpected errors") {
    // Given
    val physicalPlan = PhysicalPlan(
      null,
      0,
      new SlotConfigurations,
      new ArgumentSizes,
      new ApplyPlans,
      new NestedPlanArgumentConfigurations,
      new AvailableExpressionVariables,
      ParameterMapping.empty
    )

    val logByteCode = new AssertableLogProvider(true)
    val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator()
    val converterByteCode = new CompiledExpressionConverter(
      logByteCode.getLog("test"),
      physicalPlan,
      ReadTokenContext.EMPTY,
      readOnly = false,
      parallelExecution = false,
      codeGenerationMode = ByteCodeGeneration(new CodeSaver(false, false)),
      neverFail = true,
      compiledExpressionsContext = compiledExpressionsContext,
      logger = NullExpressionConversionLogger,
      anonymousVariableNameGenerator = anonymousVariableNameGenerator
    )
    val logSourceCode = new AssertableLogProvider(true)
    val converterSourceCode = new CompiledExpressionConverter(
      logSourceCode.getLog("test"),
      physicalPlan,
      ReadTokenContext.EMPTY,
      readOnly = false,
      parallelExecution = false,
      codeGenerationMode = SourceCodeGeneration(new CodeSaver(false, false)),
      neverFail = true,
      compiledExpressionsContext = compiledExpressionsContext,
      logger = NullExpressionConversionLogger,
      anonymousVariableNameGenerator = anonymousVariableNameGenerator
    )

    // When
    // There is a limit of 65535 on the length of a String literal, so by exceeding that limit
    // we trigger a compilation error
    val e = add(literalString("*" * (65535 + 1)), literalString("*"))

    // Then
    converterByteCode.toCommandExpression(Id.INVALID_ID, e, mock[ExpressionConverters]) should equal(None)
    logByteCode.serialize should include(s"Failed to compile expression: $e")
    converterSourceCode.toCommandExpression(Id.INVALID_ID, e, mock[ExpressionConverters]) should equal(None)
    logSourceCode.serialize should include(s"Failed to compile expression: $e")
  }
}
