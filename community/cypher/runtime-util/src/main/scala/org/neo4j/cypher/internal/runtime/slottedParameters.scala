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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.RunQueryAt
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue

/**
 * Rewrites a logical plan so that parameter access is done by offset into an array instead of accessing
 * a hash map.
 */
case object slottedParameters {

  private case class ParameterFoldState(mapping: ParameterMapping, excluded: Set[Parameter]) {

    def exclude(parameters: IterableOnce[Parameter]): ParameterFoldState =
      copy(excluded = excluded ++ parameters)

    def withParameter(parameter: Parameter): ParameterFoldState =
      if (excluded.contains(parameter)) this
      else copy(mapping = mapping.updated(parameter.name))

    def withProcArgument(name: String, value: AnyValue): ParameterFoldState =
      copy(mapping = mapping.updated(name, value))
  }

  private object ParameterFoldState {
    def empty = ParameterFoldState(ParameterMapping.empty, Set.empty)
  }

  def apply(input: LogicalPlan): (LogicalPlan, ParameterMapping) = {
    // This may look like it is dangerous, what if we both have a normal parameter and an implicit
    // procedure argument by the same name? This will not happen since implicit parameters is only supported
    // for stand-alone procedures, e.g `CALL my.proc` with `{input1: 'foo', input2: 1337}`
    val mapping: ParameterMapping = input.folder.treeFold(ParameterFoldState.empty) {
      // RunQueryAt parameters are passed to the subquery executor so should not be considered valid for the outer executor
      case p: RunQueryAt => acc => TraverseChildren(acc.exclude(p.importsAsParameters.keys))
      case p: Parameter  => acc => TraverseChildren(acc.withParameter(p))
      case ImplicitProcedureArgument(name, _, defaultValue) =>
        acc => TraverseChildren(acc.withProcArgument(name, ValueUtils.of(defaultValue)))
    }.mapping

    val rewriter = bottomUp(Rewriter.lift {
      case Parameter(name, typ, _) if mapping.contains(name) => ParameterFromSlot(mapping.offsetFor(name), name, typ)
      case ImplicitProcedureArgument(name, typ, _)           => ParameterFromSlot(mapping.offsetFor(name), name, typ)
    })

    (input.endoRewrite(rewriter), mapping)
  }
}
