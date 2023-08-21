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
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.kernel.impl.util.ValueUtils

/**
 * Rewrites a logical plan so that parameter access is done by offset into an array instead of accessing
 * a hash map.
 */
case object slottedParameters {

  def apply(input: LogicalPlan): (LogicalPlan, ParameterMapping) = {
    // This may look like it is dangerous, what if we both have a normal parameter and an implicit
    // procedure argument by the same name? This will not happen since implicit parameters is only supported
    // for stand-alone procedures, e.g `CALL my.proc` with `{input1: 'foo', input2: 1337}`
    val mapping: ParameterMapping = input.folder.treeFold(ParameterMapping.empty) {
      case Parameter(name, _, _) => acc => TraverseChildren(acc.updated(name))
      case ImplicitProcedureArgument(name, _, defaultValue) =>
        acc => TraverseChildren(acc.updated(name, ValueUtils.of(defaultValue)))
    }

    val rewriter = bottomUp(Rewriter.lift {
      case Parameter(name, typ, _)                 => ParameterFromSlot(mapping.offsetFor(name), name, typ)
      case ImplicitProcedureArgument(name, typ, _) => ParameterFromSlot(mapping.offsetFor(name), name, typ)
    })

    (input.endoRewrite(rewriter), mapping)
  }
}
