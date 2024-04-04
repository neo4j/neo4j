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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Attributes

case class EagerRewriterWithFallback(
  primaryRewriter: EagerRewriter,
  fallbackRewriter: EagerRewriter,
  attributes: Attributes[LogicalPlan]
) extends EagerRewriter(attributes) {

  override def eagerize(
    plan: LogicalPlan,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): LogicalPlan = {
    try {
      primaryRewriter.eagerize(plan, semanticTable, anonymousVariableNameGenerator)
    } catch {
      case NonFatalCypherError(primaryThrowable) =>
        try {
          fallbackRewriter.eagerize(plan, semanticTable, anonymousVariableNameGenerator)
        } catch {
          case NonFatalCypherError(fallbackThrowable) =>
            fallbackThrowable.addSuppressed(primaryThrowable)
            throw fallbackThrowable
        }
    }
  }
}
