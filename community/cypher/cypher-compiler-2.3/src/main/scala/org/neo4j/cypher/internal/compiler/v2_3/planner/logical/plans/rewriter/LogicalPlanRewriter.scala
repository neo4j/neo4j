/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, repeat}

case class LogicalPlanRewriter(rewriterSequencer: String => RewriterStepSequencer) extends Rewriter {
  val instance: Rewriter = repeat(rewriterSequencer("LogicalPlanRewriter")(
    fuseSelections,
    unnestApply,
    simplifyEquality,
    unnestOptional,
    predicateRemovalThroughJoins,
    removeIdenticalPlans
  ).rewriter)

  def apply(that: AnyRef) = instance(that)
}
