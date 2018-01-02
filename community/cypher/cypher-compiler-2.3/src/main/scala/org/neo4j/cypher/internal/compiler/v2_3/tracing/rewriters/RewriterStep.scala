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
package org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.Rewriter

object RewriterStep {
   implicit def namedProductRewriter(p: Product with Rewriter): ApplyRewriter = ApplyRewriter(p.productPrefix, p)

   def enableCondition(p: Condition) = EnableRewriterCondition(RewriterCondition(p.name, p))
   def disableCondition(p: Condition) = DisableRewriterCondition(RewriterCondition(p.name, p))
 }

sealed trait RewriterStep
final case class ApplyRewriter(name: String, rewriter: Rewriter) extends RewriterStep
final case class EnableRewriterCondition(cond: RewriterCondition) extends RewriterStep
final case class DisableRewriterCondition(cond: RewriterCondition) extends RewriterStep

trait Condition extends (Any => Seq[String]) {
   def name: String
}
