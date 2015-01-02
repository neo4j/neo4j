/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters

import org.neo4j.cypher.internal.compiler.v2_2._

object RewriterStep {
   type Named[T] = Product with T

   implicit def namedProductRewriter(p: Named[Rewriter]) = ApplyRewriter(p.productPrefix, p)
   implicit def productRewriterCondition(p: Named[Any => Seq[String]]) = RewriterCondition(p.productPrefix, p)

   def enableCondition(p: Named[Any => Seq[String]]) = EnableRewriterCondition(p)
   def disableCondition(p: Named[Any => Seq[String]]) = DisableRewriterCondition(p)
 }

sealed trait RewriterStep
final case class ApplyRewriter(name: String, rewriter: Rewriter) extends RewriterStep
final case class EnableRewriterCondition(cond: RewriterCondition) extends RewriterStep
final case class DisableRewriterCondition(cond: RewriterCondition) extends RewriterStep
case object EmptyRewriterStep extends RewriterStep
