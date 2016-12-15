/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.compiler.v3_2.helpers.closing

trait Phase[FROM, TO] extends Transformer[FROM, TO] {
  self =>

  def phase: CompilationPhase

  def description: String

  def transformReporting(from: FROM, context: Context): TO =
    closing(context.tracer.beginPhase(phase)) {
      transform(from, context)
    }
}

trait EndoPhase[K] extends Phase[K, K]

trait VisitorPhase[K] extends EndoPhase[K] {
  override def transform(from: K, context: Context): K = {
    visit(from, context)
    from
  }

  def visit(value: K, context: Context): Unit
}

trait Transformer[FROM, TO] {
  def transform(from: FROM, context: Context): TO

  def andThen[newTO, newContext](other: Transformer[TO, newTO]) =
    new PipeLine(this, other)
}

class PipeLine[From, Temp, To](first: Transformer[From, Temp], after: Transformer[Temp, To])
  extends Transformer[From, To] {
  override def transform(from: From, context: Context): To = {
    val step = first.transform(from, context)
    after.transform(step, context)
  }
}

object OrElse {

  implicit class OrElser[From, To](maybeTransform: Transformer[From, Option[To]]) {
    def orElse(fallback: Transformer[From, To]) = OrElse(maybeTransform, fallback)
  }

}

case class OrElse[From, To](first: Transformer[From, Option[To]], fallback: Transformer[From, To])
  extends Transformer[From, To] {
  override def transform(from: From, context: Context): To = {
    val result = first.transform(from, context)
    result getOrElse fallback.transform(from, context)
  }
}