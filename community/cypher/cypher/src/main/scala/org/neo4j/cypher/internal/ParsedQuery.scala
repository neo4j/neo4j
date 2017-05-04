/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.spi.v3_2.TransactionalContextWrapper

import scala.util.Try

trait ParsedQuery {
  protected def trier: Try[{ def isPeriodicCommit: Boolean }]
  def plan(transactionContext: TransactionalContextWrapper, tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any])
  final def isPeriodicCommit: Boolean = trier.map(_.isPeriodicCommit).getOrElse(false)
  final def hasErrors: Boolean = trier.isFailure
  final def onError[T](f: Throwable => T): Option[T] = trier.failed.toOption.map(f)
}
