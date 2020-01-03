/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v3_5.util.AssertionRunner.Thunk
import org.neo4j.cypher.internal.v3_5.util.Foldable._
import org.neo4j.cypher.internal.v3_5.util.{AssertionRunner, InternalException}

object OwningPipeAsserter {

  def assertAllExpressionsHaveAnOwningPipe(pipe: Pipe): Unit = {
    AssertionRunner.runUnderAssertion(new Thunk {
      override def apply(): Unit = {
        pipe.treeFold(()) {
          case e: Expression =>
            e.owningPipe.getOrElse(
              throw new InternalException(
                s"""Expressions need to be registered with it's owning Pipe, so the profiling knows where to report db-hits
                   |Top level pipe: $pipe
                   |Expression: $e
                 """.stripMargin))
            acc => (acc, Some(identity))
        }
      }
    })
  }
}
