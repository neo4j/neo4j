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
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.cypher.internal.v4_0.util.Foldable._
import org.neo4j.exceptions.InternalException

object OwningPipeAsserter {

  def assertAllExpressionsHaveAnOwningPipe(pipe: Pipe): Unit = {
    AssertionRunner.runUnderAssertion(() =>
      pipe.treeFold(()) {
        case e: Expression =>
          try {
            e.owningPipe
          } catch {
            case exp: InternalException =>
              throw new InternalException(s"${exp.getMessage}\nTop level pipe: $pipe\nExpression: $e")
          }
          acc => (acc, Some(identity))
      }
    )
  }
}
