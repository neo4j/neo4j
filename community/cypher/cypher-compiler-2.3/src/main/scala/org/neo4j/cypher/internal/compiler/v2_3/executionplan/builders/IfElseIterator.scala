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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext

/*
For each element in the input iterator, the ifClause will get a chance to run. If it returns an empty
iterator, the elseClause will get a chance to run.
 */
class IfElseIterator(input: Iterator[ExecutionContext],
                     ifClause: ExecutionContext => Iterator[ExecutionContext],
                     elseClause: ExecutionContext => Iterator[ExecutionContext],
                     finallyClause: () => Unit = () => ())
  extends Iterator[ExecutionContext] {

  private var resultIterator: Iterator[ExecutionContext] = Iterator.empty

  def hasNext: Boolean = {
    if (resultIterator.isEmpty)
      fillBuffer()

    val result = resultIterator.hasNext

    if (!result) {
      finallyClause()
    }

    result
  }

  def next(): ExecutionContext = {
    if (resultIterator.isEmpty)
      fillBuffer()

    resultIterator.next()
  }

  private def fillBuffer() {
    if (input.nonEmpty) {
      val next = input.next()

      resultIterator = ifClause(next)

      if (resultIterator.isEmpty) {
        resultIterator = elseClause(next)
      }
    }
  }
}
