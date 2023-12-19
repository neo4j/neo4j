/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeBuilder.RowMapping
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

case class UnionSlottedPipe(lhs: Pipe, rhs: Pipe,
                            lhsMapping: RowMapping,
                            rhsMapping: RowMapping)
                           (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)

    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = left.hasNext || right.hasNext
      override def next(): ExecutionContext =
        if (left.hasNext)
          lhsMapping(left.next(), state)
        else
          rhsMapping(right.next(), state)
    }
  }
}
