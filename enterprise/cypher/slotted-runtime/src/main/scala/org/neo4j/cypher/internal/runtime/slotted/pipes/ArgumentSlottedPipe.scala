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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

case class ArgumentSlottedPipe(slots: SlotConfiguration,
                               argumentSize:Size)
                              (val id: Id = Id.INVALID_ID)
  extends Pipe {

  def internalCreateResults(state: QueryState): Iterator[SlottedExecutionContext] = {
    val context = SlottedExecutionContext(slots)
    state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
    Iterator(context)
  }
}
