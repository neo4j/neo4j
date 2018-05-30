/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.opencypher.v9_0.util.InternalException

class ArgumentOperator(longsPerRow: Int, refsPerRow: Int, argumentSize: SlotConfiguration.Size) extends Operator {
  override def operate(message: Message, data: Morsel, context: QueryContext, state: QueryState): Continuation = {
    data.validRows = 1

    if(!message.isInstanceOf[StartLeafLoop])
      throw new InternalException("Weird message received")

    val currentRow = new MorselExecutionContext(data, longsPerRow, refsPerRow, currentRow = 0)
    message.iterationState.copyArgumentStateTo(currentRow, argumentSize.nLongs, argumentSize.nReferences)

    EndOfLoop(message.iterationState)
  }

  override def addDependency(pipeline: Pipeline): Dependency = Lazy(pipeline)
}
