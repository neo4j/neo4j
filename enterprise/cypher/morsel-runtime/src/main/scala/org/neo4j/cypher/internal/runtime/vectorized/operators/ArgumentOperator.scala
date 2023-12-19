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
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.InternalException

class ArgumentOperator extends Operator {
  override def operate(message: Message, data: Morsel, context: QueryContext, state: QueryState): Continuation = {
    data.validRows = 1

    if(!message.isInstanceOf[StartLeafLoop])
      throw new InternalException("Weird message received")

    EndOfLoop(message.iterationState)
  }

  override def addDependency(pipeline: Pipeline): Dependency = Lazy(pipeline)
}
