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
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.values.AnyValue

/*
The lifetime of a Morsel instance is entirely controlled by the Dispatcher. No operator should create Morsels - they
 should only operate on Morsels provided to them
 */
class Morsel(val longs: Array[Long], val refs: Array[AnyValue], var validRows: Int) {
  override def toString = s"Morsel(validRows=$validRows)"
}

object Morsel {
  def create(slots: SlotConfiguration, size: Int): Morsel = {
    val longs = new Array[Long](slots.numberOfLongs * size)
    val refs = new Array[AnyValue](slots.numberOfReferences * size)
    new Morsel(longs, refs, size)
  }
}
