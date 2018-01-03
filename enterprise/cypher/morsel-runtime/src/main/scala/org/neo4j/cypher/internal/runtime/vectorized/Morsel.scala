/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
