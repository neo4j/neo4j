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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.internal.schema
import org.neo4j.values.storable.ValueGroup

object KernelAPISupport {
  val RANGE_SEEKABLE_VALUE_GROUPS = Array(ValueGroup.NUMBER,
                                          ValueGroup.TEXT,
                                          ValueGroup.GEOMETRY,
                                          ValueGroup.DATE,
                                          ValueGroup.LOCAL_DATE_TIME,
                                          ValueGroup.ZONED_DATE_TIME,
                                          ValueGroup.LOCAL_TIME,
                                          ValueGroup.ZONED_TIME,
                                          ValueGroup.DURATION,
                                          ValueGroup.BOOLEAN)

  def asKernelIndexOrder(indexOrder: plans.IndexOrder): schema.IndexOrder =
    indexOrder match {
      case plans.IndexOrderAscending => schema.IndexOrder.ASCENDING
      case plans.IndexOrderDescending => schema.IndexOrder.DESCENDING
      case plans.IndexOrderNone => schema.IndexOrder.NONE
    }
}
