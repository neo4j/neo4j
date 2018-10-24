/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, MapValue, PathValue}

/**
  * Populates the labels, properties and relationship types of graph entities. Once we remove NodeProxyWrappingNodeValue
  * and RelationshipProxyWrappingValue, this could be repurposed to replace NodeReference with NodeValue.
  */
object ValuePopulation {

  def populate(value: AnyValue): Unit = {
    value match {
      case n: NodeProxyWrappingNodeValue =>
        n.populate()

      case r: RelationshipProxyWrappingValue =>
        r.populate()

      case p: PathValue =>
        for (n <- p.nodes()) populate(n)
        for (r <- p.relationships()) populate(r)

      case l: ListValue =>
        val it = l.iterator()
        while (it.hasNext) {
          populate(it.next())
        }

      case m: MapValue =>
        m.foreach(new ThrowingBiConsumer[String, AnyValue, Exception] {
          override def accept(key: String, value: AnyValue): Unit = populate(value)
        })

      case _ => // it's fine
    }
  }
}
