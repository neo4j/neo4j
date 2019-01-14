/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import java.util
import java.util.Collections.newSetFromMap

import org.neo4j.cypher.internal.runtime.CloseableResource
import org.neo4j.helpers.Exceptions

import scala.collection.JavaConverters._

class ResourceManager extends CloseableResource {
  private val resources: util.Set[AutoCloseable] = newSetFromMap(new util.IdentityHashMap[AutoCloseable, java.lang.Boolean]())

  def trace(resource: AutoCloseable): Unit =
    resources.add(resource)

  def release(resource: AutoCloseable): Unit = {
    resource.close()
    if (!resources.remove(resource)) {
      throw new IllegalStateException(s"$resource is not in the resource set $resources")
    }
  }

  def allResources: collection.Set[AutoCloseable] = resources.asScala

  override def close(success: Boolean): Unit = {
    val iterator = resources.iterator()
    var error: Throwable = null
    while (iterator.hasNext) {
      try iterator.next().close()
      catch {
        case t: Throwable => error = Exceptions.chain(error, t)
      }
      iterator.remove()
    }
    if (error != null) throw error
  }
}
