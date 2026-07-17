/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.lang.AutoCloseablePlus.UNTRACKED

class DummyResource extends DefaultCloseListenable {
  private var closed = false

  override def closeInternal(): Unit = {
    this.closed = true
  }

  override def isClosed: Boolean = closed
}

class ThrowingDummyResource(error: Exception) extends DummyResource {

  override def closeInternal(): Unit = {
    super.closeInternal()
    throw error
  }
}

object DummyResource {

  // Plain-assertion helper so this shared fixture stays free of the ScalaTest matcher stack in src/main.
  // The Mockito/matcher-based verify helpers remain in ResourceManagerTest, their only user.
  def verifyClose(resource: DummyResource): Unit = {
    assert(resource.getCloseListener == null, "expected the close listener to have been cleared")
    assert(resource.isClosed, "expected the resource to be closed")
    assert(resource.getTrackingHandle == UNTRACKED, "expected the tracking handle to be UNTRACKED")
  }
}
