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
package org.neo4j.cypher.internal.runtime.debug

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class DebugSupportTest extends CypherFunSuite {

  test("I think you forgot to disable DebugSupport after debugging...") {
    DebugSupport.DEBUG_PHYSICAL_PLANNING shouldBe false
    DebugSupport.DEBUG_TIMELINE shouldBe false
    DebugSupport.DEBUG_WORKERS shouldBe false
    DebugSupport.DEBUG_QUERIES shouldBe false
    DebugSupport.DEBUG_TRACKER shouldBe false
    DebugSupport.DEBUG_LOCKS shouldBe false
    DebugSupport.DEBUG_ERROR_HANDLING shouldBe false
    DebugSupport.DEBUG_CLEANUP shouldBe false
    DebugSupport.DEBUG_CURSORS shouldBe false
    DebugSupport.DEBUG_BUFFERS shouldBe false
    DebugSupport.DEBUG_SCHEDULING shouldBe false
    DebugSupport.DEBUG_ASM shouldBe false
    DebugSupport.DEBUG_TRANSACTIONAL_CONTEXT shouldBe false
    DebugSupport.DEBUG_PROGRESS shouldBe false
    DebugSupport.DEBUG_WORKERS_ON_PROGRESS_STALL shouldBe false
    DebugSupport.DEBUG_PIPELINES shouldBe false
    DebugSupport.DEBUG_GENERATED_SOURCE_CODE shouldBe false
  }
}
