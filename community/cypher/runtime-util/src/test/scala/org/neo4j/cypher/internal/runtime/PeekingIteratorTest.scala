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

import org.neo4j.cypher.internal.runtime.ClosingIteratorTest.values
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PeekingIteratorTest extends CypherFunSuite {

  test("peek on empty iterator throws") {
    a[NoSuchElementException] should be thrownBy new PeekingIterator(ClosingIterator.empty).peek()
  }

  test("one element iterator, no peek") {
    val i = new PeekingIterator(ClosingIterator(Iterator(1)))
    i.hasNext should be(true)
    i.next() should be(1)
    i.hasNext should be(false)
  }

  test("one element iterator, peek") {
    val i = new PeekingIterator(ClosingIterator(Iterator(1)))
    i.hasNext should be(true)
    i.peek() should be(1)
    i.hasNext should be(true)
    i.next() should be(1)
    i.hasNext should be(false)
  }

  test("two element iterator, no peek") {
    val i = new PeekingIterator(ClosingIterator(Iterator(1, 2)))
    i.hasNext should be(true)
    i.next() should be(1)
    i.hasNext should be(true)
    i.next() should be(2)
    i.hasNext should be(false)
  }

  test("two element iterator, peek") {
    val i = new PeekingIterator(ClosingIterator(Iterator(1, 2)))
    i.hasNext should be(true)
    i.peek() should be(1)
    i.hasNext should be(true)
    i.next() should be(1)
    i.hasNext should be(true)
    i.peek() should be(2)
    i.hasNext should be(true)
    i.next() should be(2)
    i.hasNext should be(false)
  }

  test("closes inner iterator on close") {
    val inner = values(1, 2, 3)
    val peeking = new PeekingIterator(inner)
    peeking.peek()
    peeking.close()

    inner.closed should be(true)
  }

  test("closes inner iterator on depletion") {
    val inner = values(1, 2, 3)
    val peeking = new PeekingIterator(inner)
    peeking.toList // deplete

    inner.closed should be(true)
  }
}
