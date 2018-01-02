/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import java.net.URL

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite


class LoadCsvIteratorTest extends CypherFunSuite {

  val url = new URL("file://uselessinfo.csv")
  val inner = Seq(Array[String]("1"), Array[String]("2"))

  test("should provide information about the current row") {
    val it = new LoadCsvIterator(url, inner.iterator)(())
    it.lastProcessed should equal(0)
    it.next()
    it.lastProcessed should equal(1)
  }

  test("should provide information about the last committed row") {
    val it = new LoadCsvIterator(url, inner.iterator)(())
    it.lastCommitted should equal(-1)
    it.next()
    it.notifyCommit()
    it.lastCommitted should equal(1)
  }

  test("should provide information about if we have completed reading the file") {
    val it = new LoadCsvIterator(url, inner.iterator)(())
    it.readAll should equal(false)
    it.next()
    it.next()
    it.readAll should equal(true)
  }

  test("should call onNext when next is called") {
    var called = false
    val it = new LoadCsvIterator(url, inner.iterator)({ called = true })
    called should equal(false)
    it.next()
    called should equal(true)
  }

  test("should call the inner iterator when calling hasNext") {
    var called = false
    val inner = new Iterator[Array[String]] {
      def next() = Array("yea")

      def hasNext =  {
        called = true
        true
      }
    }
    val it = new LoadCsvIterator(url, inner)(())
    called should equal(false)
    it.hasNext
    called should equal(true)
  }

  test("should call the inner iterator when calling next") {
    var called = false
    val inner = new Iterator[Array[String]] {
      def next() = {
        called = true
        Array("yea")
      }

      def hasNext = true
    }
    val it = new LoadCsvIterator(url, inner)(())
    called should equal(false)
    it.next()
    called should equal(true)
  }

}
