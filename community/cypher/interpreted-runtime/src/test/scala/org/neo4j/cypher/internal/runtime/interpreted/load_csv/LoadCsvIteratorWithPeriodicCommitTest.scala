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
package org.neo4j.cypher.internal.runtime.interpreted.load_csv

import org.neo4j.cypher.internal.runtime.interpreted.pipes.LoadCsvIterator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LoadCsvIteratorWithPeriodicCommitTest extends CypherFunSuite {

  val inner = Seq(Array[String]("1"), Array[String]("2"))
  var outer: Option[LoadCsvIteratorWithPeriodicCommit] = None

  test("should provide information about the current row") {
    val it = getIterator(inner.iterator)(())
    it.lastProcessed should equal(0)
    it.next()
    it.lastProcessed should equal(1)
  }

  test("should provide information about the last committed row") {
    val it = getIterator(inner.iterator)(notifyCommit())
    outer = Some(it)
    it.lastCommitted should equal(-1)
    it.next()
    it.lastCommitted should equal(0)
  }

  private def notifyCommit(): Unit = {
    outer.foreach(_.notifyCommit())
  }

  test("should provide information about if we have completed reading the file") {
    val it = getIterator(inner.iterator)(())
    it.readAll should equal(false)
    it.next()
    it.next()
    it.readAll should equal(true)
  }

  test("should call onNext when next is called") {
    var called = false
    val it = getIterator(inner.iterator)({ called = true })
    called should equal(false)
    it.next()
    called should equal(true)
  }

  test("should call the inner iterator when calling hasNext") {
    var called = false
    val inner = new Iterator[Array[String]] {
      def next() = Array("yea")

      def hasNext: Boolean =  {
        called = true
        true
      }
    }
    val it = getIterator(inner)(())
    called should equal(false)
    it.hasNext
    called should equal(true)
  }

  test("should call the inner iterator when calling next") {
    var called = false
    val inner = new Iterator[Array[String]] {
      def next(): Array[String] = {
        called = true
        Array("yea")
      }

      def hasNext = true
    }
    val it = getIterator(inner)(())
    called should equal(false)
    it.next()
    called should equal(true)
  }

  private def getIterator(inner: Iterator[Array[String]])(onNext: => Unit) = {
    new LoadCsvIteratorWithPeriodicCommit(new LoadCsvIterator{
      var lastProcessed: Long = 0L
      var readAll: Boolean = false

      override protected[this] def closeMore(): Unit = ()

      override def innerHasNext: Boolean = inner.hasNext

      override def next(): Array[String] = {
        val next = inner.next()
        lastProcessed += 1
        readAll = !hasNext
        next
      }
    })(onNext)
  }
}
