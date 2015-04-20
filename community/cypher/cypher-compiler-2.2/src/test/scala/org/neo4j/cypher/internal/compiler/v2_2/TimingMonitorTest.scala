/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.commons.CypherFunSuite

class TimingMonitorTest extends CypherFunSuite with TimingMonitor[String] {
  private val query = "hello"
  var _currentTime: Long = 0L

  override def currentTime = _currentTime


  test("keeps track of time") {
    var ran = false

    _currentTime = 10
    start(query)
    _currentTime = 20
    end(query, (q, t: Long) => {
      q should equal(query)
      t should equal(10L)
      ran = true
    })

    ran should be(true)
  }

  test("does not mix events") {
    var ran = false

    start(query)
    end(query + " NOT IT", (_,_) => ran = true)

    ran should be(false)
  }

  test("reset the counter if start is called multiple times") {
    _currentTime = 0
    start(query)

    _currentTime = 500
    start(query)

    _currentTime = 600
    end(query, (_, t) => t should equal(100L))
  }

  test("calling end twice does not cause two monitor calls") {
    var timesCalled = 0

    start(query)
    end(query, (_,_) => timesCalled += 1)
    end(query, (_,_) => timesCalled += 1)

    timesCalled should equal(1)
  }
}
