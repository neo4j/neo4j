/**
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
package org.neo4j.cypher

import org.junit.{Before, Test}

class ColumnNameTest extends ExecutionEngineJUnitSuite {

  override def initTest() {
    super.initTest()
    createNode()
  }

  @Test def shouldKeepUsedExpression1() {
    val result = execute("start n=node(0) return cOuNt( * )")
    assert(result.columns === List("cOuNt( * )"))
  }

  @Test def shouldKeepUsedExpression2() {
    val result = execute("start n=node(0) match p=n-->b return nOdEs( p )")
    assert(result.columns === List("nOdEs( p )"))
  }

  @Test def shouldKeepUsedExpression3() {
    val result = execute("start n=node(0) match p=n-->b return coUnt( dIstInct p )")
    assert(result.columns === List("coUnt( dIstInct p )"))
  }

  @Test def shouldKeepUsedExpression4() {
    val result = execute("start n=node(0) match p=n-->b return aVg(    n.aGe     )")
    assert(result.columns === List("aVg(    n.aGe     )"))
  }
}
