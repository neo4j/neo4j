/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Test
import org.scalatest.Assertions

class CollectionAcceptanceTest extends ExecutionEngineHelper with Assertions {
  @Test
  def should_return_null_for_empty_collections_tail() {
    assert(executeScalar[Any]("return last([])") === null)
  }

  @Test
  def should_return_null_for_empty_collections_head() {
    assert(executeScalar[Any]("return head([])") === null)
  }

  @Test
  def should_return_empty_collection_for_empty_collections_tail() {
    assert(executeScalar[Seq[_]]("return tail([])").isEmpty, "Returned collection should be empty")
  }

  @Test
  def should_return_null_for_null_collections_tail() {
    assert(executeScalar[Any]("return last(null)") === null)
  }

  @Test
  def should_return_null_for_null_collections_head() {
    assert(executeScalar[Any]("return head(null)") === null)
  }

  @Test
  def should_return_empty_collection_for_null_collections_tail() {
    assert(executeScalar[Any]("return tail(null)") === null)
  }

  @Test
  def reduce_on_empty_collection_returns_acc() {
    assert(executeScalar[Any]("return reduce(acc=666, x in [] | acc + x)") === 666)
  }

  @Test
  def reduce_on_null_collection_returns_acc() {
    assert(executeScalar[Any]("return reduce(acc=666, x in null | acc + x)") === null)
  }
}