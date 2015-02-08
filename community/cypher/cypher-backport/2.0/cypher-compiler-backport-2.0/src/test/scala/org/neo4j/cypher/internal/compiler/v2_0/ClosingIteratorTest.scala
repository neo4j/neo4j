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
package org.neo4j.cypher.internal.compiler.v2_0

import org.neo4j.cypher.internal.compiler.v2_0.spi.QueryContext
import org.junit.{Before, Test}
import org.hamcrest.CoreMatchers.is
import org.junit.Assert.assertThat
import org.mockito.Mockito._
import org.scalatest.Assertions
import org.scalatest.mock.MockitoSugar

class ClosingIteratorTest extends Assertions with MockitoSugar {
  var ctx: QueryContext = _

  @Before
  def before() {
    ctx = mock[QueryContext]
  }

  @Test
  def should_cleanup_when_we_reach_the_end() {
    //Given
    val wrapee   = Iterator(Map("k" -> 42))
    val iterator = new ClosingIterator(wrapee, ctx)

    //When
    val result = iterator.next()

    //Then
    verify(ctx).close(success = true)
    assertThat(result, is(Map[String, Any]("k" -> 42)))
  }

  @Test
  def should_cleanup_even_for_empty_iterator() {
    //Given
    val wrapee   = Iterator.empty
    val iterator = new ClosingIterator(wrapee, ctx)

    //When
    val result = iterator.hasNext

    //Then
    verify(ctx).close(success = true)
    assertThat(result, is(false))
  }

  @Test
  def multiple_has_next_should_not_close_more_than_once() {
    //Given
    val wrapee   = Iterator.empty
    val iterator = new ClosingIterator(wrapee, ctx)

    //When
    val result = iterator.hasNext
    iterator.hasNext
    iterator.hasNext
    iterator.hasNext
    iterator.hasNext

    //Then
    verify(ctx, times(1)).close(success = true)
    assertThat(result, is(false))
  }

  @Test
  def exception_in_hasNext_should_fail_transaction() {
    //Given
    val wrapee = mock[Iterator[Map[String, Any]]]
    when(wrapee.hasNext).thenThrow(new RuntimeException)
    val iterator = new ClosingIterator(wrapee, ctx)

    //When
    intercept[RuntimeException](iterator.hasNext)

    //Then
    verify(ctx).close(success = false)
  }

  @Test
  def exception_in_next_should_fail_transaction() {
    //Given
    val wrapee = mock[Iterator[Map[String, Any]]]
    when(wrapee.hasNext).thenReturn(true)
    when(wrapee.next()).thenThrow(new RuntimeException)

    val iterator = new ClosingIterator(wrapee, ctx)

    //When
    intercept[RuntimeException](iterator.next())

    //Then
    verify(ctx).close(success = false)
  }
}
