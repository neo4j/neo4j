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
package org.neo4j.cypher.internal.helpers

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.kernel.impl.util.PrimitiveLongIteratorForArray

class JavaConversionSupportTest extends Assertions {

  @Test
  def shouldConvertPrimitiveLongIterators() {
    // given
    val iterator = new PrimitiveLongIteratorForArray( 12l, 14l )

    // when
    val result = JavaConversionSupport.asScala(iterator)

    // then
    assert( List( 12l, 14l ) === result.toList )
  }


  @Test
  def shouldConvertAndMapPrimitiveLongIterators() {
    // given
    val iterator = new PrimitiveLongIteratorForArray( 12l, 14l )

    // when
    val result = JavaConversionSupport.mapToScala(iterator){ _ + 1l }

    // then
    assert( List( 13l, 15l ) === result.toList )
  }
}