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
package org.neo4j.cypher.internal.executionplan

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.values.{TokenType, KeyToken}

class LabelResolutionTest extends Assertions {

  val blue  = KeyToken.Resolved("blue", 42, TokenType.Label)
  val green = KeyToken.Resolved("green", 28, TokenType.Label)

  @Test
  def testResolvedLabelsAreNotTouched() {
    // GIVEN
    val resolver = LabelResolution(mapper())

    // WHEN
    assert(blue === resolver(blue))
  }

  @Test
  def testLabelNamesAreResolved() {
    // GIVEN
    val resolver = LabelResolution(mapper("green" -> green))
    val expr     = KeyToken.Unresolved("green", TokenType.Label)

    // WHEN
    assert(green === resolver(expr))
  }

  @Test
  def testResolveViaTypedRewrite() {
    // GIVEN
    val resolver = LabelResolution(mapper("green" -> green))
    val expr     = KeyToken.Unresolved("green", TokenType.Label)

    // WHEN
    assert(green === expr.typedRewrite[KeyToken](resolver))
  }

  @Test
  def testUnknownLabelDefersToLazyResolving() {
    // GIVEN
    val resolver = LabelResolution(mapper("green" -> green))
    val expr     = KeyToken.Unresolved("red", TokenType.Label)

    // WHEN
    assert(expr === resolver(expr))
  }

  def mapper(pairs: (String, KeyToken.Resolved)*): (String => Option[KeyToken.Resolved]) = {
    val mapping: Map[String, KeyToken.Resolved] = pairs.toMap
    val fn: (String => Option[KeyToken.Resolved]) = { (name: String) => mapping.get(name) }
    fn
  }
}