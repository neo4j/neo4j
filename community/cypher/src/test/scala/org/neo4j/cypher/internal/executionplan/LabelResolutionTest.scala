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
import org.neo4j.cypher.internal.commands.values.{LabelValue, LabelName, ResolvedLabel}

class LabelResolutionTest extends Assertions {

  val blue  = ResolvedLabel("blue", 42)
  val green = ResolvedLabel("green", 28)

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
    val expr     = LabelName("green")

    // WHEN
    assert(green === resolver(expr))
  }

  @Test
  def testResolveViaTypedRewrite() {
    // GIVEN
    val resolver = LabelResolution(mapper("green" -> green))
    val expr     = LabelName("green")

    // WHEN
    assert(green === expr.typedRewrite[LabelValue](resolver))
  }

  @Test
  def testUnknownLabelDefersToLazyResolving() {
    // GIVEN
    val resolver = LabelResolution(mapper("green" -> green))
    val expr     = LabelName("red")

    // WHEN
    assert(expr === resolver(expr))
  }

  def mapper(pairs: (String, ResolvedLabel)*): (String => Option[ResolvedLabel]) = {
    val mapping: Map[String, ResolvedLabel] = pairs.toMap
    val fn: (String => Option[ResolvedLabel]) = { (name: String) => mapping.get(name) }
    fn
  }
}