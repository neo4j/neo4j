/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser

import java.util.Arrays.asList

import cypher.feature.parser.SideEffects.{State, Values}
import org.neo4j.graphdb.Entity

import scala.language.implicitConversions

class SideEffectsTest extends ParsingTestSupport {

  test("state node diffs") {
    State(Set(1, 2, 3)) diff State(Set(2, 3, 4)) should equal(Values(
      nodesCreated = 1, nodesDeleted = 1
    ))
    State() diff State(Set(2, 3, 4)) should equal(Values(nodesCreated = 3))

  }

  test("state rel diffs") {
    State(rels = Set(2, 600, 99)) diff State(rels = Set(600, 601)) should equal(Values(
      relsCreated = 1, relsDeleted = 2
    ))
    State() diff State(rels = Set(600, 601)) should equal(Values(
      relsCreated = 2
    ))
  }

  test("state label diffs") {
    State(labels = Set("Foo", "Bar")) diff State() should equal(Values(labelsDeleted = 2))
    State(labels = Set("Foo", "Bar")) diff State(Set(2, 3)) should equal(Values(labelsDeleted = 2, nodesCreated = 2))
    State(labels = Set("Foo", "Bar")) diff State(labels = Set("Foo", "Baz")) should equal(Values(labelsDeleted = 1, labelsCreated = 1))
  }

  test("state prop diffs") {
    State(props = Set((1, "foo", any(1)), (4, "bar", any("val")))) diff State() should equal(Values(
      propsDeleted = 2
    ))
    State(props = Set(
      (1, "foo", any(1)),
      (4, "bar", any("val")))
    ) diff State(props = Set(
      (1, "foo", any(2)),
      (3, "bar", any("val")))
    ) should equal(Values(
      propsDeleted = 2,
      propsCreated = 2
    ))
    State(props = Set(
      (1, "foo", any(1)),
      (4, "bar", any("val")))
    ) diff State(props = Set(
      (1, "foo", any(2)),
      (3, "bar", any("val")))
    ) should equal(Values(
      propsDeleted = 2,
      propsCreated = 2
    ))
  }

  test("converts primitive arrays") {
    SideEffects.convertArrays(Array(1, 2, 3)) should equal(IndexedSeq(1, 2, 3))
    SideEffects.convertArrays(Array("1", "2")) should equal(IndexedSeq("1", "2"))
  }

  test("doesn't convert other stuff") {
    SideEffects.convertArrays("foo") should equal("foo")
    SideEffects.convertArrays(Vector("1", "2")) should equal(Vector("1", "2"))
    SideEffects.convertArrays(asList("foo", 1)) should equal(asList("foo", 1))
  }

  private def any(a: Any): AnyRef = a.asInstanceOf[AnyRef]

  implicit def asEntity(id: Int): Entity = entity(id.toLong)

}
