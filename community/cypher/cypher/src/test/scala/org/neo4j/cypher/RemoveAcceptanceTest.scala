/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

class RemoveAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  trait TestCase {
    val a = createLabeledNode(Map("name" -> "A"), "A")
    val b = createLabeledNode(Map("name" -> "B"), "B")
    val c = createLabeledNode(Map("name" -> "C"), "C")
  }

  test("remove label from a simple label string expression") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) REMOVE n LABEL 'A'")

      // then
      a shouldNot haveLabels("A")
      b should haveLabels("B")
      c should haveLabels("C")
    }
  }

  test("removes label based on a string property on the node") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) REMOVE n LABEL n.name")

      // then
      a shouldNot haveLabels("A")
      b shouldNot haveLabels("B")
      c shouldNot haveLabels("C")
    }
  }

  test("removes multiple labels based on string collection") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) REMOVE n LABELS ['A','B']")

      // then
      a shouldNot haveLabels("A")
      b shouldNot haveLabels("B")
      c should haveLabels("C")
    }
  }

  test("when given empty collection, does nothing") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) REMOVE n LABELS []")

      // then
      a should haveLabels("A")
      b should haveLabels("B")
      c should haveLabels("C")
    }
  }

  test("when given a null expression, does nothing") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) REMOVE n LABELS null")

      // then
      a should haveLabels("A")
      b should haveLabels("B")
      c should haveLabels("C")
    }
  }

  test("when given a null expression2, does nothing") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) REMOVE n LABEL n.missingProperty")

      // then
      a should haveLabels("A")
      b should haveLabels("B")
      c should haveLabels("C")
    }
  }

  test("when given a collection mixing nulls and non-nulls, only uses the non-nulls") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) REMOVE n LABELS ['B', null]")

      // then
      a should haveLabels("A")
      b shouldNot haveLabels("B")
      c should haveLabels("C")
    }
  }

  test("removing labels inside foreach") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) FOREACH(x in ['A','B','C'] | REMOVE n LABEL x)")

      // then
      a shouldNot haveLabels("A")
      b shouldNot haveLabels("B")
      c shouldNot haveLabels("C")
    }
  }

  test("removing labels from an expression") {
    // given
    new TestCase {
      // when
      execute("MATCH (n) WITH collect(n.name) as names MATCH (n) REMOVE n LABELS names")

      // then
      a should haveOnlyLabels()
      b should haveOnlyLabels()
      c should haveOnlyLabels()
    }
  }
}
