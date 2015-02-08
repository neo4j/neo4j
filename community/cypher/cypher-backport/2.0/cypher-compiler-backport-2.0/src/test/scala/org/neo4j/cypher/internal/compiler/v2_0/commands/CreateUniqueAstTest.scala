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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_0._
import mutation.{NamedExpectation, UniqueLink, CreateUniqueAction}
import org.neo4j.graphdb.Direction
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.Test
import org.scalatest.Assertions
import org.junit.runners.Parameterized.Parameters

@RunWith(value = classOf[Parameterized])
class CreateUniqueAstTest(name: String,
                          patterns: Seq[AbstractPattern],
                          expectedNamedPaths: Seq[NamedPath],
                          expectedStartItems: Seq[StartItem]) extends Assertions {
  @Test
  def testNextStepOnCreateUniqueAst() {
    //given is the constructor of this class

    // when
    val (startItems, namedPaths) = new CreateUniqueAst(patterns).nextStep()

    //then
    assert(startItems === expectedStartItems)
    assert(namedPaths === expectedNamedPaths)
  }
}

object CreateUniqueAstTest {
  val simplePropMap = Map("name" -> Literal("Neo"))

  @Parameters(name = "{0}")
  def parameters: java.util.Collection[Array[AnyRef]] = {
    val list = new java.util.ArrayList[Array[AnyRef]]()
    def add(name: String, given: Seq[AbstractPattern], expectedNamedPaths: Seq[NamedPath], expectedLinks: Seq[UniqueLink]) {
      list.add(Array(name, given, expectedNamedPaths, Seq(CreateUniqueStartItem(CreateUniqueAction(expectedLinks: _*)))))
    }

    add(
      name = "a-[r:REL]->b",
      given = Seq(ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING)),
      expectedLinks = Seq(UniqueLink("a", "b", "r", "REL", Direction.OUTGOING)),
      expectedNamedPaths = Seq()
    )

    add(
      name = "a-[r:REL]->b-[r2:REL]->c",
      given = Seq(
        ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING),
        ParsedRelation("r2", "b", "c", Seq("REL"), Direction.OUTGOING)
      ),
      expectedLinks = Seq(
        UniqueLink("a", "b", "r", "REL", Direction.OUTGOING),
        UniqueLink("b", "c", "r2", "REL", Direction.OUTGOING)
      ),
      expectedNamedPaths = Seq()
    )

    add(
      name = "a-[r:REL {name:'Neo'}]->b",
      given = Seq(
        ParsedRelation(name = "r",
          props = simplePropMap,
          start = ParsedEntity("a"),
          end = ParsedEntity("b"), typ = Seq("REL"),
          dir = Direction.OUTGOING, optional = false)),

      expectedLinks = Seq(UniqueLink(
        start = NamedExpectation("a"),
        end = NamedExpectation("b"),
        rel = NamedExpectation("r", simplePropMap),
        relType = "REL", dir = Direction.OUTGOING)),

      expectedNamedPaths = Seq()
    )

    add(
      name = "p = a-[r:REL]->b",
      given = Seq(ParsedNamedPath("p", Seq(ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING)))),
      expectedLinks = Seq(UniqueLink("a", "b", "r", "REL", Direction.OUTGOING)),
      expectedNamedPaths = Seq(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING)))
    )


    list
  }
}
