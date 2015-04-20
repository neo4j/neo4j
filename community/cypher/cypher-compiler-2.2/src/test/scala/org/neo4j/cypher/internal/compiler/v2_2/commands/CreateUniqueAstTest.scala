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
package org.neo4j.cypher.internal.compiler.v2_2.commands

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_2.mutation.{CreateUniqueAction, NamedExpectation, UniqueLink}
import org.neo4j.graphdb.Direction

class CreateUniqueAstTest extends CypherFunSuite {

  val simplePropMap = Map("name" -> Literal("Neo"))

  test("testNextStepOnCreateUniqueAst") {
    // "a-[r:REL]->b"
    Seq(ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING)) -->(
      expectedLinks = Seq(UniqueLink("a", "b", "r", "REL", Direction.OUTGOING)),
      expectedNamedPaths = Seq()
    )

    // "a-[r:REL]->b-[r2:REL]->c"
    Seq(
      ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING),
      ParsedRelation("r2", "b", "c", Seq("REL"), Direction.OUTGOING)
    ) -->(
      expectedLinks = Seq(
        UniqueLink("a", "b", "r", "REL", Direction.OUTGOING),
        UniqueLink("b", "c", "r2", "REL", Direction.OUTGOING)
      ),
      expectedNamedPaths = Seq()
    )

    // "a-[r:REL {name:'Neo'}]->b"
    Seq(
      ParsedRelation(name = "r",
        props = simplePropMap,
        start = ParsedEntity("a"),
        end = ParsedEntity("b"), typ = Seq("REL"),
        dir = Direction.OUTGOING, optional = false)) -->(
      expectedLinks = Seq(UniqueLink(
        start = NamedExpectation("a"),
        end = NamedExpectation("b"),
        rel = NamedExpectation("r", simplePropMap),
        relType = "REL", dir = Direction.OUTGOING)),
      expectedNamedPaths = Seq()
    )

    // "p = a-[r:REL]->b"
    Seq(ParsedNamedPath("p", Seq(ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING)))) -->(
      expectedLinks = Seq(UniqueLink("a", "b", "r", "REL", Direction.OUTGOING)),
      expectedNamedPaths = Seq(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING)))
    )
  }

  implicit class Check(patterns: Seq[AbstractPattern]) {

    def -->(expectedNamedPaths: Seq[NamedPath], expectedLinks: Seq[UniqueLink]) {
      val (startItems, namedPaths) = new CreateUniqueAst(patterns).nextStep()
      val expectedStartItems = Seq(CreateUniqueStartItem(CreateUniqueAction(expectedLinks: _*)))
      startItems should equal(expectedStartItems)
      namedPaths should equal(expectedNamedPaths)
    }
  }
}
