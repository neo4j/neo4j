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
package org.neo4j.cypher.internal.parser

import v2_0._
import org.junit.Test
import org.neo4j.cypher.internal.mutation.{NamedExpectation, UniqueLink}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.helpers.LabelSupport
import org.neo4j.cypher.internal.commands.expressions.{Collection, Identifier}

class CreateUniqueTest extends CreateUnique with MatchClause with ParserTest with Expressions {

  @Test
  def create_unique_action_with_labels() {
    implicit val parserToTest = usePattern(createUniqueTranslate)

    val aLink = NamedExpectation("a", Identifier("a"), Map.empty, LabelSupport.labelCollection("Foo"), bare = false)
    val bLink = NamedExpectation("b", Identifier("b"), Map.empty, Seq.empty, bare = true)
    val relLink = NamedExpectation("  UNNAMED7", Identifier("  UNNAMED7"), Map.empty, Seq.empty, bare = true)

    parsing("a:Foo-[:x]->b") shouldGive
      Seq(PathAndRelateLink(None, Seq(UniqueLink(aLink, bLink, relLink, "x", Direction.OUTGOING))))
  }


  def createProperty(entity: String, propName: String) = ???
}