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

import v2_0.{AbstractPattern, Expressions, ParsedEntity, ParserPattern}
import org.junit.Test
import org.neo4j.cypher.internal.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.commands.values.LabelName

class ParserPatternTest extends ParserPattern with ParserTest with Expressions {

  @Test def label_literal_list_short_form() {
    implicit val parserToTest = labelLongForm

    parsing(":FOO") or
    parsing("label :FOO") shouldGive
      Literal(List(LabelName("FOO")))

    parsing(":FOO:BAR") or
    parsing("label :FOO:BAR") shouldGive
      Literal(List(LabelName("FOO"), LabelName("BAR")))

    assertFails("[:foo, :bar]")
  }

  @Test def node_forms() {
    implicit val parserToTest = node

    parsing("n") shouldGive
      ParsedEntity("n", Identifier("n"), Map.empty, True(), Literal(Seq.empty), true)

    parsing("(n)") shouldGive
      ParsedEntity("n", Identifier("n"), Map.empty, True(), Literal(Seq.empty), true)

    parsing("n {name:'Andres'}") shouldGive
      ParsedEntity("n", Identifier("n"), Map("name"->Literal("Andres")), True(), Literal(Seq.empty), false)

    parsing("n VALUES {name:'Andres'}") shouldGive
      ParsedEntity("n", Identifier("n"), Map("name"->Literal("Andres")), True(), Literal(Seq.empty), false)

    parsing("n LABEL :FOO") shouldGive
      ParsedEntity("n", Identifier("n"), Map.empty, True(), Literal(List(LabelName("FOO"))), false)
  }

  def matchTranslator(abstractPattern: AbstractPattern) = ???

  def createProperty(entity: String, propName: String) = ???
}
