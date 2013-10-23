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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import commands.{Pattern => LegacyPattern}
import org.junit.Test
import org.parboiled.scala._
import org.neo4j.graphdb.Direction

class PatternPartTest extends ParserTest[ast.PatternPart, Seq[LegacyPattern]] with Patterns with Expressions {
  implicit val parserToTest = PatternPart ~ EOI

  def convert(astNode: ast.PatternPart) = astNode.toLegacyPatterns

  @Test def label_literal_list_parsing() {
    parsing("(a)-[r:FOO|BAR]->(b)") or
    parsing("a-[r:FOO|:BAR]->b") shouldGive
      Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq("FOO", "BAR"), Direction.OUTGOING, optional = false))
  }
}
