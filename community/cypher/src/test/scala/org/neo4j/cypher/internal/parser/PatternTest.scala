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

import org.junit.Test
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.{Pattern => LegacyPattern}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.parser.v2_0.rules.{Expressions, Patterns}
import org.neo4j.cypher.internal.parser.v2_0.ast

class PatternTest extends ParserExperimentalTest[ast.Pattern, Seq[LegacyPattern]] with Patterns with Expressions {

  def convert(astNode: ast.Pattern) = astNode.toLegacyPatterns

  @Test def label_literal_list_parsing() {
    implicit val parserToTest = Pattern

    parsing("(a)-[r:FOO|BAR]->(b)") or
    parsing("a-[r:FOO|:BAR]->b") shouldGive
      Seq(RelatedTo("a", "b", "r", Seq("FOO", "BAR"), Direction.OUTGOING, optional = false))
  }
}