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
import org.parboiled.scala.rules.Rule1
import org.neo4j.cypher.internal.parser.v2_0.ast
import org.neo4j.cypher.internal.{commands => legacyCommands}
import org.neo4j.cypher.internal.parser.v2_0.rules.Command
import org.neo4j.cypher.internal.parser.v2_0.ast.Expression

class ConstraintTest extends ParserExperimentalTest[ast.Command, legacyCommands.AbstractQuery] with Command {

  @Test
  def create_uniqueness_constraint() {
    implicit val parserToTest = Command

    parsing("CREATE CONSTRAINT ON (foo:Foo) ASSERT foo.name IS UNIQUE") or
      parsing("CREATE CONSTRAINT ON (foo:Foo) foo.name IS UNIQUE") or
      parsing("create constraint on (foo:Foo) assert foo.name is unique") shouldGive
      legacyCommands.CreateUniqueConstraint("foo", "Foo", "foo", "name")

    parsing("CREATE CONSTRAINT ON (foo:Foo) ASSERT bar.name IS UNIQUE") shouldGive
      legacyCommands.CreateUniqueConstraint("foo", "Foo", "bar", "name")
  }

  @Test
  def drop_uniqueness_constraint() {
    implicit val parserToTest = Command

    parsing("DROP CONSTRAINT ON (foo:Foo) ASSERT foo.name IS UNIQUE") or
      parsing("DROP CONSTRAINT ON (foo:Foo) foo.name IS UNIQUE") or
      parsing("drop constraint on (foo:Foo) assert foo.name is unique") shouldGive
      legacyCommands.DropUniqueConstraint("foo", "Foo", "foo", "name")

    parsing("DROP CONSTRAINT ON (foo:Foo) ASSERT bar.name IS UNIQUE") shouldGive
      legacyCommands.DropUniqueConstraint("foo", "Foo", "bar", "name")
  }

  def convert(astNode: ast.Command): legacyCommands.AbstractQuery = astNode.toLegacyQuery

  def Expression: Rule1[Expression] = ???
}