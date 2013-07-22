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
import org.neo4j.cypher.internal.commands.CreateUniqueConstraint
import org.neo4j.cypher.internal.commands.DropUniqueConstraint

class ConstraintTest extends Constraint with ParserTest {

  @Test
  def create_uniqueness_constraint() {
    implicit val parserToTest = createUniqueConstraint
    
    parsing( "CREATE CONSTRAINT ON (foo:Foo) ASSERT foo.name IS UNIQUE" ) or
    parsing( "CREATE CONSTRAINT ON (foo:Foo) foo.name IS UNIQUE" ) or
    parsing( "create constraint on (foo:Foo) assert foo.name is unique" ) shouldGive
      CreateUniqueConstraint( "foo", "Foo", "foo", "name" )
    
    parsing( "CREATE CONSTRAINT ON (foo:Foo) ASSERT bar.name IS UNIQUE" ) shouldGive
      CreateUniqueConstraint( "foo", "Foo", "bar", "name" )
  }

  @Test
  def drop_uniqueness_constraint() {
    implicit val parserToTest = dropUniqueConstraint
    
    parsing( "DROP CONSTRAINT ON (foo:Foo) ASSERT foo.name IS UNIQUE" ) or
    parsing( "DROP CONSTRAINT ON (foo:Foo) foo.name IS UNIQUE" ) or
    parsing( "drop constraint on (foo:Foo) assert foo.name is unique" ) shouldGive
      DropUniqueConstraint( "foo", "Foo", "foo", "name" )
    
    parsing( "DROP CONSTRAINT ON (foo:Foo) ASSERT bar.name IS UNIQUE" ) shouldGive
      DropUniqueConstraint( "foo", "Foo", "bar", "name" )
  }
}