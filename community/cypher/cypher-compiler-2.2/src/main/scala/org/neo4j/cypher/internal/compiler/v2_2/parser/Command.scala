/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.parser

import org.neo4j.cypher.internal.compiler.v2_2.ast
import org.parboiled.scala._

trait Command extends Parser
  with Expressions
  with Literals
  with Base {

  def Command: Rule1[ast.Command] = rule(
    CreateUniqueConstraint
      | CreateIndex
      | DropUniqueConstraint
      | DropIndex
  )

  def CreateIndex: Rule1[ast.CreateIndex] = rule {
    group(keyword("CREATE INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyName ~~ ")") ~~>> (ast.CreateIndex(_, _))
  }

  def DropIndex: Rule1[ast.DropIndex] = rule {
    group(keyword("DROP INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyName ~~ ")") ~~>> (ast.DropIndex(_, _))
  }

  def CreateUniqueConstraint: Rule1[ast.CreateUniqueConstraint] = rule {
    group(keyword("CREATE") ~~ ConstraintSyntax) ~~>> (ast.CreateUniqueConstraint(_, _, _))
  }

  def DropUniqueConstraint: Rule1[ast.DropUniqueConstraint] = rule {
    group(keyword("DROP") ~~ ConstraintSyntax) ~~>> (ast.DropUniqueConstraint(_, _, _))
  }

  private def ConstraintSyntax = keyword("CONSTRAINT ON") ~~ "(" ~~ Identifier ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ PropertyExpression ~~ keyword("IS UNIQUE")
}
