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
package org.neo4j.cypher.internal.compiler.v2_3.ast

import org.neo4j.cypher.internal.commons.CypherTestSupport
import org.neo4j.cypher.internal.compiler.v2_3.{DummyPosition, InputPosition}

trait AstConstructionTestSupport extends CypherTestSupport {
  protected val pos = DummyPosition(0)

  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

  def ident(name: String): Identifier = Identifier(name)(pos)

  def propEquality(identifier: String, propKey: String, intValue: Int) = {
    val prop: Expression = Property(ident(identifier), PropertyKeyName(propKey)(pos))(pos)
    val literal: Expression = SignedDecimalIntegerLiteral(intValue.toString)(pos)
    Equals(prop, literal)(pos)
  }
}
