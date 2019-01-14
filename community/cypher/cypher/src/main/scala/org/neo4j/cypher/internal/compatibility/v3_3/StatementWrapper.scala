/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.compatibility.v3_3.helpers.as3_4
import org.neo4j.cypher.internal.frontend.v3_3.{ast => astV3_3}
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, ast => astV3_4}
import org.neo4j.cypher.internal.util.v3_4.InputPosition

case class StatementWrapper(statement: astV3_3.Statement) extends astV3_4.Statement {
  override def semanticCheck: SemanticCheck = ???

  override lazy val returnColumns: List[String] = statement.returnColumns

  override lazy val position: InputPosition = as3_4(statement.position)
}