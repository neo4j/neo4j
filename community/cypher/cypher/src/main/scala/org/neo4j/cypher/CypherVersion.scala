/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher

sealed abstract class CypherVersion(versionName: String) extends CypherOption(versionName)

case object CypherVersion extends CypherOptionCompanion[CypherVersion] {
  case object v1_9 extends CypherVersion("1.9")
  case object v2_2 extends CypherVersion("2.2")
  case object v2_3 extends CypherVersion("2.3")

  val default = v2_3
  val all: Set[CypherVersion] = Set(v1_9, v2_2, v2_3)
}
