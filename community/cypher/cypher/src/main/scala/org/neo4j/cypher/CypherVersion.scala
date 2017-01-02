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
package org.neo4j.cypher

sealed abstract class CypherVersion(versionName: String) {
  val name = CypherVersionName.asCanonicalName(versionName)
}

object CypherVersionName {
  def asCanonicalName(versionName: String) = versionName.toLowerCase
}

object CypherVersion {

  case object v1_9 extends CypherVersion("1.9")
  case object v2_0 extends CypherVersion("2.0")
  case object v2_1 extends CypherVersion("2.1")
  case object v2_2 extends CypherVersion("2.2")

  def apply(versionName: String) = findVersionByExactName(CypherVersionName.asCanonicalName(versionName)).getOrElse {
    throw new SyntaxException(s"Supported versions are: ${allVersions.map(_.name).mkString(", ")}")
  }

  def findVersionByExactName(versionName: String) = allVersions.find( _.name == versionName )

  val vDefault = v2_2
  val allVersions = Seq(v1_9, v2_0, v2_1, v2_2)
}
