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
package org.neo4j.cypher

import internal.commands.AbstractQuery
import org.neo4j.cypher.internal.parser.ActualParser

sealed abstract class CypherVersion(versionName: String) {
  val name = CypherVersionName.asCanonicalName(versionName)
  def parser: ActualParser
}

object CypherVersionName {
  def asCanonicalName(versionName: String) = versionName.toLowerCase
}

object CypherVersion {

  case object v1_9 extends CypherVersion("1.9") {
    val parser = new internal.parser.v1_9.CypherParserImpl
  }

  case object v2_0 extends CypherVersion("2.0") {
    val parser = new internal.parser.v2_0.CypherParserImpl
  }

  case object vLegacy extends CypherVersion("legacy") {
    val parser = new internal.parser.legacy.CypherParserImpl
  }

  def apply(versionName: String) = findVersionByExactName(CypherVersionName.asCanonicalName(versionName)).getOrElse {
    throw new SyntaxException(s"Supported versions are: $allVersionNames")
  }

  def findVersionByExactName(versionName: String) = allVersions.find( _.name == versionName )

  val vDefault = v2_0

  val allVersions = Seq(v1_9, v2_0, vLegacy)
  val allVersionNames = CypherVersion.allVersions.map(_.name).mkString(", ")
}

class CypherParser(val defaultVersion: CypherVersion) {

  def this() = this(CypherVersion.vDefault)
  def this(versionName: String) = this(CypherVersion(versionName))

  private val hasVersionDefined = """(?si)^\s*cypher\s*([^\s]+)\s*(.*)""".r

  @throws(classOf[SyntaxException])
  def parse(queryText: String): AbstractQuery = {

    val result = queryText match {
      case hasVersionDefined(versionName, remainingQuery) => CypherVersion(versionName).parser.parse(remainingQuery)
      case _                                              => defaultVersion.parser.parse(queryText)
    }

    result.verifySemantics()
    result
  }
}
