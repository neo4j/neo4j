/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.cucumber

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets

import gherkin.formatter.model.{Match, Result}

import scala.io.Source

object BlacklistPlugin {
  private var _blacklist: Set[String] = null

  def blacklisted(name: String) = blacklist().contains(normalizedScenarioName(name))

  def normalizedScenarioName(name: String) = {
    val builder = new StringBuilder

    var inBlanks = false
    name.trim.foreach { (ch) =>
      if (ch == ' ') {
        if (!inBlanks) {
          builder += ' '
          inBlanks = true
        }
      } else {
        builder += ch.toLower
        inBlanks = false
      }
    }

    val result = builder.toString()
    result
  }

  private def blacklist(): Set[String] = {
    assert(_blacklist != null)
    _blacklist
  }
}

class BlacklistPlugin(blacklistFile: URI) extends CucumberAdapter {

  override def before(`match`: Match, result: Result): Unit = {
    val url = getClass.getResource(blacklistFile.getPath)
    if (url == null) throw new FileNotFoundException(s"blacklist file not found at: $blacklistFile")
    val itr = Source.fromFile(url.getPath, StandardCharsets.UTF_8.name()).getLines()
    BlacklistPlugin._blacklist = itr.foldLeft(Set.empty[String]) {
      case (set, scenarioName) =>
        val normalizedName = BlacklistPlugin.normalizedScenarioName(scenarioName)
        if (normalizedName.isEmpty || normalizedName.startsWith("//")) set else set + normalizedName
    }
  }
}
