/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.cucumber

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets

import gherkin.formatter.model.{Match, Result}

import scala.io.Source

object CypherOptionPlugin {
  private var _options: String = ""

  def options = _options
}

class CypherOptionPlugin(optionUri: URI) extends CucumberAdapter {

  override def before(`match`: Match, result: Result): Unit = {
    val url = getClass.getResource(optionUri.getPath)
    if (url == null) throw new FileNotFoundException(s"Cypher option file not found at: $optionUri")
    val itr = Source.fromFile(url.getPath, StandardCharsets.UTF_8.name()).getLines()
    CypherOptionPlugin._options = itr.foldLeft("") {
      case (optionString, line) =>
        if (line.isEmpty || line.startsWith("//")) optionString else optionString + " " + line
    }
  }

  override def after(`match`: Match, result: Result) = {
    CypherOptionPlugin._options = ""
  }
}
