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
package cypher.cucumber.db

import java.io.{File => JFile}
import java.net.URI

import cypher.cucumber.CucumberAdapter
import gherkin.formatter.model.{Match, Result}
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

import scala.reflect.io.File

object DatabaseConfigProvider {
  private var config: Map[Setting[_], String] = null

  def cypherConfig(): Map[Setting[_], String] = {
    assert(config != null)
    config
  }
}

class DatabaseConfigProvider(jsonFile: URI) extends CucumberAdapter {

  override def before(`match`: Match, result: Result): Unit = {
  val file = new JFile(getClass.getResource(jsonFile.getPath).getPath)
    assert(file.exists(), file + " should exist")
    val content = File.apply(file).slurp()

    {
      import org.json4s._
      import org.json4s.native.JsonMethods._

      val json = parse(content)
      DatabaseConfigProvider.config = json match {
        case JObject(entries) => entries.toMap.map {
          case ("planner", JString(planner)) => GraphDatabaseSettings.cypher_planner -> planner.toUpperCase
          case ("runtime", JString(runtime)) => GraphDatabaseSettings.cypher_runtime -> runtime.toUpperCase
          case ("language", JString(language)) => GraphDatabaseSettings.cypher_parser_version -> language.toLowerCase
          case (key, value) => throw new IllegalStateException(s"unsupported config: $key = $value")
        }
        case _ => throw new IllegalStateException(s"Unable to parse json file containing params at $file")
      }
    }
  }
}
