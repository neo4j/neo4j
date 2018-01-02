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
package cypher.cucumber.db

import java.io.{File => JFile}
import java.nio.file.{Path => JPath, Files, Paths}

import org.neo4j.graphdb.factory.{GraphDatabaseSettings, GraphDatabaseFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.io.File

case class ImportQuery(script: String, params: java.util.Map[String, Object], fileRoot: JPath)

object AvailableDatabase {
  final val archive = Map("cineast" -> importInto("/cypher/db/cineast/"))
  final val dbPaths : mutable.Map[String, JFile] = new mutable.HashMap
  private final val SCRIPT_FILENAME = "import.cyp"
  private final val PARAMS_FILENAME = "params.json"

  private def importInto(path: String): ImportQuery = {
    val basePath = Paths.get(getClass.getResource(path).toURI)
    val scriptPath = basePath.resolve(SCRIPT_FILENAME)
    val paramsPath = basePath.resolve(PARAMS_FILENAME)
    assert(Files.exists(scriptPath), scriptPath + " should exist")
    assert(Files.exists(paramsPath), paramsPath + " should exist")

    val script = File.apply(scriptPath.toFile).slurp()
    val content = File.apply(paramsPath.toFile).slurp()
    val json = scala.util.parsing.json.JSON.parseFull(content)
    val params = json match {
      case Some(map: Map[_,_]) => map.asInstanceOf[Map[String,AnyRef]].asJava
      case _ => throw new IllegalStateException(s"Unable to parse json file containing params at $paramsPath")
    }

    ImportQuery(script, params, basePath)
  }
}

case class DatabaseFactory(dbDir: JFile) extends ((String) => Unit) {
  override def apply(dbName: String): Unit = {
    val ImportQuery(script, params, fileRoot) = AvailableDatabase.archive(dbName)
    val dbPath = new JFile(dbDir, dbName)
    if (!dbPath.exists()) {
      val graph = new GraphDatabaseFactory()
        .newEmbeddedDatabaseBuilder(dbPath)
        .setConfig(GraphDatabaseSettings.load_csv_file_url_root, fileRoot.toAbsolutePath.toString)
        .newGraphDatabase()
      script.split(';').filter(_.trim.nonEmpty) foreach { q =>
        graph.execute(q.trim, params)
      }
      graph.shutdown()
    }
    AvailableDatabase.dbPaths += dbName -> dbPath.getAbsoluteFile
  }
}

object DatabaseLoader extends ((String) => JFile) {
  override def apply(dbName: String): JFile = AvailableDatabase.dbPaths(dbName)
}
