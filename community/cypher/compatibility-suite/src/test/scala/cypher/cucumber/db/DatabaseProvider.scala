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

import java.io.File

import cypher.cucumber.CucumberAdapter
import gherkin.formatter.model.{Feature, Tag}

import scala.collection.JavaConverters._
import scala.collection.mutable

object DBTagParser {
  private val TAG_PREFIX = "@db"
  private val SEPARATOR = ":"

  def isDBTag(tag: Tag): Boolean = tag.getName.startsWith(TAG_PREFIX + SEPARATOR)

  def parseDBName(tag: Tag): String = {
    val array = tag.getName.split(SEPARATOR)
    assert(array.length == 2)
    assert(array(0) == TAG_PREFIX)
    array(1)
  }
}

class DatabaseProvider(dbFactory: (String) => Unit) extends CucumberAdapter {

  private val builtDbs: mutable.Set[String] = new mutable.HashSet

  def this(reportDir: File) = {
    this(dbFactory = DatabaseFactory(reportDir))
  }

  override def feature(feature: Feature): Unit = {
    import DBTagParser.{isDBTag, parseDBName}

    feature.getTags.asScala.filter(isDBTag).foreach { tag =>
      val dbName = parseDBName(tag)
      if (!builtDbs(dbName)) {
        dbFactory(dbName)
        builtDbs += dbName
      }
    }
  }
}
