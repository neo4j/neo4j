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
package cypher.feature.parser

import java.util

import cucumber.api.DataTable
import cypher.feature.parser.matchers.{ResultMatcher, RowMatcher, ValueMatcher}

import scala.collection.JavaConverters._

/**
  * Parses the expected results of the Cucumber features and constructs a matcher
  * that can be used to match on a {@link org.neo4j.graphdb.Result Result} object.
  */
object constructResultMatcher extends ((DataTable, Boolean) => ResultMatcher) {

  override def apply(table: DataTable, unorderedLists: Boolean = false): ResultMatcher = {
    val keys = table.topCells().asScala
    val cells = table.cells(1).asScala

    new ResultMatcher(cells.map { list =>
      new RowMatcher(list.asScala.zipWithIndex.map { case (value, index) =>
        (keys(index), matcherParser(value, unorderedLists))
      }.toMap.asJava)
    }.toList.asJava)
  }
}

/**
  * Parses one cell of the expected results in Cucumber feature files and constructs
  * a matcher than can be used to match a certain Cypher value.
  */
object matcherParser extends ((String, Boolean) => ValueMatcher) {

  // has to be a def to renew the instance
  private def parser = new ResultsParser
  private val listener = new CypherMatchersCreator

  def apply(input: String, unorderedLists: Boolean): ValueMatcher = {
    parser.matcherParse(input, listener.setLists(unorderedLists))
  }
}

/**
  * Parses the parameter data table in Cucumber feature files, and constructs a parameter map
  * to be sent for execution alongside a parameterized Cypher query.
  */
object parseParameters extends (DataTable => java.util.Map[String, AnyRef]) {

  override def apply(input: DataTable): util.Map[String, AnyRef] = {
    val keys = input.transpose().topCells().asScala
    val values = input.transpose().cells(1).asScala.head

    keys.zipWithIndex.map { case (key, index) =>
      key -> paramsParser(values.get(index))
    }.toMap.asJava
  }
}

/**
  * Parses a whole data table into a list of column names and a list of maps from column names to values.
  *
  * Values in each cell are parsed as if they were parameters.
  */
object parseValueTable extends (DataTable => (List[String], List[Array[AnyRef]])) {
  override def apply(input: DataTable): (List[String], List[Array[AnyRef]]) = {
    val keys = input.topCells().asScala.toList
    val builder = List.newBuilder[Array[AnyRef]]
    input.cells(1).asScala.foreach { values =>
      builder += values.asScala.map(paramsParser).toArray
    }
    (keys, builder.result())
  }
}

/**
  * Parses a single cell containing a parameter value, and constructs an object with
  * correct type and state for Cypher consumption.
  */
object paramsParser extends (String => AnyRef) {

  // has to be a def to renew the instance
  private def parser = new ResultsParser
  private val listener = new CypherParametersCreator

  def apply(input: String): AnyRef = {
    parser.parseParameter(input, listener)
  }
}
