/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.docgen.tooling

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.graphdb.GraphDatabaseService

case class Document(title: String, initQueries: Seq[String], content: Content) {
  def tests: Seq[(String, QueryAssertions)] = content.tests
}

sealed trait Content {
  def ~(other: Content): Content = ContentChain(this, other)
  def tests: Seq[(String, QueryAssertions)]
}

trait NoTests {
  self : Content =>
  def tests: Seq[(String, QueryAssertions)] = Seq.empty
}

case class ContentChain(a: Content, b: Content) extends Content {
  override def tests: Seq[(String, QueryAssertions)] = a.tests ++ b.tests
}
case class Title(s: String) extends Content with NoTests
case class Abstract(s: String) extends Content with NoTests
case class Paragraph(s: String) extends Content with NoTests
case class Tip(s: String) extends Content with NoTests
case class GraphImage(s: ImageType) extends Content with NoTests
case class Query(queryText: String, assertions: QueryAssertions, content: Content) extends Content {
  override def tests: Seq[(String, QueryAssertions)] = Seq(queryText -> assertions)
}
case class Section(heading: String, content: Content) extends Content {
  override def tests: Seq[(String, QueryAssertions)] = content.tests
}

sealed trait QueryAssertions

case class ResultAssertions(f: InternalExecutionResult => Unit) extends QueryAssertions
case class ResultAndDbAssertions(f: (InternalExecutionResult, GraphDatabaseService) => Unit) extends QueryAssertions
case object NoAssertions extends QueryAssertions
case object QueryResultTable extends Content with NoTests

trait ImageType

object ImageType {
  case object INITIAL extends ImageType
}
