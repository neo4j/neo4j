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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.neo4j.graphdb.Node
import collection.JavaConverters._
import org.neo4j.cypher.ExecutionResult

class LabelTest extends DocumentingTestBase {
  def graphDescription = List(
    "Anders:Person KNOWS Peter",
    "Stefan KNOWS Andres",
    "Michael KNOWS Stefan",
    "Emil KNOWS Peter",
    "Julian KNOWS Andres"
  )

  def section = "Label"


  @Test def add_a_label_to_a_node() {
    testQuery(
      title = "Add a label to a node",
      text = "To add a label to a node, use +ADD+ +LABEL+.",
      queryText = "start n = node(%Anders%) add n label :swedish return n",
      returns = "The newly labeled node is returned by the query.",
      assertions = (p) => { getLabelsFromNode(p) === List("swedish") }
    )
  }


  @Test def add_multiple_labels_to_a_node() {
    testQuery(
      title = "Add multiple labels to a node",
      text = "To add multiple labels to a node, use +ADD+ +LABEL+ and separate the different lables using +:+.",
      queryText = "start n = node(%Anders%) add n label :swedish:polish return n",
      returns = "The newly labeled node is returned by the query.",
      assertions = (p) => { getLabelsFromNode(p) === List("swedish", "polish") }
    )
  }


  private def getLabelsFromNode(p: ExecutionResult): Iterable[String] =
    p.columnAs[Node]("n").next().getLabels.asScala.map(_.name())
}