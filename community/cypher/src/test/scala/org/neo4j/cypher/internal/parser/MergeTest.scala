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
package org.neo4j.cypher.internal.parser

import org.junit.Test
import org.neo4j.cypher.internal.commands._
import expressions.Property
import expressions.TimestampFunction
import org.neo4j.cypher.internal.mutation.{PropertySetAction, MergeNodeAction}
import expressions._
import org.neo4j.cypher.internal.commands.values.LabelName
import org.neo4j.cypher.internal.parser.experimental.{Updates, StartAndCreateClause, MatchClause}
import org.neo4j.cypher.internal.mutation.PropertySetAction
import org.neo4j.cypher.internal.mutation.MergeNodeAction
import org.neo4j.cypher.internal.commands.MergeNodeStartItem
import org.neo4j.cypher.internal.commands.LabelAction
import values.LabelName
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.HasLabel


class MergeTest extends StartAndCreateClause with MatchClause with Updates with ParserTest {
  @Test def tests() {
    implicit val parserToTest = createStart
    val node = "nodeName"
    val A = "a"
    val B = "b"
    val NO_PATHS = Seq.empty
    val NO_PRODUCER = None
    def nodeHasLabelPredicate(id: String) = HasLabel(Identifier(id), LabelName("Label"))
    def setNodeLabels(id: String) = LabelAction(Identifier(id), LabelSetOp, Seq(LabelName("Label")))
    def setProperty(id: String) = PropertySetAction(Property(Identifier(id), "property"), TimestampFunction())

    parsing("MERGE (nodeName)") shouldGive
      (Seq(MergeNodeStartItem(MergeNodeAction(node,
        expectations = Seq.empty,
        onCreate = Seq.empty,
        onMatch = Seq.empty,
        nodeProducerOption = NO_PRODUCER))), NO_PATHS)

    parsing("MERGE (nodeName {prop:42})") shouldGive
      (Seq(MergeNodeStartItem(MergeNodeAction(node,
        expectations = Seq(Equals(Property(Identifier("nodeName"), "prop"), Literal(42))),
        onCreate = Seq(PropertySetAction(Property(Identifier("nodeName"), "prop"), Literal(42))),
        onMatch = Seq.empty,
        nodeProducerOption = NO_PRODUCER))), NO_PATHS)


    parsing("MERGE (nodeName:Label)") shouldGive
      (Seq(MergeNodeStartItem(MergeNodeAction(node,
        expectations = Seq(nodeHasLabelPredicate(node)),
        onCreate = Seq(setNodeLabels(node)),
        onMatch = Seq.empty,
        nodeProducerOption = NO_PRODUCER))),
        NO_PATHS)

    parsing("MERGE (nodeName:Label) ON CREATE nodeName SET nodeName.property = timestamp()") shouldGive
      (Seq(MergeNodeStartItem(MergeNodeAction(node,
        expectations = Seq(nodeHasLabelPredicate(node)),
        onCreate = Seq(setProperty(node), setNodeLabels(node)),
        onMatch = Seq.empty,
        nodeProducerOption = NO_PRODUCER))),
        NO_PATHS)

    parsing("MERGE (nodeName:Label) ON MATCH nodeName SET nodeName.property = timestamp()") shouldGive
      (Seq(MergeNodeStartItem(MergeNodeAction(node,
        expectations = Seq(nodeHasLabelPredicate(node)),
        onCreate = Seq(setNodeLabels(node)),
        onMatch = Seq(setProperty(node)),
        nodeProducerOption = NO_PRODUCER))),
        NO_PATHS)

    parsing(
      """MERGE (a:Label)
MERGE (b:Label)
ON MATCH a SET a.property = timestamp()
ON CREATE a SET a.property = timestamp()
ON CREATE b SET b.property = timestamp()
ON MATCH b SET b.property = timestamp()
      """) shouldGive
      (Seq(
        MergeNodeStartItem(MergeNodeAction(A,
          expectations = Seq(nodeHasLabelPredicate(A)),
          onCreate = Seq(setProperty(A), setNodeLabels(A)),
          onMatch = Seq(setProperty(A)),
          nodeProducerOption = NO_PRODUCER)),

        MergeNodeStartItem(MergeNodeAction(B,
          expectations = Seq(nodeHasLabelPredicate(B)),
          onCreate = Seq(setProperty(B), setNodeLabels(B)),
          onMatch = Seq(setProperty(B)),
          nodeProducerOption = NO_PRODUCER))),
        Seq.empty)
  }

  def createProperty(entity: String, propName: String): Expression = Property(Identifier(entity), propName)
}