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
package org.neo4j.cypher.internal.commands

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.commands.expressions._
import org.neo4j.cypher.internal.commands.values.{TokenType, KeyToken}
import org.neo4j.cypher.internal.commands.values.TokenType.Label
import org.neo4j.cypher.internal.parser.{On, OnAction, ParsedEntity}
import org.neo4j.cypher.internal.mutation.PropertySetAction
import org.neo4j.cypher.internal.mutation.MergeNodeAction
import org.neo4j.cypher.internal.commands.expressions.TimestampFunction
import org.neo4j.cypher.internal.commands.expressions.Nullable
import org.neo4j.cypher.internal.commands.expressions.Property


class MergeAstTest extends Assertions {
  @Test
  def simple_node_without_labels_or_properties() {
    // given
    val from = MergeAst(Seq(ParsedEntity(A, Identifier(A), Map.empty, Seq.empty, bare = true)), Seq.empty)

    // then
    assert(from.nextStep() === Seq(MergeNodeAction(A, Seq.empty, Seq.empty, Seq.empty, None)))
  }

  @Test
  def node_with_labels() {
    // given
    val from = MergeAst(Seq(ParsedEntity(A, Identifier(A), Map.empty, Seq(KeyToken.Unresolved(labelName, Label)), bare = true)), Seq.empty)

    // then
    val a = from.nextStep()
    val b = Seq(MergeNodeAction(A,
      expectations = Seq(nodeHasLabelPredicate(A)),
      onCreate = Seq(setNodeLabels(A)),
      onMatch = Seq.empty,
      nodeProducerOption = NO_PRODUCER))
    assert(a === b)
  }

  @Test
  def node_with_properties() {
    // given
    val from = MergeAst(Seq(ParsedEntity(A, Identifier(A), Map(propertyKey -> expression), Seq.empty, bare = true)), Seq.empty)

    // then
    assert(from.nextStep() === Seq(MergeNodeAction(A,
      expectations = Seq(Equals(Nullable(Property(Identifier(A), propertyKey)), expression)),
      onCreate = Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)),
      onMatch = Seq.empty,
      nodeProducerOption = NO_PRODUCER)))
  }

  @Test
  def node_with_on_create() {
    // given MERGE A ON CREATE SET A.prop = exp
    val from = MergeAst(
      Seq(
        ParsedEntity(A, Identifier(A), Map.empty, Seq.empty, bare = true)),
      Seq(
        OnAction(On.Create, A, Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)))))

    // then
    assert(from.nextStep() === Seq(MergeNodeAction(A,
      expectations = Seq.empty,
      onCreate = Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)),
      onMatch = Seq.empty,
      nodeProducerOption = NO_PRODUCER)))
  }

  @Test
  def node_with_on_match() {
    // given MERGE A ON MATCH SET A.prop = exp
    val from = MergeAst(
      Seq(
        ParsedEntity(A, Identifier(A), Map.empty, Seq.empty, bare = true)),
      Seq(
        OnAction(On.Match, A, Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)))))

    // then
    assert(from.nextStep() === Seq(MergeNodeAction(A,
      expectations = Seq.empty,
      onCreate = Seq.empty,
      onMatch = Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)),
      nodeProducerOption = NO_PRODUCER)))
  }

  val A = "a"
  val B = "b"
  val NO_PATHS = Seq.empty
  val NO_PRODUCER = None
  val labelName = "Label"
  val propertyKey = "property"
  val expression = TimestampFunction()

  def nodeHasLabelPredicate(id: String) = HasLabel(Identifier(id), KeyToken.Unresolved(labelName, TokenType.Label))

  def setNodeLabels(id: String) = LabelAction(Identifier(id), LabelSetOp, Seq(KeyToken.Unresolved(labelName, TokenType.Label)))

  def setProperty(id: String) = PropertySetAction(Property(Identifier(id), propertyKey), expression)

}