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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, HasLabel}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_3.mutation._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class MergeAstTest extends CypherFunSuite {

  test("simple_node_without_labels_or_properties") {
    // given
    val from = mergeAst(patterns = Seq(ParsedEntity(A, Identifier(A), Map.empty, Seq.empty)))

    // then
    from.nextStep() should equal(Seq(MergeNodeAction(A, Map.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, None)))
  }

  test("node_with_labels") {
    // given
    val from = mergeAst(patterns = Seq(ParsedEntity(A, Identifier(A), Map.empty, Seq(KeyToken.Unresolved(labelName, Label)))))

    // then
    val a = from.nextStep().head
    val b = Seq(MergeNodeAction(A,
      props = Map.empty,
      labels = Seq(Label(labelName)),
      expectations = Seq(nodeHasLabelPredicate(A)),
      onCreate = Seq(setNodeLabels(A)),
      onMatch = Seq.empty,
      maybeNodeProducer = NO_PRODUCER)).head

    a should equal(b)
  }

  test("node_with_properties") {
    // given
    val from = mergeAst(patterns = Seq(ParsedEntity(A, Identifier(A), Map(propertyKey.name -> expression), Seq.empty)))

    from.nextStep() should equal(Seq(MergeNodeAction(A,
      props = Map(propertyKey -> expression),
      labels = Seq.empty,
      expectations = Seq(Equals(Property(Identifier(A), propertyKey), expression)),
      onCreate = Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)),
      onMatch = Seq.empty,
      maybeNodeProducer = NO_PRODUCER)))
  }

  test("node_with_on_create") {
    // given MERGE A ON CREATE SET A.prop = exp
    val from = mergeAst(
      patterns = Seq(ParsedEntity(A, Identifier(A), Map.empty, Seq.empty)),
      onActions = Seq(OnAction(On.Create, Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)))))

    // then
    from.nextStep() should equal(Seq(MergeNodeAction(A,
      props = Map.empty,
      labels = Seq.empty,
      expectations = Seq.empty,
      onCreate = Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)),
      onMatch = Seq.empty,
      maybeNodeProducer = NO_PRODUCER)))
  }

  test("node_with_on_match") {
    // given MERGE A ON MATCH SET A.prop = exp
    val from = mergeAst(
      patterns = Seq(ParsedEntity(A, Identifier(A), Map.empty, Seq.empty)),
      onActions = Seq(OnAction(On.Match, Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)))))

    // then
    from.nextStep() should equal(Seq(MergeNodeAction(A,
      props = Map.empty,
      labels = Seq.empty,
      expectations = Seq.empty,
      onCreate = Seq.empty,
      onMatch = Seq(PropertySetAction(Property(Identifier(A), propertyKey), expression)),
      maybeNodeProducer = NO_PRODUCER)))
  }

  val A = "a"
  val B = "b"
  val NO_PATHS = Seq.empty
  val NO_PRODUCER = None
  val labelName = "Label"
  val propertyKey = PropertyKey("property")
  val expression = TimestampFunction()

  def nodeHasLabelPredicate(id: String) = HasLabel(Identifier(id), KeyToken.Unresolved(labelName, TokenType.Label))

  def setNodeLabels(id: String) = LabelAction(Identifier(id), LabelSetOp, Seq(KeyToken.Unresolved(labelName, TokenType.Label)))

  def setProperty(id: String) = PropertySetAction(Property(Identifier(id), propertyKey), expression)

  def mergeAst(patterns: Seq[AbstractPattern] = Seq.empty,
               onActions: Seq[OnAction] = Seq.empty,
               matches: Seq[Pattern] = Seq.empty,
               create: Seq[UpdateAction] = Seq.empty) = MergeAst(patterns, onActions, matches, create)
}
