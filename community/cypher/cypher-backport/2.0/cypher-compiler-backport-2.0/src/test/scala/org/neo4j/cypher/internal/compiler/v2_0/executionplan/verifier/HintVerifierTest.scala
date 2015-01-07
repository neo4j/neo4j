/**
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.verifier

import org.scalatest.Assertions
import org.junit.Test

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.verifiers.HintVerifier
import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.{LabelScanHintException, IndexHintException}
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_0.commands.Or
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_0.commands.Equals
import org.neo4j.cypher.internal.compiler.v2_0.commands.NodeByLabel
import org.neo4j.cypher.internal.compiler.v2_0.commands.SchemaIndex
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Property
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.UnresolvedLabel
import org.neo4j.graphdb.Direction

class HintVerifierTest extends Assertions {
  val labeledA = SingleNode("a", Seq(UnresolvedLabel("Person")))
  val relatedTo = RelatedTo(labeledA, SingleNode("b"), "r", Seq.empty, Direction.OUTGOING, Map.empty)

  @Test
  def throws_when_the_predicate_is_not_usable_for_index_seek() {
    //GIVEN
    val q = Query.empty.copy(
      where = Or(
        Equals(Property(Identifier("n"), PropertyKey("name")), Literal("Stefan")),
        Equals(Property(Identifier("n"), PropertyKey("age")), Literal(35))),
      hints = Seq(SchemaIndex("n", "Person", "name", AnyIndex, None)))

    //THEN
    intercept[IndexHintException](HintVerifier.verify(q))
  }

  @Test
  def throws_when_scan_is_forced_but_label_not_used_in_rest_of_query() {
    //GIVEN
    val q = Query.empty.copy(
      hints = Seq(NodeByLabel("n", "Person")))

    //THEN
    intercept[LabelScanHintException](HintVerifier.verify(q))
  }

  @Test
  def accepts_query_with_label_in_single_node() {
    //GIVEN  MATCH a:Person
    val q = Query.
      matches(SingleNode("a", Seq(UnresolvedLabel("Person")))).
      using(NodeByLabel("a", "Person")).
      returns()

    //THEN does not throw
    HintVerifier.verify(q)
  }

  @Test
  def throws_if_identifier_name_is_wrong() {
    //GIVEN  MATCH a:Person
    val q = Query.
      matches(labeledA).
      using(NodeByLabel("n", "Person")).
      returns()

    //THEN
    intercept[LabelScanHintException](HintVerifier.verify(q))
  }

  @Test
  def accepts_query_with_label_in_relationship_pattern() {
    //GIVEN  MATCH a:Person-->b
    val q = Query.
      matches(relatedTo).
      using(NodeByLabel("a", "Person")).
      returns()

    //THEN does not throw
    HintVerifier.verify(q)
  }

  @Test
  def accepts_query_with_label_on_the_right_side() {
    //GIVEN  MATCH b-->a:Person
    val q = Query.
      matches(RelatedTo(SingleNode("b"), labeledA, "r", Seq.empty, Direction.OUTGOING, Map.empty)).
      using(NodeByLabel("a", "Person")).
      returns()

    //THEN does not throw
    HintVerifier.verify(q)
  }

  @Test
  def accepts_if_equality_is_turned_around() {
    //GIVEN  MATCH a:Person-->b USING INDEX ON a:Person(foo)
    val q = Query.
      matches(relatedTo.copy(left = labeledA)).
      where(Equals(Literal("bar"), Property(Identifier("a"), PropertyKey("foo")))).
      using(SchemaIndex("a", "Person", "foo", AnyIndex, None)).
      returns()

    //THEN
    HintVerifier.verify(q)
  }

  @Test
  def throws_if_label_not_used() {
    //GIVEN  MATCH a-->b USING INDEX ON a:Person(foo)
    val q = Query.
      matches(relatedTo.copy(left = SingleNode("a"))).
      where(Equals(Property(Identifier("a"), PropertyKey("foo")), Literal("bar"))).
      using(SchemaIndex("a", "Person", "foo", AnyIndex, None)).
      returns()

    //THEN
    intercept[IndexHintException](HintVerifier.verify(q))
  }

}
