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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import commands.expressions.{Literal, Property, Identifier}
import commands.values.{UnresolvedProperty, UnresolvedLabel}
import executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.graphdb.Direction
import java.util
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.Test

@RunWith(value = classOf[Parameterized])
class PredicateRewriterTest(name: String,
                            inputMatchPattern: Pattern,
                            expectedWhere: Seq[Predicate],
                            expectedPattern: Seq[Pattern]) extends BuilderTest {
  def builder: PlanBuilder = new PredicateRewriter

  @Test def should_rewrite_patterns_with_labels() {
    // Given
    val q = Query.
      matches(inputMatchPattern).
      returns(ReturnItem(Identifier("a"), "a"))

    // When supposed to reject...
    if (expectedPattern.isEmpty) {
      assertRejects(q)
      return
    }

    // Otherwise, when supposed to accept...
    val result = untilDone(plan(q))

    // Then
    assert(result.query.where.toSet === expectedWhere.map(Unsolved.apply).toSet)
    assert(result.query.patterns.toSet === expectedPattern.map(Unsolved.apply).toSet)
  }

  private def untilDone(q: ExecutionPlanInProgress): ExecutionPlanInProgress =
    if (builder.canWorkWith(q, context))
      untilDone(builder(q, context))
    else q
}

object PredicateRewriterTest {
  val properties = Map("foo" -> Literal("bar"))
  val label = UnresolvedLabel("Person")

  // Nodes
  val labeledA = SingleNode("a", Seq(label))
  val labeledB = SingleNode("b", Seq(label))
  val propertiedA = SingleNode("a", properties = properties)
  val propertiedB = SingleNode("b", properties = properties)
  val bareA = SingleNode("a")
  val bareB = SingleNode("b")

  // Relationships
  val relationshipLabeledBoth = RelatedTo(labeledA, labeledB, "r", Seq.empty, Direction.OUTGOING, optional = false, Map.empty)
  val relationshipLabeledLeft = relationshipLabeledBoth.copy(right = bareB)
  val relationshipLabeledRight = relationshipLabeledBoth.copy(left = bareA)
  val relationshipBare = relationshipLabeledLeft.copy(left = bareA)
  val relationshipPropsOnBoth = relationshipBare.copy(left = propertiedA, right = propertiedB)
  val relationshipPropsOnLeft = relationshipBare.copy(left = propertiedA)
  val relationshipPropsOnRight = relationshipBare.copy(right = propertiedB)
  val varlengthRelatedToNoLabels = VarLengthRelatedTo("p", bareA, bareB, None, None, Seq(), Direction.OUTGOING, None, optional = false)


  val predicateForLabelA = HasLabel(Identifier("a"), label)
  val predicateForLabelB = HasLabel(Identifier("b"), label)
  val shortestPathNoLabels = ShortestPath("p", bareA, bareB, Seq.empty, Direction.OUTGOING, None, optional = false, single = false, None)

  val predicateForPropertiedA = Equals(Property(Identifier("a"), UnresolvedProperty("foo")), Literal("bar"))
  val predicateForPropertiedB = Equals(Property(Identifier("b"), UnresolvedProperty("foo")), Literal("bar"))

  @Parameters(name = "{0}")
  def parameters: util.Collection[Array[AnyRef]] = {
    val list = new util.ArrayList[Array[AnyRef]]()
    def add(name: String,
            inputMatchPattern: Pattern,
            expectedWhere: Seq[Predicate],
            expectedPattern: Seq[Pattern]) {
      list.add(Array(name, inputMatchPattern, expectedWhere, expectedPattern))
    }

    add("MATCH a RETURN a => :(",
      bareA,
      Seq(),
      Seq()
    )

    add("MATCH a:Person RETURN a => MATCH a WHERE a:Person RETURN a",
      labeledA,
      Seq(predicateForLabelA),
      Seq(bareA)
    )

    add("MATCH a:Person-->b RETURN a => MATCH a-->b WHERE a:Person RETURN a",
      relationshipLabeledLeft,
      Seq(predicateForLabelA),
      Seq(relationshipBare)
    )

    add("MATCH a-->b:Person RETURN a => MATCH a-->b WHERE b:Person RETURN a",
      relationshipLabeledRight,
      Seq(predicateForLabelB),
      Seq(relationshipBare)
    )

    add("MATCH (a)-->(b?:Person) RETURN a => :(",
      relationshipBare.copy(right = labeledB.copy(optional = true)),
      Seq(),
      Seq()
    )

    add("MATCH p = a-[*]->b RETURN a => :(",
      varlengthRelatedToNoLabels,
      Seq(),
      Seq()
    )

    add("MATCH p = a:Person-[*]->b RETURN a => MATCH p = a-[*]->b WHERE a:Person RETURN a",
      varlengthRelatedToNoLabels.copy(left = labeledA),
      Seq(predicateForLabelA),
      Seq(varlengthRelatedToNoLabels)
    )

    add("MATCH p = a-[*]->b:Person RETURN a => MATCH p = a-[*]->b WHERE b:Person RETURN a",
      varlengthRelatedToNoLabels.copy(right = labeledB),
      Seq(predicateForLabelB),
      Seq(varlengthRelatedToNoLabels)
    )

    add("MATCH p = a-[*]->b?:Person RETURN a => :(",
      varlengthRelatedToNoLabels.copy(right = labeledB.copy(optional = true)),
      Seq(),
      Seq()
    )

    add("MATCH p = shortestPath(a-[*]->b) RETURN a => :(",
      shortestPathNoLabels,
      Seq(),
      Seq()
    )

    add("MATCH p = shortestPath(a:Person-[*]->b) => MATCH p = shortestPath(a-[*]->b) WHERE a:Person",
      shortestPathNoLabels.copy(left = labeledA),
      Seq(predicateForLabelA),
      Seq(shortestPathNoLabels)
    )

    add("MATCH p = shortestPath(a-[*]->b:Person) => MATCH p = shortestPath(a-[*]->b) WHERE b:Person",
      shortestPathNoLabels.copy(right = labeledB),
      Seq(predicateForLabelB),
      Seq(shortestPathNoLabels)
    )

    add("MATCH p = shortestPath(a-[*]->b?:Person) RETURN a => :(",
      shortestPathNoLabels.copy(right = labeledB.copy(optional = true)),
      Seq(),
      Seq()
    )

    add("MATCH (a {foo:'bar'}) RETURN a => MATCH a WHERE a.foo='bar'",
      propertiedA,
      Seq(predicateForPropertiedA),
      Seq(bareA)
    )

    add("MATCH (a {foo:'bar'})-->(b) RETURN a => MATCH (a)-->(b) WHERE a.foo='bar'",
      relationshipPropsOnLeft,
      Seq(predicateForPropertiedA),
      Seq(relationshipBare)
    )

    add("MATCH (a)-->(b {foo:'bar'}) RETURN a => MATCH (a)-->(b) WHERE b.foo='bar'",
      relationshipPropsOnRight,
      Seq(predicateForPropertiedB),
      Seq(relationshipBare)
    )

    add("MATCH (a {foo:'bar'})-->(b {foo:'bar'}) RETURN a => MATCH (a)-->(b) WHERE a.foo = 'bar' AND b.foo='bar'",
      relationshipPropsOnBoth,
      Seq(predicateForPropertiedB, predicateForPropertiedA),
      Seq(relationshipBare)
    )

    list
  }
}
