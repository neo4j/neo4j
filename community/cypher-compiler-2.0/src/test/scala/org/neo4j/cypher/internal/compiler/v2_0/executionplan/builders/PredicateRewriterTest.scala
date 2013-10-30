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

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.PlanBuilder
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v2_0.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_0.commands.HasLabel
import org.neo4j.cypher.internal.compiler.v2_0.commands.SingleNode
import org.neo4j.graphdb.Direction
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.util

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
    val result = assertAccepts(q)

    // Then
    assert(result.query.where === expectedWhere.map(Unsolved(_)))
    assert(result.query.patterns === expectedPattern.map(Unsolved(_)))
  }
}

object PredicateRewriterTest {

  val label = UnresolvedLabel("Person")
  val labeledA = SingleNode("a", Seq(label))
  val labeledB = SingleNode("b", Seq(label))
  val bareA = SingleNode("a")
  val bareB = SingleNode("b")
  val relationshipLabeledBoth = RelatedTo(labeledA, labeledB, "r", Seq.empty, Direction.OUTGOING, false)
  val relationshipLabeledLeft = relationshipLabeledBoth.copy(right = bareB)
  val relationshipLabeledRight = relationshipLabeledBoth.copy(left = bareA)
  val relationshipLabeledNone = relationshipLabeledLeft.copy(left = bareA)
  val varlengthRelatedToNoLabels = VarLengthRelatedTo("p", bareA, bareB, None, None, Seq(), Direction.OUTGOING, None, false)
  val predicateA = HasLabel(Identifier("a"), label)
  val predicateB = HasLabel(Identifier("b"), label)
  val shortestPathNoLabels = ShortestPath("p", bareA, bareB, Seq.empty, Direction.OUTGOING, None, optional = false, single = false, None)


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
      Seq(predicateA),
      Seq(bareA)
    )

    add("MATCH a:Person-->b RETURN a => MATCH a-->b WHERE a:Person RETURN a",
      relationshipLabeledLeft,
      Seq(predicateA),
      Seq(relationshipLabeledNone)
    )

    add("MATCH a-->b:Person RETURN a => MATCH a-->b WHERE b:Person RETURN a",
      relationshipLabeledRight,
      Seq(predicateB),
      Seq(relationshipLabeledNone)
    )

    add("MATCH (a)-->(b?:Person) RETURN a => :(",
      relationshipLabeledNone.copy(right = labeledB.copy(optional = true)),
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
      Seq(predicateA),
      Seq(varlengthRelatedToNoLabels)
    )

    add("MATCH p = a-[*]->b:Person RETURN a => MATCH p = a-[*]->b WHERE b:Person RETURN a",
      varlengthRelatedToNoLabels.copy(right = labeledB),
      Seq(predicateB),
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
      Seq(predicateA),
      Seq(shortestPathNoLabels)
    )

    add("MATCH p = shortestPath(a-[*]->b:Person) => MATCH p = shortestPath(a-[*]->b) WHERE b:Person",
      shortestPathNoLabels.copy(right = labeledB),
      Seq(predicateB),
      Seq(shortestPathNoLabels)
    )

    add("MATCH p = shortestPath(a-[*]->b?:Person) RETURN a => :(",
      shortestPathNoLabels.copy(right = labeledB.copy(optional = true)),
      Seq(),
      Seq()
    )

    list
  }
}