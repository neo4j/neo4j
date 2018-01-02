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
package org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.Rewriter
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class RewriterStepSequencerTest extends CypherFunSuite {

  test("if no conditions are used, what goes in is what comes out") {
    val dummyRewriter1 = Rewriter.noop
    val dummyRewriter2 = Rewriter.lift { case x: AnyRef => x }

    RewriterStepSequencer.newValidating("test")() should equal(RewriterContract(Seq(), Set()))
    RewriterStepSequencer.newValidating("test")(ApplyRewriter("1", dummyRewriter1), ApplyRewriter("2", dummyRewriter2)) should equal(RewriterContract(Seq(dummyRewriter1, dummyRewriter2), Set()))
  }

  test("Should enable conditions between rewriters and collect the post conditions at the end") {
    val dummyCond1 = RewriterCondition("a", (x: Any) => Seq("1"))
    val dummyCond2 = RewriterCondition("b", (x: Any) => Seq("2"))
    val dummyRewriter1 = Rewriter.noop
    val dummyRewriter2 = Rewriter.lift { case x: AnyRef => x }

    val sequencer = RewriterStepSequencer.newValidating("test")(
      ApplyRewriter("1", dummyRewriter1),
      EnableRewriterCondition(dummyCond1),
      ApplyRewriter("2", dummyRewriter2),
      EnableRewriterCondition(dummyCond2)
    )

    sequencer.childRewriters should equal(Seq(
      dummyRewriter1,
      RunConditionRewriter("test", Some("1"), Set(dummyCond1)),
      dummyRewriter2,
      RunConditionRewriter("test", Some("2"), Set(dummyCond1, dummyCond2))
    ))
    sequencer.postConditions should equal(Set(dummyCond1, dummyCond2))
  }

  test("Should enable/disable conditions between rewriters and collect the post conditions at the end") {
    val dummyCond1 = RewriterCondition("a", (x: Any) => Seq("1"))
    val dummyCond2 = RewriterCondition("b", (x: Any) => Seq("2"))
    val dummyRewriter1 = Rewriter.noop
    val dummyRewriter2 = Rewriter.lift { case x: AnyRef => x}
    val dummyRewriter3 = Rewriter.noop

    val sequencer = RewriterStepSequencer.newValidating("test")(
      ApplyRewriter("1", dummyRewriter1),
      EnableRewriterCondition(dummyCond1),
      ApplyRewriter("2", dummyRewriter2),
      EnableRewriterCondition(dummyCond2),
      ApplyRewriter("3", dummyRewriter3),
      DisableRewriterCondition(dummyCond2)
    )

    sequencer.childRewriters should equal(Seq(
      dummyRewriter1,
      RunConditionRewriter("test", Some("1"), Set(dummyCond1)),
      dummyRewriter2,
      RunConditionRewriter("test", Some("2"), Set(dummyCond1, dummyCond2)),
      dummyRewriter3,
      RunConditionRewriter("test", Some("3"), Set(dummyCond1))
    ))
    sequencer.postConditions should equal(Set(dummyCond1))
  }
}
