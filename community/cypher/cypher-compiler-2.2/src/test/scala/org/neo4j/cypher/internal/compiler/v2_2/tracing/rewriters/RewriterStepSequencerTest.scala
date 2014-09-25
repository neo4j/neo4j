/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.Rewriter

class RewriterStepSequencerTest extends CypherFunSuite {

  test("if no conditions are used, what goes in is what comes out") {
    val dummyRewriter1 = Rewriter.noop
    val dummyRewriter2 = Rewriter.lift { case x: AnyRef => x }

    RewriterStepSequencer.newValidating("test").fromStepSeq(List.empty) should equal(Seq())
    RewriterStepSequencer.newValidating("test").fromStepSeq(List(ApplyRewriter("1", dummyRewriter1), ApplyRewriter("2", dummyRewriter2))) should equal(Seq(dummyRewriter1, dummyRewriter2))
  }
}
