/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CompressAnonymousVariablesTest extends CypherFunSuite
    with LogicalPlanningAttributesTestSupport
    with LogicalPlanConstructionTestSupport
    with AstConstructionTestSupport {

  test("should compress anonymous variables in simple plan") {
    val plan = new LogicalPlanBuilder()
      .produceResults("`  UNNAMED42`", "`  UNNAMED23`")
      .expandAll("(`  UNNAMED42`)-[`  UNNAMED23`]-(`  UNNAMED2`)")
      .nodeByLabelScan("`  UNNAMED2`", "Label")
      .build()

    rewrite(plan) should equal(
      new LogicalPlanBuilder()
        .produceResults("`  UNNAMED2`", "`  UNNAMED1`")
        .expandAll("(`  UNNAMED2`)-[`  UNNAMED1`]-(`  UNNAMED0`)")
        .nodeByLabelScan("`  UNNAMED0`", "Label")
        .build()
    )

    // we want the rewrite to be idempotent
    rewrite(plan) should equal(rewrite(rewrite(plan)))
  }

  test("should compress anonymous variables in NFAs") {
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (`  UNNAMED21`)")
      .addTransition(1, 2, "(`  UNNAMED21`)-[`  UNNAMED11`]->(`  UNNAMED22`)")
      .addTransition(2, 3, "(`  UNNAMED22`)-[`  UNNAMED12`]->(`  UNNAMED10`)")
      .addTransition(3, 1, "(`  UNNAMED10`) (`  UNNAMED21`)")
      .addTransition(3, 4, "(`  UNNAMED10`) (t)")
      .addFinalState(4)
      .build()

    rewrite(nfa) should equal(
      new TestNFABuilder(0, "s")
        .addTransition(0, 1, "(s) (`  UNNAMED3`)")
        .addTransition(1, 2, "(`  UNNAMED3`)-[`  UNNAMED1`]->(`  UNNAMED4`)")
        .addTransition(2, 3, "(`  UNNAMED4`)-[`  UNNAMED2`]->(`  UNNAMED0`)")
        .addTransition(3, 1, "(`  UNNAMED0`) (`  UNNAMED3`)")
        .addTransition(3, 4, "(`  UNNAMED0`) (t)")
        .addFinalState(4)
        .build()
    )

    // we want the rewrite to be idempotent
    rewrite(nfa) should equal(rewrite(rewrite(nfa)))
  }

  test("should compress anonymous variables in shortest path even in solved expression string") {
    val plan = new LogicalPlanBuilder()
      .produceResults("s")
      .statefulShortestPath(
        "s",
        "t",
        "SHORTEST 1 ((s) ((`  UNNAMED21`)-[`  UNNAMED11`:R]->(`  UNNAMED22`)-[`  UNNAMED12`:T]-(`  UNNAMED23`)-[`  UNNAMED13`:T]-(`  UNNAMED24`)-[`  UNNAMED14`:T]-(`  UNNAMED25`)-[`  UNNAMED15`:R]->(`  UNNAMED10`) WHERE NOT `  UNNAMED15` = `  UNNAMED11` AND NOT `  UNNAMED14` = `  UNNAMED13` AND NOT `  UNNAMED14` = `  UNNAMED12` AND NOT `  UNNAMED13` = `  UNNAMED12`){1, } (t) WHERE unique((((`  UNNAMED20` + `  UNNAMED16`) + `  UNNAMED17`) + `  UNNAMED18`) + `  UNNAMED19`))",
        None,
        Set(
          ("  UNNAMED21", "  UNNAMED27"),
          ("  UNNAMED22", "  UNNAMED28"),
          ("  UNNAMED25", "  UNNAMED29"),
          ("  UNNAMED23", "  UNNAMED30"),
          ("  UNNAMED24", "  UNNAMED26")
        ),
        Set(
          ("  UNNAMED11", "  UNNAMED20"),
          ("  UNNAMED13", "  UNNAMED17"),
          ("  UNNAMED15", "  UNNAMED19"),
          ("  UNNAMED12", "  UNNAMED16"),
          ("  UNNAMED14", "  UNNAMED18")
        ),
        Set("t" -> "t"),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "s")
          .addTransition(0, 1, "(s) (`  UNNAMED21`)")
          .addTransition(1, 2, "(`  UNNAMED21`)-[`  UNNAMED11`:R]->(`  UNNAMED22`)")
          .addTransition(2, 3, "(`  UNNAMED22`)-[`  UNNAMED12`:T]-(`  UNNAMED23`)")
          .addTransition(3, 4, "(`  UNNAMED23`)-[`  UNNAMED13`:T]-(`  UNNAMED24`)")
          .addTransition(4, 5, "(`  UNNAMED24`)-[`  UNNAMED14`:T]-(`  UNNAMED25`)")
          .addTransition(5, 6, "(`  UNNAMED25`)-[`  UNNAMED15`:R]->(`  UNNAMED10`)")
          .addTransition(6, 1, "(`  UNNAMED10`) (`  UNNAMED21`)")
          .addTransition(6, 7, "(`  UNNAMED10`) (t)")
          .addFinalState(7)
          .build(),
        ExpandAll,
        reverseGroupVariableProjections = false
      )
      .nodeByLabelScan("s", "User")
      .build()

    rewrite(plan) should equal(
      new LogicalPlanBuilder()
        .produceResults("s")
        .statefulShortestPath(
          "s",
          "t",
          "SHORTEST 1 ((s) ((`  UNNAMED11`)-[`  UNNAMED1`:R]->(`  UNNAMED12`)-[`  UNNAMED2`:T]-(`  UNNAMED13`)-[`  UNNAMED3`:T]-(`  UNNAMED14`)-[`  UNNAMED4`:T]-(`  UNNAMED15`)-[`  UNNAMED5`:R]->(`  UNNAMED0`) WHERE NOT `  UNNAMED5` = `  UNNAMED1` AND NOT `  UNNAMED4` = `  UNNAMED3` AND NOT `  UNNAMED4` = `  UNNAMED2` AND NOT `  UNNAMED3` = `  UNNAMED2`){1, } (t) WHERE unique((((`  UNNAMED10` + `  UNNAMED6`) + `  UNNAMED7`) + `  UNNAMED8`) + `  UNNAMED9`))",
          None,
          Set(
            ("  UNNAMED11", "  UNNAMED17"),
            ("  UNNAMED12", "  UNNAMED18"),
            ("  UNNAMED15", "  UNNAMED19"),
            ("  UNNAMED13", "  UNNAMED20"),
            ("  UNNAMED14", "  UNNAMED16")
          ),
          Set(
            ("  UNNAMED1", "  UNNAMED10"),
            ("  UNNAMED3", "  UNNAMED7"),
            ("  UNNAMED5", "  UNNAMED9"),
            ("  UNNAMED2", "  UNNAMED6"),
            ("  UNNAMED4", "  UNNAMED8")
          ),
          Set("t" -> "t"),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "s")
            .addTransition(0, 1, "(s) (`  UNNAMED11`)")
            .addTransition(1, 2, "(`  UNNAMED11`)-[`  UNNAMED1`:R]->(`  UNNAMED12`)")
            .addTransition(2, 3, "(`  UNNAMED12`)-[`  UNNAMED2`:T]-(`  UNNAMED13`)")
            .addTransition(3, 4, "(`  UNNAMED13`)-[`  UNNAMED3`:T]-(`  UNNAMED14`)")
            .addTransition(4, 5, "(`  UNNAMED14`)-[`  UNNAMED4`:T]-(`  UNNAMED15`)")
            .addTransition(5, 6, "(`  UNNAMED15`)-[`  UNNAMED5`:R]->(`  UNNAMED0`)")
            .addTransition(6, 1, "(`  UNNAMED0`) (`  UNNAMED11`)")
            .addTransition(6, 7, "(`  UNNAMED0`) (t)")
            .addFinalState(7)
            .build(),
          ExpandAll,
          reverseGroupVariableProjections = false
        )
        .nodeByLabelScan("s", "User")
        .build()
    )
  }

  private def rewrite(e: AnyRef): AnyRef = CompressAnonymousVariables(e)
}
