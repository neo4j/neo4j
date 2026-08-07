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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.InferSchemaPartsStrategy
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.UsingExpandStepHint
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ExpandHints
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.exceptions.HintException
import org.neo4j.exceptions.SyntaxException

class ExpandHintPlanningIntegrationTest
    extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanningAttributesTestSupport {

  private val plannerBuilderWithoutFeature =
    plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("()-[]->()", 500)
      .setRelationshipCardinality("(:A)-[]->()", 200)
      // when starting in a, it is cheaper to expand the incoming edges first (100 < 200)
      .setRelationshipCardinality("()-[]->(:A)", 100)

  private val plannerBuilderWithFeature =
    plannerBuilderWithoutFeature
      .addSemanticFeature(ExpandHints)

  test("should be able to enforce expand to be solved in a specific direction - one relationship") {
    val planner = plannerBuilderWithFeature.build()

    def query(hint: String) =
      s"""MATCH (a:A)-->(b)
         |$hint
         |RETURN a, b""".stripMargin

    withClue("without the hint") {
      planner.plan(
        query("")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b")
          .expandAll("(a)-->(b)")
          .nodeByLabelScan("a", "A")
          .build()
      )
    }

    withClue("with the hint") {
      planner.plan(
        query("USING EXPAND FROM b TO a")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b")
          .filter("a:A")
          .expandAll("(b)<--(a)")
          .allNodeScan("b")
          .build()
      )
    }
  }

  test("should be able to enforce expand to be solved in a specific direction - two relationships outwards") {
    val planner = plannerBuilderWithFeature.build()

    def query(hint: String) =
      s"""MATCH (a)<--(b:A)-->(c)
         |$hint
         |RETURN a, b, c""".stripMargin

    withClue("with hint to expand right first and then left") {
      planner.plan(
        query("USING EXPAND FROM b TO c, FROM b TO a")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c")
          .filter("NOT anon_1 = anon_0")
          .expandAll("(b)-[anon_0]->(a)")
          .expandAll("(b)-[anon_1]->(c)")
          .nodeByLabelScan("b", "A")
          .build()
      )
    }

    withClue("with hint to expand left first and then right") {
      planner.plan(
        query("USING EXPAND FROM b TO a, FROM b TO c")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c")
          .filter("NOT anon_1 = anon_0")
          .expandAll("(b)-[anon_1]->(c)")
          .expandAll("(b)-[anon_0]->(a)")
          .nodeByLabelScan("b", "A")
          .build()
      )
    }

    withClue("with hint to expand left to right") {
      planner.plan(
        query("USING EXPAND FROM a TO b, FROM b TO c")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c")
          .filter("NOT anon_1 = anon_0")
          .expandAll("(b)-[anon_1]->(c)")
          .filter("b:A")
          .expandAll("(a)<-[anon_0]-(b)")
          .allNodeScan("a")
          .build()
      )
    }
  }

  test("should throw HintException when expand hint references variables with no connecting relationship") {
    val planner = plannerBuilderWithFeature.build()

    for {
      pattern <- Seq(
        // same path pattern but not directly connected
        "(a:A)-[r1]->(b)-[r2]->(c)",
        // disconnected components
        "(a:A)-[r1]->(b), (c)"
      )
    } {
      withClue(pattern) {
        a[HintException] should be thrownBy {
          planner.plan(
            s"""MATCH $pattern
               |USING EXPAND FROM a TO c
               |RETURN a, b, c""".stripMargin
          )
        }
      }
    }
  }

  test("should fail on USING EXPAND hint when ExpandHints feature is not enabled") {
    val planner = plannerBuilderWithoutFeature.build()

    val exception = the[SyntaxException] thrownBy {
      planner.plan(
        """MATCH (a:A)-->(b)
          |USING EXPAND FROM b TO a
          |RETURN a, b""".stripMargin
      )
    }
    exception.getMessage should include(
      "`USING EXPAND` is not available in this implementation of Cypher due to lack of support for Expand hints."
    )
  }

  test("should enforce expand direction for variable-length patterns") {
    val planner = plannerBuilderWithFeature.build()

    def query(hint: String) =
      s"""MATCH (a:A)-[r*1..3]->(b)
         |$hint
         |RETURN a, b, r""".stripMargin

    withClue("without hint: starts from the cheaper label-scanned node") {
      planner.plan(
        query("")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "r")
          .expand("(a)-[r*1..3]->(b)")
          .nodeByLabelScan("a", "A")
          .build()
      )
    }

    withClue("with hint FROM b TO a: starts from the all-node scan side") {
      planner.plan(
        query("USING EXPAND FROM b TO a")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "r")
          .filter("a:A")
          .expand("(b)<-[r*1..3]-(a)")
          .allNodeScan("b")
          .build()
      )
    }
  }

  test("should enforce a 3-step expand chain regardless of textual ordering") {
    val planner = plannerBuilderWithFeature.build()

    def query(hint: String) =
      s"""MATCH (a)<--(b:A)-->(c)-->(d)
         |$hint
         |RETURN a, b, c, d""".stripMargin

    withClue("path order (b→c), (c→d), (b→a) — expands stack bottom-up in that order") {
      planner.plan(
        query("USING EXPAND FROM b TO c, FROM c TO d, FROM b TO a")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c", "d")
          .filter("NOT anon_2 = anon_0", "NOT anon_1 = anon_0")
          .expandAll("(b)-[anon_0]->(a)")
          .filter("NOT anon_2 = anon_1")
          .expandAll("(c)-[anon_2]->(d)")
          .expandAll("(b)-[anon_1]->(c)")
          .nodeByLabelScan("b", "A")
          .build()
      )
    }

    withClue("middle-branch order (b→c), (b→a), (c→d) — non-monotonic chain") {
      planner.plan(
        query("USING EXPAND FROM b TO c, FROM b TO a, FROM c TO d")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c", "d")
          .filter("NOT anon_2 = anon_1", "NOT anon_2 = anon_0")
          .expandAll("(c)-[anon_2]->(d)")
          .filter("NOT anon_1 = anon_0")
          .expandAll("(b)-[anon_0]->(a)")
          .expandAll("(b)-[anon_1]->(c)")
          .nodeByLabelScan("b", "A")
          .build()
      )
    }

    withClue("side-first order (b→a), (b→c), (c→d) — another valid permutation") {
      planner.plan(
        query("USING EXPAND FROM b TO a, FROM b TO c, FROM c TO d")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c", "d")
          .filter("NOT anon_2 = anon_1", "NOT anon_2 = anon_0")
          .expandAll("(c)-[anon_2]->(d)")
          .filter("NOT anon_1 = anon_0")
          .expandAll("(b)-[anon_1]->(c)")
          .expandAll("(b)-[anon_0]->(a)")
          .nodeByLabelScan("b", "A")
          .build()
      )
    }
  }

  test("should throw HintException when chain ordering is infeasible") {
    // The hint `FROM b TO a, FROM c TO d, FROM b TO c` is impossible to fulfil:
    // step 2 (c→d) requires c in scope, but c is only introduced by step 3 (b→c),
    // which the chain says must come last.
    // We could only solve this via all nodes scan on c, which we never consider.
    val planner = plannerBuilderWithFeature.build()

    val ex = the[HintException] thrownBy {
      planner.plan(
        """MATCH (a)<--(b:A)-->(c)-->(d)
          |USING EXPAND FROM b TO a, FROM c TO d, FROM b TO c
          |RETURN a, b, c, d""".stripMargin
      )
    }
    ex.getMessage should include("USING EXPAND")
  }

  test("should throw HintException when VIA chain ordering is infeasible") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[]->()", 200)
      .addSemanticFeature(ExpandHints)
      .build()

    // The same topology as the above "should throw HintException when chain ordering is infeasible"
    // test, but last hint is a VIA hints.
    val ex = the[HintException] thrownBy {
      planner.plan(
        """MATCH (a)<-[r1]-(b:A)-[r2]->(c)-[r3]->(d)
          |USING EXPAND FROM b TO a, FROM c TO d, VIA r2
          |RETURN *""".stripMargin
      )
    }
    ex.getMessage should include("USING EXPAND")
  }

  test("should constrain only hinted steps when chain covers a subset of pattern relationships") {
    val planner = plannerBuilderWithFeature.build()

    // Pattern has 3 relationships; chain hints only 2 of them.
    // The hinted pair (b→c) then (c→d) must appear in order; the (a)-->(b) expand
    // is unconstrained and is placed by the cost-based planner.
    planner.plan(
      """MATCH (a:A)-->(b)-->(c)-->(d)
        |USING EXPAND FROM b TO c, FROM c TO d
        |RETURN a, b, c, d""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c", "d")
        .filter("NOT anon_2 = anon_1", "NOT anon_2 = anon_0")
        .expandAll("(c)-[anon_2]->(d)")
        .filter("NOT anon_1 = anon_0")
        .expandAll("(b)-[anon_1]->(c)")
        .expandAll("(a)-[anon_0]->(b)")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should honour a 2-step expand chain inside OPTIONAL MATCH") {
    val planner = plannerBuilderWithFeature.build()

    planner.plan(
      """OPTIONAL MATCH (a)<--(b:A)-->(c)
        |USING EXPAND FROM b TO c, FROM b TO a
        |RETURN a, b, c""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c")
        .optional()
        .filter("NOT anon_1 = anon_0")
        .expandAll("(b)-[anon_0]->(a)")
        .expandAll("(b)-[anon_1]->(c)")
        .nodeByLabelScan("b", "A")
        .build()
    )
  }

  test("should apply expand hint in standalone OPTIONAL MATCH") {
    val planner = plannerBuilderWithFeature.build()

    def query(hint: String) =
      s"""OPTIONAL MATCH (a:A)-->(b)
         |$hint
         |RETURN a, b""".stripMargin

    withClue("without hint: starts from cheaper label-scanned side") {
      planner.plan(
        query("")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b")
          .optional()
          .expandAll("(a)-->(b)")
          .nodeByLabelScan("a", "A")
          .build()
      )
    }

    withClue("with hint FROM b TO a: direction is reversed inside the Optional") {
      planner.plan(
        query("USING EXPAND FROM b TO a")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b")
          .optional()
          .filter("a:A")
          .expandAll("(b)<--(a)")
          .allNodeScan("b")
          .build()
      )
    }
  }

  test("should accept USING EXPAND ALL/INTO on a simple expand") {
    val planner = plannerBuilderWithFeature.build()

    def query(mode: String) =
      s"""MATCH (a:A)-->(b)
         |USING EXPAND $mode FROM a TO b
         |RETURN a, b""".stripMargin

    withClue("Expanding ALL") {
      planner.plan(
        query("ALL")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b")
          .expandAll("(a)-->(b)")
          .nodeByLabelScan("a", "A")
          .build()
      )
    }

    withClue("Expanding INTO") {
      planner.plan(
        query("INTO")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b")
          .expandInto("(a)-[]->(b)")
          .cartesianProduct()
          .|.nodeByLabelScan("a", "A")
          .allNodeScan("b")
          .build()
      )(SymmetricalLogicalPlanEquality)
    }
  }

  test("should honour USING EXPAND INTO on a triangle-closing edge") {
    val planner = plannerBuilderWithFeature.build()

    // In this case, the planner would naturally start in `a` and then has the choice to go to b or c first.
    // To go to c first is preferred from the statistics, because there are fewer incoming relationships into a.
    // Here we try to force the opposite direction.
    planner.plan(
      """MATCH (a:A)-->(b)-->(c)-->(a)
        |USING EXPAND INTO FROM c TO a
        |RETURN a, b, c""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c")
        .filter("NOT anon_2 = anon_0", "NOT anon_2 = anon_1")
        .expandInto("(c)-[anon_2]->(a)")
        .filter("NOT anon_1 = anon_0")
        .expandAll("(b)-[anon_1]->(c)")
        .expandAll("(a)-[anon_0]->(b)")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should force a different plan topology when ALL vs INTO is used on the same edge") {
    // Swapping the qualifier on the `(c→a)` edge flips the topology the
    // planner must choose:
    // * with INTO the (c→a) edge must be the last ExpandInto,
    // * with ALL the planner is forced off the a-first topology and picks a
    //   different leaf/ordering so that (c→a) can be an ExpandAll.
    val planner = plannerBuilderWithFeature.build()

    def query(hint: String) =
      s"""MATCH (a:A)-->(b)-->(c)-->(a)
         |$hint
         |RETURN a, b, c""".stripMargin

    withClue("INTO: closing edge is ExpandInto, a-first topology") {
      planner.plan(
        query("USING EXPAND INTO FROM c TO a")
      ) should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c")
          .filter("NOT anon_2 = anon_0", "NOT anon_2 = anon_1")
          .expandInto("(c)-[anon_2]->(a)")
          .filter("NOT anon_1 = anon_0")
          .expandAll("(b)-[anon_1]->(c)")
          .expandAll("(a)-[anon_0]->(b)")
          .nodeByLabelScan("a", "A")
          .build()
      )
    }

    withClue("ALL: (c→a) is ExpandAll — solver picks a topology where a is introduced by it") {
      val plan = planner.plan(
        query("USING EXPAND ALL FROM c TO a")
      )
      plan should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c")
          .filter("NOT anon_2 = anon_0", "NOT anon_1 = anon_0")
          .expandInto("(a)-[anon_0]->(b)")
          .filter("NOT anon_2 = anon_1", "a:A")
          // the planner actually came up with a plan where we expand from c to a and do not have a bound yet.
          .expandAll("(c)-[anon_2]->(a)")
          .allRelationshipsScan("(b)-[anon_1]->(c)")
          .build()
      )
    }
  }

  test("should allow qualifiers on the first step of a chain") {
    val planner = plannerBuilderWithFeature.build()

    planner.plan(
      """MATCH (a:A)-->(b)-->(c)-->(a)
        |USING EXPAND ALL FROM a TO b, INTO FROM c TO a
        |RETURN a, b, c""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c")
        .filter("NOT anon_2 = anon_0", "NOT anon_2 = anon_1")
        .expandInto("(c)-[anon_2]->(a)")
        .filter("NOT anon_1 = anon_0")
        .expandAll("(b)-[anon_1]->(c)")
        .expandAll("(a)-[anon_0]->(b)")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should be able to use expand order hints to solve regression in stackoverflow macro benchmark") {
    val planner =
      plannerBuilder()
        // these are the statistics from the stackoverflow dataset
        .enableMinimumGraphStatistics()
        .defaultRelationshipCardinalityTo0()
        .setAllNodesCardinality(50752488)
        .setLabelCardinality("User", 8917507)
        .setLabelCardinality("Tag", 52445)
        .setLabelCardinality("Question", 16389567)
        .setLabelCardinality("Post", 41782536)
        .setLabelCardinality("Answer", 8703794)
        .setRelationshipCardinality("()-[]->()", 123922676)
        .setRelationshipCardinality("()-[:POSTED]->()", 41181660)
        .setRelationshipCardinality("(:User)-[:POSTED]->()", 41181660)
        .setRelationshipCardinality("()-[:POSTED]->(:Post)", 41181660)
        .setRelationshipCardinality("()-[:POSTED]->(:Question)", 16034672)
        .setRelationshipCardinality("()-[:POSTED]->(:Answer)", 8639849)
        .setRelationshipCardinality("()-[:TAGGED]->()", 48691700)
        .setRelationshipCardinality("(:Post)-[:TAGGED]->()", 48691700)
        .setRelationshipCardinality("(:Question)-[:TAGGED]->()", 48691700)
        .setRelationshipCardinality("(:Answer)-[:TAGGED]->()", 1)
        .setRelationshipCardinality("()-[:TAGGED]->(:Tag)", 48691700)
        .setRelationshipCardinality("()-[:SIMILAR]->()", 47592)
        .setRelationshipCardinality("(:Tag)-[:SIMILAR]->()", 47592)
        .setRelationshipCardinality("()-[:SIMILAR]->(:Tag)", 47592)
        // this is the setting that broke the plan
        .withSetting(
          GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
          InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
        )
        // ... and this is how we will fix it
        .addSemanticFeature(ExpandHints)
        .build()

    // Simplified version of the tricky part of stackoverflow query MostUsedSimilarTags
    val query =
      """MATCH (user:User), (tag: Tag)
        |  WHERE EXISTS {
        |    MATCH (user)-[:POSTED]->(q:Question)-[:TAGGED]->(otherTag:Tag)-[:SIMILAR*0..]-(tag)
        |    USING EXPAND FROM tag TO otherTag, FROM q TO otherTag
        |  }
        |RETURN *""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("tag", "user")
        .semiApply()
        .|.expandInto("(q)-[:TAGGED]->(otherTag)")
        .|.filter("otherTag:Tag")
        .|.bfsPruningVarExpand("(tag)-[:SIMILAR*0..2147483647]-(otherTag)")
        .|.filter("q:Question")
        .|.expandAll("(user)-[:POSTED]->(q)")
        .|.argument("user", "tag")
        .cartesianProduct()
        .|.nodeByLabelScan("user", "User")
        .nodeByLabelScan("tag", "Tag")
        .build()
    )
  }

  test("should solve USING EXPAND hint on the inner expand rewritten by triadic selection") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .setLabelCardinality("X", 20)
      .setRelationshipCardinality("(:X)-[]->()", 20)
      .setRelationshipCardinality("()-[]->()", 200)
      .addSemanticFeature(ExpandHints)
      .build()

    val query =
      """MATCH (a:X)-[r1]->(b)-[r2]->(c)
        |USING EXPAND FROM b TO c
        |WHERE NOT (a)-->(c)
        |RETURN 1""".stripMargin

    val state = planner.planState(query)
    val plan = state.logicalPlan

    plan should equal(
      planner.planBuilder()
        .produceResults("1")
        .projection("1 AS 1")
        .filter("not r2 = r1")
        // we expect a triadicSelection even if there is an expand hint
        .triadicSelection(positivePredicate = false, "a", "b", "c")
        .|.expandAll("(b)-[r2]->(c)")
        .|.argument("b", "r1")
        .expandAll("(a)-[r1]->(b)")
        .nodeByLabelScan("a", "X")
        .build()
    )

    // make sure that the hints are preserved
    state.planningAttributes.solveds
      .get(state.logicalPlan.id)
      .allHints
      .collect { case h: UsingExpandStepHint => h } should not be empty
  }

  test("should disambiguate among multiple rels between same endpoints via VIA") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .addSemanticFeature(ExpandHints)
      .build()

    val plan = planner.plan(
      """MATCH (a)-[r]->(b), (a)-[s]->(b)
        |USING EXPAND FROM a TO b VIA r
        |RETURN *""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("a", "b", "r", "s")
        .filter("NOT r = s")
        .expandInto("(a)-[r]->(b)")
        .allRelationshipsScan("(a)-[s]->(b)")
        .build()
    )
  }

  test("should treat USING EXPAND VIA as a pure ordering hint without direction enforcement") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .addSemanticFeature(ExpandHints)
      .build()

    val plan = planner.plan(
      """MATCH (a)-[r]->(b), (b)-[s]->(c)
        |USING EXPAND VIA s, VIA r
        |RETURN *""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c", "r", "s")
        .filter("NOT r = s")
        .expandAll("(b)<-[r]-(a)")
        .expandAll("(b)-[s]->(c)")
        .allNodeScan("b")
        .build()
    )
  }

  test("should order a QPP relative to a simple expand sharing endpoints") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .addSemanticFeature(ExpandHints)
      .build()

    val plan = planner.plan(
      """MATCH (a)-[r]->(b), (a)-[s]->*(b)
        |USING EXPAND VIA r, VIA s
        |RETURN *""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("a", "b", "r", "s")
        .filter("NOT r IN s")
        .expand("(a)-[s*0..]->(b)", expandMode = ExpandInto)
        .expandAll("(a)-[r]->(b)")
        .allNodeScan("a")
        .build()
    )
  }

  test("should enforce QPP iteration direction via FROM/TO + VIA") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .setLabelCardinality("A", 5)
      .setRelationshipCardinality("(:A)-[]->()", 5)
      .addSemanticFeature(ExpandHints)
      .build()

    val state = planner.planState(
      """MATCH (a:A)-[s]->*(b)
        |USING EXPAND FROM b TO a VIA s
        |RETURN *""".stripMargin
    )

    val plan = state.logicalPlan

    val claimedExpandHints =
      state.planningAttributes.solveds
        .get(plan.id)
        .allHints
        .collect { case h: UsingExpandStepHint => h }

    claimedExpandHints should not be empty

    plan should equal(
      planner.planBuilder()
        .produceResults("a", "b", "s")
        .filter("a:A")
        .expand("(b)<-[s*0..]-(a)")
        .allNodeScan("b")
        .build()
    )
  }

  test("should be able to hint order of fixed length relationship and QPP") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .addSemanticFeature(ExpandHints)
      .build()

    val trailParameters = TrailParameters(
      1,
      Unlimited,
      "outer_1",
      "outer_2",
      "a",
      "c",
      Set(("a", "a"), ("b", "b"), ("c", "c")),
      Set(("r", "r"), ("s", "s")),
      Set("r", "s"),
      Set(),
      Set(),
      false,
      ExpandAll,
      Set()
    )

    withClue("QPP first") {
      val plan = planner.plan(
        """MATCH (outer_1)((a)-[r]-(b)-[s]-(c))+(outer_2), (outer_1)-[t]->(outer_2)
          |USING EXPAND FROM outer_1 TO outer_2 VIA s, FROM outer_1 TO outer_2 VIA t
          |RETURN *""".stripMargin
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c", "outer_1", "outer_2", "r", "s", "t")
          .filter("NOT t IN r + s")
          .expandInto("(outer_1)-[t]->(outer_2)")
          .repeatTrail(trailParameters)
          .|.filter("NOT s = r", "isRepeatTrailUnique(s)")
          .|.expandAll("(b)-[s]-(c)")
          .|.filter("isRepeatTrailUnique(r)")
          .|.expandAll("(a)-[r]-(b)")
          .|.argument("a")
          .allNodeScan("outer_1")
          .build()
      )
    }

    withClue("fixed relationship first") {
      val plan = planner.plan(
        """MATCH (outer_1)((a)-[r]-(b)-[s]-(c))+(outer_2), (outer_1)-[t]->(outer_2)
          |USING EXPAND FROM outer_1 TO outer_2 VIA t, FROM outer_1 TO outer_2 VIA s
          |RETURN *""".stripMargin
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("a", "b", "c", "outer_1", "outer_2", "r", "s", "t")
          .repeatTrail(trailParameters.copy(expansionMode = ExpandInto, previouslyBoundRelationships = Set("t")))
          .|.filter("NOT s = r", "isRepeatTrailUnique(s)")
          .|.expandAll("(b)-[s]-(c)")
          .|.filter("isRepeatTrailUnique(r)")
          .|.expandAll("(a)-[r]-(b)")
          .|.argument("a")
          .expandAll("(outer_1)-[t]->(outer_2)")
          .allNodeScan("outer_1")
          .build()
      )
    }
  }

  test("should hint same multi-rel QPP via either inner relationship group var") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .addSemanticFeature(ExpandHints)
      .build()

    val planViaS = planner.plan(
      """MATCH (outer_1)((a)-[r]-(b)-[s]-(c))+(outer_2)
        |USING EXPAND FROM outer_1 TO outer_2 VIA s
        |RETURN *""".stripMargin
    ).stripProduceResults

    val planViaR = planner.plan(
      """MATCH (outer_1)((a)-[r]-(b)-[s]-(c))+(outer_2)
        |USING EXPAND FROM outer_1 TO outer_2 VIA r
        |RETURN *""".stripMargin
    ).stripProduceResults

    planViaS shouldEqual planViaR
  }

  test("should throw HintException when two VIA hints target the same Trail") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .addSemanticFeature(ExpandHints)
      .build()

    val ex = intercept[HintException] {
      planner.plan(
        """MATCH (outer_1)((a)-[r]-(b)-[s]-(c))+(outer_2)
          |USING EXPAND VIA r, VIA s
          |RETURN *""".stripMargin
      )
    }
    ex.getMessage should include("USING EXPAND")
  }

  test("should throw for non-sensical tuples") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(200)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[]->()", 200)
      .addSemanticFeature(ExpandHints)
      .build()

    for (
      hint <- Seq(
        // correct: "FROM b TO a VIA r1"
        "FROM b TO a VIA r2",
        "FROM b TO d VIA r1",
        "FROM c TO a VIA r1"
      )
    ) {
      withClue(hint) {
        val ex = intercept[HintException] {
          planner.plan(
            s"""MATCH (a)<-[r1]-(b:A)-[r2]->(c)-[r3]->(d)
               |USING EXPAND $hint
               |RETURN *""".stripMargin
          )
        }
        ex.getMessage should include("USING EXPAND")
      }
    }
  }
}
