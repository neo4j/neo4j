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
package org.neo4j.cypher.internal.plandescription.rewrite

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.planDescription
import org.neo4j.cypher.internal.util.attribution.Id

class FusedPlanDescriptionArgumentRewriterTest extends CommunityCypherTestSuite {

  test("single fused pipeline should aggregate time and page cache hits/misses") {
    val argsLeaf1 = Seq(PageCacheHits(1), PageCacheMisses(10), Time(1000000), pipelineInfo(1, fused = true))
    val argsLeaf2 = Seq(PageCacheHits(2), PageCacheMisses(11), Time(2000000), pipelineInfo(1, fused = true))
    val argsPlan = Seq(PageCacheHits(3), PageCacheMisses(12), Time(3000000), pipelineInfo(1, fused = true))

    val leaf1 = planDescription(Id(0), "LEAF1", Seq.empty, argsLeaf1, Set())
    val leaf2 = planDescription(Id(1), "LEAF2", Seq.empty, argsLeaf2, Set())
    val plan = planDescription(Id(2), "ROOT", Seq(leaf1, leaf2), argsPlan, Set())

    val result = new FusedPlanDescriptionArgumentRewriter().rewrite(plan)

    val expectedArguments = Map(
      Id(0) -> Seq(
        pipelineInfo(1, fused = true),
        Time(1000000 + 2000000 + 3000000),
        PageCacheHits(1 + 2 + 3),
        PageCacheMisses(10 + 11 + 12)
      ),
      Id(1) -> Seq(pipelineInfo(1, fused = true)),
      Id(2) -> Seq(pipelineInfo(1, fused = true))
    )

    planArgumentTest(result, expectedArguments)
  }

  test("multiple fused pipelines should aggregate time and page cache hits/misses individually") {
    val allNodeScanArgs = Seq(PageCacheHits(1), PageCacheMisses(10), Time(1000000), pipelineInfo(0, fused = true))
    val filterArgs = Seq(PageCacheHits(2), PageCacheMisses(11), Time(2000000), pipelineInfo(0, fused = true))
    val skipArgs = Seq(PageCacheHits(3), PageCacheMisses(12), Time(3000000), pipelineInfo(1, fused = true))
    val limitArgs = Seq(PageCacheHits(4), PageCacheMisses(13), Time(4000000), pipelineInfo(1, fused = true))
    val produceResultArgs = Seq(Time(1000000), pipelineInfo(2, fused = false))

    val allNodeScan = planDescription(Id(0), "ALLNODESCAN", Seq.empty, allNodeScanArgs, Set())
    val filter = planDescription(Id(1), "FILTER", Seq(allNodeScan), filterArgs, Set())
    val skip = planDescription(Id(2), "SKIP", Seq(filter), skipArgs, Set())
    val limit = planDescription(Id(3), "LIMIT", Seq(skip), limitArgs, Set())
    val produceResult = planDescription(Id(4), "PRODUCERESULT", Seq(limit), produceResultArgs, Set())

    val result = new FusedPlanDescriptionArgumentRewriter().rewrite(produceResult)

    val expectedArguments = Map(
      Id(0) -> Seq(
        pipelineInfo(0, fused = true),
        Time(1000000 + 2000000),
        PageCacheHits(1 + 2),
        PageCacheMisses(10 + 11)
      ),
      Id(1) -> Seq(pipelineInfo(0, fused = true)),
      Id(2) -> Seq(
        pipelineInfo(1, fused = true),
        Time(3000000 + 4000000),
        PageCacheHits(3 + 4),
        PageCacheMisses(12 + 13)
      ),
      Id(3) -> Seq(pipelineInfo(1, fused = true)),
      Id(4) -> Seq(pipelineInfo(2, fused = false), Time(1000000))
    )

    planArgumentTest(result, expectedArguments)
  }

  test("not fused pipelines should not aggregate") {
    val produceResultsArgs = Seq(PageCacheHits(5), PageCacheMisses(1), Time(200000), pipelineInfo(1, fused = false))
    val projectArgs = Seq()
    val allNodesScanArgs = Seq(PageCacheHits(1), PageCacheMisses(2), Time(1000000), pipelineInfo(1, fused = true))

    val allNodesScan = planDescription(Id(0), "ALLNODESSCAN", Seq.empty, allNodesScanArgs, Set.empty)
    val project = planDescription(Id(1), "PROJECT", Seq(allNodesScan), projectArgs, Set.empty)
    val produceResults = planDescription(Id(2), "PRODUCERESULTS", Seq(project), produceResultsArgs, Set.empty)

    new FusedPlanDescriptionArgumentRewriter().rewrite(produceResults) shouldBe produceResults
  }

  test(
    "multiple explicitly and implicitly fused pipelines should aggregate time and page cache hits/misses individually"
  ) {
    val indexSeekArgs = Seq(
      Rows(5),
      DbHits(1),
      EstimatedRows(4),
      PageCacheHits(5),
      PageCacheMisses(10),
      Time(200000),
      pipelineInfo(1, fused = true)
    )
    val filterArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4), pipelineInfo(1, fused = true))
    val allNodeScanArgs = Seq(
      Rows(5),
      DbHits(1),
      EstimatedRows(4),
      PageCacheHits(5),
      PageCacheMisses(10),
      Time(200000),
      pipelineInfo(0, fused = true)
    )
    val projectArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4), pipelineInfo(0, fused = true))
    val applyArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4))
    val aggregationArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4), Time(100000), pipelineInfo(1, fused = true))
    val produceResultArgs = Seq(
      Rows(5),
      DbHits(1),
      EstimatedRows(4),
      PageCacheHits(5),
      PageCacheMisses(10),
      Time(200000),
      pipelineInfo(2, fused = false)
    )

    val indexSeek = planDescription(Id(0), "INDEXSEEK", Seq.empty, indexSeekArgs, Set())
    val filter = planDescription(Id(1), "FILTER", Seq(indexSeek), filterArgs, Set())
    val allNodeScan = planDescription(Id(2), "ALLNODESCAN", Seq.empty, allNodeScanArgs, Set())
    val project = planDescription(Id(3), "PROJECT", Seq(allNodeScan), projectArgs, Set())
    val apply = planDescription(Id(4), "APPLY", Seq(project, filter), applyArgs, Set())
    val aggregation = planDescription(Id(5), "AGGREGATION", Seq(apply), aggregationArgs, Set())
    val produceResult = planDescription(Id(6), "PRODUCERESULT", Seq(aggregation), produceResultArgs, Set())

    val result = new FusedPlanDescriptionArgumentRewriter().rewrite(produceResult)

    val expectedArguments = Map(
      Id(0) -> Seq(
        Rows(5),
        DbHits(1),
        EstimatedRows(4),
        PageCacheHits(5),
        PageCacheMisses(10),
        Time(300000),
        pipelineInfo(1, fused = true)
      ),
      Id(1) -> filterArgs,
      Id(2) -> allNodeScanArgs,
      Id(3) -> projectArgs,
      Id(4) -> applyArgs,
      Id(5) -> Seq(Rows(5), DbHits(1), EstimatedRows(4), pipelineInfo(1, fused = true)),
      Id(6) -> produceResultArgs
    )

    planArgumentTest(result, expectedArguments)
  }

  test("aggregate over two fused apply (ldbc_sf010 read 6)") {
    val nodeUniqueIndexSeek1 = planDescription(
      Id(0),
      "NodeUniqueIndexSeek",
      Seq.empty,
      Seq(pipelineInfo(0, fused = false), PageCacheHits(2), PageCacheMisses(0), Time(8850000))
    )

    val nodeUniqueIndexSeek2 = planDescription(
      Id(1),
      "NodeUniqueIndexSeek",
      Seq.empty,
      Seq(pipelineInfo(1, fused = true), PageCacheHits(1537), PageCacheMisses(0), Time(21756000))
    )
    val varLengthExpand = planDescription(
      Id(2),
      "VarLengthExpand(All)",
      Seq(nodeUniqueIndexSeek2),
      Seq(pipelineInfo(1, fused = true))
    )
    val filter1 = planDescription(Id(3), "Filter", Seq(varLengthExpand), Seq(pipelineInfo(1, fused = true)))

    val cartesianProduct = planDescription(
      Id(4),
      "CartesianProduct",
      Seq(nodeUniqueIndexSeek1, filter1),
      Seq(pipelineInfo(2, fused = false), Time(5483000))
    )

    val distinct = planDescription(
      Id(5),
      "Distinct",
      Seq(cartesianProduct),
      Seq(pipelineInfo(2, fused = false), PageCacheHits(0), PageCacheMisses(0), Time(15565000))
    )

    val argument1 = planDescription(
      Id(6),
      "Argument",
      Seq.empty,
      Seq(pipelineInfo(3, fused = true), PageCacheHits(10957), PageCacheMisses(0), Time(65466000))
    )
    val expandAll1 = planDescription(Id(7), "Expand(All)", Seq(argument1), Seq(pipelineInfo(3, fused = true)))

    val argument2 = planDescription(
      Id(8),
      "Argument",
      Seq.empty,
      Seq(pipelineInfo(4, fused = true), PageCacheHits(1211031), PageCacheMisses(0), Time(577688000))
    )
    val expandInto = planDescription(Id(9), "Expand(Into)", Seq(argument2), Seq(pipelineInfo(4, fused = true)))
    val limit = planDescription(Id(10), "Limit", Seq(expandInto), Seq(pipelineInfo(4, fused = true)))

    val apply1 =
      planDescription(Id(11), "Apply", Seq(expandAll1, limit), Seq(PageCacheHits(0), PageCacheMisses(0)))

    val apply2 =
      planDescription(Id(12), "Apply", Seq(distinct, apply1), Seq(PageCacheHits(0), PageCacheMisses(0)))

    val expandAll2 = planDescription(Id(13), "Expand(All)", Seq(apply2), Seq(pipelineInfo(4, fused = true)))
    val filter2 = planDescription(Id(14), "Filter", Seq(expandAll2), Seq(pipelineInfo(4, fused = true)))
    val eagerAggregation =
      planDescription(Id(15), "EagerAggregation", Seq(filter2), Seq(pipelineInfo(4, fused = true)))
    val projection = planDescription(
      Id(16),
      "Projection",
      Seq(eagerAggregation),
      Seq(pipelineInfo(5, fused = false), PageCacheHits(29), PageCacheMisses(0), Time(1492000))
    )
    val top = planDescription(
      Id(17),
      "Top",
      Seq(projection),
      Seq(pipelineInfo(6, fused = false), PageCacheHits(0), PageCacheMisses(0), Time(1726000))
    )
    val produceResult = planDescription(
      Id(18),
      "ProduceResults",
      Seq(top),
      Seq(pipelineInfo(6, fused = false), PageCacheHits(0), PageCacheMisses(0), Time(1143000))
    )

    val result = new FusedPlanDescriptionArgumentRewriter().rewrite(produceResult)

    val expectedArguments = Map(
      Id(0) -> nodeUniqueIndexSeek1.arguments,
      Id(1) -> nodeUniqueIndexSeek2.arguments,
      Id(2) -> varLengthExpand.arguments,
      Id(3) -> filter1.arguments,
      Id(4) -> cartesianProduct.arguments,
      Id(5) -> distinct.arguments,
      Id(6) -> argument1.arguments,
      Id(7) -> expandAll1.arguments,
      Id(8) -> argument2.arguments,
      Id(9) -> expandInto.arguments,
      Id(10) -> limit.arguments,
      Id(11) -> Seq.empty[Argument],
      Id(12) -> Seq.empty[Argument],
      Id(13) -> expandAll2.arguments,
      Id(14) -> filter2.arguments,
      Id(15) -> eagerAggregation.arguments,
      Id(16) -> projection.arguments,
      Id(17) -> top.arguments,
      Id(18) -> produceResult.arguments
    )

    planArgumentTest(result, expectedArguments)
  }

  private def planArgumentTest(
    plan: InternalPlanDescription,
    expectedArgumentsByPlanId: Map[Id, Seq[Argument]]
  ): Unit = {
    withClue(s"""Plan "${plan.name}" (${plan.id}) arguments""") {
      plan.arguments should contain theSameElementsAs expectedArgumentsByPlanId.getOrElse(plan.id, Seq.empty)
    }
    plan.children.foreach(childPlan => planArgumentTest(childPlan, expectedArgumentsByPlanId))
  }

  private def pipelineInfo(id: Int, fused: Boolean, markAsSerial: Boolean = false): PipelineInfo =
    PipelineInfo(id, fused, markAsSerial)
}
