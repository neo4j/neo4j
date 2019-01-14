/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{ComparePlansWithAssertion, Configs}

class ShortestPathComplexQueryAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("allShortestPaths with complex LHS should be planned with exhaustive fallback and include predicate") {
    setupModel()
    val result = executeWith(Configs.Interpreted,
      """
        |PROFILE MATCH (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |WITH kim AS kimDeal, collect(charles) AS charlesT, collect(joey) AS joeyS
        |UNWIND charlesT AS charlesThompson
        |UNWIND joeyS AS joeySantiago
        |MATCH pathx = allShortestPaths((charlesThompson)-[*1..5]-(joeySantiago))
        |WHERE none (n IN nodes(pathx) WHERE id(n) = id(kimDeal))
        |RETURN extract(node in nodes(pathx) | id(node)) as ids
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("VarLengthExpand(Into)", "AntiConditionalApply"),
        expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3), expectedDifferentResults = Configs.Cost2_3)

    val results = result.columnAs("ids").toList
    results should be(List(List(0, 4, 3, 2)))
  }

  test("shortestPath with complex LHS should be planned with exhaustive fallback and include predicate") {
    setupModel()
    val result = executeWith(Configs.Interpreted,
      """
        |PROFILE MATCH (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |WITH kim AS kimDeal, collect(charles) AS charlesT, collect(joey) AS joeyS
        |UNWIND charlesT AS charlesThompson
        |UNWIND joeyS AS joeySantiago
        |MATCH pathx = shortestPath((charlesThompson)-[*1..5]-(joeySantiago))
        |WHERE none (n IN nodes(pathx) WHERE id(n) = id(kimDeal))
        |RETURN extract(node in nodes(pathx) | id(node)) as ids
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("VarLengthExpand(Into)", "AntiConditionalApply"),
        expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3), expectedDifferentResults = Configs.Cost2_3)

    val results = result.columnAs("ids").toList
    results should be(List(List(0, 4, 3, 2)))
  }

  private def setupModel(): Unit = {
    graph.execute(
      """
        |MERGE (p1:Pixie {fname:'Charles'})
        |MERGE (p2:Pixie {fname:'Kim'})
        |MERGE (p3:Pixie {fname:'Joey'})
        |MERGE (p4:Pixie {fname:'David'})
        |MERGE (p5:Pixie {fname:'Paz'})
        |MERGE (p1)-[:KNOWS]->(p2)-[:KNOWS]->(p3)-[:KNOWS]->(p4)-[:KNOWS]->(p5)-[:KNOWS]->(p1)
        |RETURN p1,p2,p3,p4,p5
      """.stripMargin)
  }
}
