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
import org.neo4j.cypher.internal.{CompiledRuntimeOption, InterpretedRuntimeOption, SlottedRuntimeOption}
import org.neo4j.graphdb.Result
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

class ExecutionResultAcceptanceTest extends ExecutionEngineFunSuite{

  test("closing the result without exhausting it should not fail the transaction") {
    val query = "UNWIND [1, 2, 3] as x RETURN x"

    Configs.All.scenarios.map(s =>
    s"CYPHER ${s.preparserOptions} $query").foreach(q => {
      val tx = graph.beginTransaction(Type.`explicit`, AUTH_DISABLED)
      val result = eengine.execute(q, Map.empty[String, Object], graph.transactionalContext(query = q -> Map.empty))
      tx.success()
      result.close()
      tx.close()
    })
  }

  test("without PROFILE it shouldn't be needed to iterate over the results before calling getExecutionPlanDescription in compiled runtime") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n"
    val runtime = CompiledRuntimeOption.name

    val description1 = executeQueryAndGetExecutionPlanDescription(query, runtime, false)
    val description2 = executeQueryAndGetExecutionPlanDescription(query, runtime, true)
    description1 should equal(description2)
  }

  test("without PROFILE it shouldn't be needed to iterate over the results before calling getExecutionPlanDescription in interpreted runtime") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n"
    val runtime = InterpretedRuntimeOption.name

    val description1 = executeQueryAndGetExecutionPlanDescription(query, runtime, false)
    val description2 = executeQueryAndGetExecutionPlanDescription(query, runtime, true)
    description1 should equal(description2)
  }

  test("without PROFILE it shouldn't be needed to iterate over the results before calling getExecutionPlanDescription in slotted runtime") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n"
    val runtime = SlottedRuntimeOption.name

    val description1 = executeQueryAndGetExecutionPlanDescription(query, runtime, false)
    val description2 = executeQueryAndGetExecutionPlanDescription(query, runtime, true)
    description1 should equal(description2)
  }

  private def executeQueryAndGetExecutionPlanDescription(query: String, runtime: String, iterateOverResult: Boolean) = {
    val executedQuery = "CYPHER runtime = " + runtime + " " + query
    val result: Result = graph.execute(executedQuery)
    if (iterateOverResult)
      result.hasNext should be (false) // don't really care for the assertion, just consume the results
    result.getExecutionPlanDescription
  }

}
