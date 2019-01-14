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
import org.neo4j.cypher.internal.InterpretedRuntimeOption
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.Runtime
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

class FallbackAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Need to override so that graph.execute will not throw an exception
  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_hints_error -> "false"
  )

  test("slotted should fall back to interpreted if it encounters a PatternExpression in the plan") {
    val n1 = createLabeledNode("X")
    val n2 = createLabeledNode("X")

    relate(n1, createNode())
    relate(n1, createNode())
    relate(n1, createNode())
    relate(createNode(), n2)
    relate(createNode(), n2)
    relate(createNode(), n2)

    val r = graph.execute(
      "MATCH (n) SET n.friends = size((n)<--())")
    r.getExecutionPlanDescription.getArguments.get(Runtime("").name) should equal(InterpretedRuntimeOption.name.toUpperCase)
  }

  test("slotted should fall back to interpreted if it encounters a PatternComprehension in the plan") {
    val n1 = createLabeledNode("X")
    val n2 = createLabeledNode("X")

    relate(n1, createNode())
    relate(n1, createNode())
    relate(n1, createNode())
    relate(createNode(), n2)
    relate(createNode(), n2)
    relate(createNode(), n2)

    val r = graph.execute(
      "MATCH (n), (m) SET n.friends = size([(n)--(m) | n.foo])")
    r.getExecutionPlanDescription.getArguments.get(Runtime("").name) should equal(InterpretedRuntimeOption.name.toUpperCase)
  }
}
