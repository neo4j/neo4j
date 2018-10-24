/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual._

class PrePopulateResultsAcceptanceTest extends ExecutionEngineFunSuite {

  test("should not populate node unless asked to") {
    val n1 = createNode()

    val query = "MATCH (n) RETURN n"

    assertOnOnlyReturnValue(query, false,
      {
        case n: NodeReference => // ok
        case n: NodeProxyWrappingNodeValue => if (n.isPopulated) fail("Node proxy is populated")
        case n: NodeValue => fail("did not expect populated node value")
      }
    )
  }

  test("should not populate relationship unless asked to") {
    val n1 = createNode()
    relate(n1, createNode())

    val query = "MATCH ()-[r]-() RETURN r"

    assertOnOnlyReturnValue(query, false,
      {
        case r: RelationshipReference => // ok
        case r: RelationshipProxyWrappingValue => if (r.isPopulated) fail("Relationship proxy is populated")
        case r: RelationshipValue => fail("did not expect populated relationship value")
      }
    )
  }

  test("should populate node if asked to") {
    val n1 = createNode()

    val query = "MATCH (n) RETURN n"

    assertOnOnlyReturnValue(query, true, assertPopulatedNode)
  }

  test("should populate relationship if asked to") {
    val n1 = createNode()
    relate(n1, createNode())

    val query = "MATCH ()-[r]-() RETURN r"

    assertOnOnlyReturnValue(query, true, assertPopulatedRelationship)
  }

  test("should populate paths if asked to") {
    val n1 = createNode()
    relate(n1, createNode())

    val query = "MATCH p = ()-[r]-() RETURN p"

    assertOnOnlyReturnValue(query, true,
      value => {
        val path = value.asInstanceOf[PathValue]
        for (n <- path.nodes()) assertPopulatedNode(n)
        for (r <- path.relationships()) assertPopulatedRelationship(r)
      }
    )
  }

  test("should populate inside maps and lists if asked to") {
    val n1 = createNode()

    val query = "MATCH (n) RETURN {prop: [{prop: [n]}]}"

    assertOnOnlyReturnValue(query, true,
      {
        case m1: MapValue =>
          val l1 = m1.get("prop").asInstanceOf[ListValue]
          l1.value(0) match {
            case m2: MapValue =>
              val l2 = m2.get("prop").asInstanceOf[ListValue]
              assertPopulatedNode(l2.value(0))
          }
      }
    )
  }

  private def assertPopulatedNode(value: AnyValue): Unit =
    value match {
      case n: NodeReference => fail("did not expect node reference")
      case n: NodeProxyWrappingNodeValue => if (!n.isPopulated) fail("Node proxy is not populated")
      case n: NodeValue => // ok
    }

  private def assertPopulatedRelationship(value: AnyValue): Unit =
    value match {
      case r: RelationshipReference => fail("did not expect relationship reference")
      case r: RelationshipProxyWrappingValue => if (!r.isPopulated) fail("Relationship proxy is not populated")
      case r: RelationshipValue => // ok
    }

  private def assertOnOnlyReturnValue(query: String, prePopulateResults: Boolean, f: AnyValue => Unit): Unit = {
    val prefixes =
      List(
        // If we wanted to change tx-handling in bolt-server we would need to support 2.3, 3.1 and rule as well.
        // However, if the goal is only to get rid of NodeProxy and RelationshipProxy in the runtime, we can
        // get away with only populating in the current version. As we move to 4.0 there will only be the current
        // runtime anyway.
//        "CYPHER 2.3",
//        "CYPHER 3.1",
//        "CYPHER planner=rule",
        "CYPHER runtime=interpreted",
        "CYPHER runtime=slotted",
        "CYPHER runtime=compiled",
        ""
      )

    for (prefix <- prefixes) {
      withClue(s"[$prefix]: ") {
        val q = prefix + " " + query
        val context = graph.transactionalContext(query = q -> Map.empty)
        try {
          val result = eengine.execute(q, VirtualValues.EMPTY_MAP, context, false, prePopulateResults).asInstanceOf[ExecutionResult]
          result.queryResult().accept(new QueryResultVisitor[Exception] {
            override def visit(row: QueryResult.Record): Boolean = {
              f(row.fields()(0))
              true
            }
          })
        } finally {
          context.close(true)
        }
      }
    }
  }
}
