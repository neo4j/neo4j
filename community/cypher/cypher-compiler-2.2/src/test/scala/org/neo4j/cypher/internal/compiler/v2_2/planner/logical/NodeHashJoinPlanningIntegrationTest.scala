/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands.ManyQueryExpression
import org.neo4j.cypher.internal.compiler.v2_2.{PropertyKeyId, LabelId}
import org.neo4j.cypher.internal.compiler.v2_2.ast.{PropertyKeyToken, LabelToken, LabelName, HasLabels, StringLiteral, Collection, PropertyKeyName, Property, In, Equals, Identifier, Not}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.{PlannerQuery, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.graphdb.Direction

class NodeHashJoinPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing joins") {
    val result= (new given {
      cardinality = mapCardinality {
        // node label scan
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.size == 1 => 100.0
        // all node scan
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.isEmpty => 10000.0
        // expand
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        case _                             => Double.MaxValue
      }

    } planFor "MATCH (a:X)<-[r1]-(b)-[r2]->(c:X) RETURN b").plan

    val expected = Projection(
      Selection(
        Seq(Not(Equals(Identifier("r1")_, Identifier("r2")_)_)_),
        NodeHashJoin(
          Set(IdName("b")),
          Expand(
            NodeByLabelScan(IdName("a"), LazyLabel("X"), Set.empty)(solved),
            IdName("a"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r1"))(solved),
          Expand(
            NodeByLabelScan(IdName("c"), LazyLabel("X"), Set.empty)(solved),
            IdName("c"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r2"))(solved)
        )(solved)
      )(solved),
      Map("b" -> Identifier("b") _))(solved)

    result should equal(expected)
  }

  test("Should build plans with leaves for both sides if that is requested by using hints") {
    (new given {
      cardinality = mapCardinality {
        // node index seek
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.nonEmpty => 1000.0
        // expand from a
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 && queryGraph.patternNodeLabels(IdName("a")).nonEmpty => 100.0
        // expand from b
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 && queryGraph.patternNodeLabels(IdName("b")).nonEmpty => 200.0
        case _                                  => 10.0
      }

      indexOn("Person", "name")
    } planFor "MATCH (a)-[r]->(b) USING INDEX a:Person(name) USING INDEX b:Person(name) WHERE a:Person AND b:Person AND a.name = 'Jakub' AND b.name = 'Andres' return r").plan should equal(
      Projection(
        NodeHashJoin(
          Set(IdName("b")),
          Selection(
            Seq(In(Property(ident("b"), PropertyKeyName("name")_)_, Collection(Seq(StringLiteral("Andres")_))_)_, HasLabels(ident("b"), Seq(LabelName("Person")_))_),
            Expand(
              NodeIndexSeek("a", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Jakub") _)) _), Set.empty)(solved),
              "a", Direction.OUTGOING, Seq.empty, "b", "r"
            )(solved)
          )(solved),
          NodeIndexSeek("b", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Andres") _)) _), Set.empty)(solved)
        )(solved),
        Map("r" -> ident("r"))
      )(solved)
    )
  }
}
