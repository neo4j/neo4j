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
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.commands.ManyQueryExpression
import org.neo4j.cypher.internal.compiler.v2_2.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport2, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, PropertyKeyId}
import org.neo4j.graphdb.Direction

class ExpandPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("Should build plans containing expand for single relationship pattern") {
    planFor("MATCH (a)-[r]->(b) RETURN r").plan should equal(
      Projection(
        Expand(
          AllNodesScan("b", Set.empty)(solved),
          "b", Direction.INCOMING, Seq.empty, "a", "r"
        )(solved),
        Map("r" -> Identifier("r") _)
      )(solved)
    )
  }

  test("Should build plans containing expand for two unrelated relationship patterns") {

    (new given {
      cardinality = mapCardinality {
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("a")) => 1000.0
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("b")) => 2000.0
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("c")) => 3000.0
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("d")) => 4000.0
        case _ => 100.0
      }
    } planFor "MATCH (a)-[r1]->(b), (c)-[r2]->(d) RETURN r1, r2").plan should beLike {
      case Projection(
            Selection(_,
              CartesianProduct(
                Expand(
                  AllNodesScan(IdName("c"), _), _, _, _, _, _, _),
                Expand(
                  AllNodesScan(IdName("a"), _), _, _, _, _, _, _)
              )
            ), _) => ()
    }
  }

  test("Should build plans containing expand for self-referencing relationship patterns") {
    val result = planFor("MATCH (a)-[r]->(a) RETURN r").plan

    result should equal(
      Projection(
        Expand(
          AllNodesScan("a", Set.empty)(solved),
          "a", Direction.OUTGOING, Seq.empty, "a", "r", ExpandInto)(solved),
        Map("r" -> Identifier("r") _)
      )(solved)
    )
  }

  test("Should build plans containing expand for looping relationship patterns") {
    (new given {
      cardinality = mapCardinality {
        // all node scans
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 => 1000.0
        case _                                                                   => 1.0
      }

    } planFor "MATCH (a)-[r1]->(b)<-[r2]-(a) RETURN r1, r2").plan should equal(
    Projection(
      Selection(Seq(Not(Equals(Identifier("r1")_,Identifier("r2")_)_)_),
        Expand(
          Expand(
            AllNodesScan(IdName("b"),Set.empty)(solved),
            IdName("b"), Direction.INCOMING, Seq.empty, IdName("a"), IdName("r1"),ExpandAll)(solved),
          IdName("b"), Direction.INCOMING, Seq.empty, IdName("a"), IdName("r2"), ExpandInto)(solved)
        )(solved),
        Map("r1" -> Identifier("r1")_, "r2" -> Identifier("r2")_))(solved)
    )
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {

    def myCardinality(plan: PlannerQuery): Cardinality = Cardinality(plan match {
      case PlannerQuery(queryGraph, _, _) if !queryGraph.selections.isEmpty  => 10
      case _ => 1000
    })

    (new given {
      cardinality = PartialFunction(myCardinality)
    } planFor "MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a").plan should equal(
      Projection(
        Expand(
          Selection(
            Seq(In(Property(Identifier("a")_, PropertyKeyName("name")_)_, Collection(Seq(StringLiteral("Andres")_))_)_),
            AllNodesScan("a", Set.empty)(solved)
          )(solved),
          "a", Direction.BOTH, Seq(RelTypeName("x")_), "start", "rel"
        )(solved),
        Map("a" -> Identifier("a") _)
      )(solved)
    )
  }

  test("Should build plans expanding from the more expensive side if that is requested by using a hint") {
    (new given {
      cardinality = mapCardinality {
        case PlannerQuery(queryGraph, _, _) if queryGraph.selections.predicates.size == 2 => 1000.0
        case _                => 10.0
      }

      indexOn("Person", "name")
    } planFor "MATCH (a)-[r]->(b) USING INDEX b:Person(name) WHERE b:Person AND b.name = 'Andres' return r").plan should equal(
      Projection(
        Expand(
          NodeIndexSeek("b", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Andres") _)) _), Set.empty)(solved),
          "b", Direction.INCOMING, Seq.empty, "a", "r"
        )(solved),
        Map("r" -> ident("r"))
      )(solved)
    )
  }
}
