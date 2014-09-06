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
          AllNodesScan("b", Set.empty)(PlannerQuery.empty),
          "b", Direction.INCOMING, Direction.INCOMING, Seq.empty, "a", "r", SimplePatternLength
        )(PlannerQuery.empty),
        Map("r" -> Identifier("r") _)
      )(PlannerQuery.empty)
    )
  }

  test("Should build plans containing expand for two unrelated relationship patterns") {

    (new given {
      cardinality = mapCardinality {
        case AllNodesScan(IdName("a"), _) => 1000
        case AllNodesScan(IdName("b"), _) => 2000
        case AllNodesScan(IdName("c"), _) => 3000
        case AllNodesScan(IdName("d"), _) => 4000
        case _: Expand => 100.0
        case _ => Double.MaxValue
      }
    } planFor "MATCH (a)-[r1]->(b), (c)-[r2]->(d) RETURN r1, r2").plan should beLike {
      case Projection(
            Selection(_,
              CartesianProduct(
                Expand(
                  AllNodesScan(IdName("a"), _), _, _, _, _, _, _, _),
                Expand(
                  AllNodesScan(IdName("c"), _), _, _, _, _, _, _, _)
              )
            ), _) => ()
    }
  }

  test("Should build plans containing expand for self-referencing relationship patterns") {
    planFor("MATCH (a)-[r]->(a) RETURN r").plan should equal(
      Projection(
        Selection(
          predicates = Seq(Equals(Identifier("a") _, Identifier("a$$$") _) _),
          left = Expand(
            AllNodesScan("a", Set.empty)(PlannerQuery.empty),
            "a", Direction.OUTGOING, Direction.OUTGOING, Seq.empty, "a$$$", "r", SimplePatternLength)(PlannerQuery.empty)
        )(PlannerQuery.empty),
        Map("r" -> Identifier("r") _)
      )(PlannerQuery.empty)
    )
  }

  test("Should build plans containing expand for looping relationship patterns") {
    planFor("MATCH (a)-[r1]->(b)<-[r2]-(a) RETURN r1, r2").plan should equal(
      Projection(
        Selection(
          predicates = Seq(NotEquals(Identifier("r1") _, Identifier("r2") _) _),
          left = Selection(
            Seq(Equals(Identifier("a") _, Identifier("a$$$") _) _),
            Expand(
              Expand(AllNodesScan("b", Set.empty)(PlannerQuery.empty), "b", Direction.INCOMING, Direction.INCOMING, Seq.empty, "a", "r1", SimplePatternLength)(PlannerQuery.empty),
              "b", Direction.INCOMING, Direction.INCOMING, Seq.empty, "a$$$", "r2", SimplePatternLength)(PlannerQuery.empty)
          )(PlannerQuery.empty)
        )(PlannerQuery.empty),
        Map(
          "r1" -> Identifier("r1") _,
          "r2" -> Identifier("r2") _
        )
      )(PlannerQuery.empty)
    )
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {
    (new given {
      cardinality = mapCardinality {
        case _: NodeIndexSeek => 10.0
        case _: AllNodesScan  => 100.04
        case _                => Double.MaxValue
      }
    } planFor "MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a").plan should equal(
      Projection(
        Expand(
          Selection(
            Seq(In(Property(Identifier("a")_, PropertyKeyName("name")_)_, Collection(Seq(StringLiteral("Andres")_))_)_),
            AllNodesScan("a", Set.empty)(PlannerQuery.empty)
          )(PlannerQuery.empty),
          "a", Direction.BOTH, Direction.INCOMING, Seq(RelTypeName("x")_), "start", "rel", SimplePatternLength
        )(PlannerQuery.empty),
        Map("a" -> Identifier("a") _)
      )(PlannerQuery.empty)
    )
  }

  test("Should build plans expanding from the more expensive side if that is requested by using a hint") {
    (new given {
      cardinality = mapCardinality {
        case _: NodeIndexSeek => 1000.0
        case _                => 10.0
      }

      indexOn("Person", "name")
    } planFor "MATCH (a)-[r]->(b) USING INDEX b:Person(name) WHERE b:Person AND b.name = 'Andres' return r").plan should equal(
      Projection(
        Expand(
          NodeIndexSeek("b", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Andres") _)) _), Set.empty)(PlannerQuery.empty),
          "b", Direction.INCOMING, Direction.INCOMING, Seq.empty, "a", "r", SimplePatternLength
        )(PlannerQuery.empty),
        Map("r" -> ident("r"))
      )(PlannerQuery.empty)
    )
  }

  test("Should build plans with leaves for both sides if that is requested by using hints") {
    (new given {
      cardinality = mapCardinality {
        case _: NodeIndexSeek                   => 1000.0
        case x: Expand if x.from == IdName("a") => 100.0
        case x: Expand if x.from == IdName("b") => 200.0
        case _                                  => 10.0
      }
      indexOn("Person", "name")
    } planFor "MATCH (a)-[r]->(b) USING INDEX a:Person(name) USING INDEX b:Person(name) WHERE a:Person AND b:Person AND a.name = 'Jakub' AND b.name = 'Andres' return r").plan should equal(
      Projection(
        NodeHashJoin(
          "b",
          Selection(
            Seq(In(Property(ident("b"), PropertyKeyName("name")_)_, Collection(Seq(StringLiteral("Andres")_))_)_, HasLabels(ident("b"), Seq(LabelName("Person")_))_),
            Expand(
              NodeIndexSeek("a", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Jakub") _)) _), Set.empty)(PlannerQuery.empty),
              "a", Direction.OUTGOING, Direction.OUTGOING, Seq.empty, "b", "r", SimplePatternLength
            )(PlannerQuery.empty)
          )(PlannerQuery.empty),
          NodeIndexSeek("b", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Andres") _)) _), Set.empty)(PlannerQuery.empty)
        )(PlannerQuery.empty),
        Map("r" -> ident("r"))
      )(PlannerQuery.empty)
    )
  }
}
