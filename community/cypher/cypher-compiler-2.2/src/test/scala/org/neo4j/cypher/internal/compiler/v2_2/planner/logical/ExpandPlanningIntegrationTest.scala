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
          "b", Direction.INCOMING, Seq.empty, "a", "r"
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
          AllNodesScan("a", Set.empty)(PlannerQuery.empty),
          "a", Direction.OUTGOING, Seq.empty, "a", "r", ExpandInto)(PlannerQuery.empty),
        Map("r" -> Identifier("r") _)
      )(PlannerQuery.empty)
    )
  }

  /* re-enable perty to make it pass */ ignore("Should build plans containing expand for looping relationship patterns") {
    val result: String = planFor("MATCH (a)-[r1]->(b)<-[r2]-(a) RETURN r1, r2").plan.toString
    println(result)
    result should equal(
      """Projection[r1,r2](Map("r1" → r1, "r2" → r2))
        |↳ Selection[a,a$$$,b,r1,r2](Vector(r1 <> r2))
        |↳ Selection[a,a$$$,b,r1,r2](a = a$$$ ⸬ ⬨)
        |↳ Expand[a,a$$$,b,r1,r2](b, INCOMING, INCOMING, ⬨, a$$$, r2, , Vector())
        |↳ Expand[a,b,r1](b, INCOMING, OUTGOING, ⬨, a, r1, , Vector())
        |↳ AllNodesScan[b](b, Set())""".stripMargin
    )
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {

    def myCardinality(plan: LogicalPlan): Cardinality = Cardinality(plan match {
      case _: NodeIndexSeek                 => 10.0
      case _: AllNodesScan                  => 100.04
      case Expand(lhs, _, _, _, _, _, _)    => (myCardinality(lhs) * Multiplier(10)).amount
      case _: Selection                     => 100.04
      case _                                => Double.MaxValue
    })

    (new given {
      cardinality = PartialFunction(myCardinality)
    } planFor "MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a").plan should equal(
      Projection(
        Expand(
          Selection(
            Seq(In(Property(Identifier("a")_, PropertyKeyName("name")_)_, Collection(Seq(StringLiteral("Andres")_))_)_),
            AllNodesScan("a", Set.empty)(PlannerQuery.empty)
          )(PlannerQuery.empty),
          "a", Direction.BOTH, Seq(RelTypeName("x")_), "start", "rel"
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
          "b", Direction.INCOMING, Seq.empty, "a", "r"
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
          Set(IdName("b")),
          Selection(
            Seq(In(Property(ident("b"), PropertyKeyName("name")_)_, Collection(Seq(StringLiteral("Andres")_))_)_, HasLabels(ident("b"), Seq(LabelName("Person")_))_),
            Expand(
              NodeIndexSeek("a", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Jakub") _)) _), Set.empty)(PlannerQuery.empty),
              "a", Direction.OUTGOING, Seq.empty, "b", "r"
            )(PlannerQuery.empty)
          )(PlannerQuery.empty),
          NodeIndexSeek("b", LabelToken("Person", LabelId(0)), PropertyKeyToken("name", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Andres") _)) _), Set.empty)(PlannerQuery.empty)
        )(PlannerQuery.empty),
        Map("r" -> ident("r"))
      )(PlannerQuery.empty)
    )
  }
}
