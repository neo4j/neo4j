/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_2._
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Match, Query, SingleQuery, Where, _}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class SimpleTokenResolverTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v3_2.parser.ParserFixture._

  val resolver = new SimpleTokenResolver


  parseTest("match (n) where n.name = 'Resolved' return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(12))

    resolver.resolve(query)(semanticTable, planContext)

    query match {
      case Query(_,
        SingleQuery(Seq(
          Match(
            false,
            Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), Seq(), None)))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Resolved"))))
          ),
          Return(false, ReturnItems(true, Seq()), None, None, None, _)
        ))) =>
            pkToken.name should equal("name")
            pkToken.id should equal(Some(PropertyKeyId(12)))
    }
  }

  parseTest("match (n) where n.name = 'Unresolved' return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptPropertyKeyId("name")).thenReturn(None)

    resolver.resolve(query)(semanticTable, planContext)

    query match {
      case Query(_,
        SingleQuery(Seq(
          Match(
            false,
            Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), Seq(), None)))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Unresolved"))))
          ),
          Return(false, ReturnItems(true, Seq()), None, None, None, _)
        ))) =>
            pkToken.name should equal("name")
            pkToken.id should equal(None)
    }
  }

  parseTest("match (n) where n:Resolved return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptLabelId("Resolved")).thenReturn(Some(12))

    resolver.resolve(query)(semanticTable, planContext)

    query match {
      case Query(_,
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), Seq(), None)))),
          Seq(),
          Some(Where(HasLabels(Variable("n"), Seq(labelToken))))
        ),
        Return(false, ReturnItems(true, Seq()), None, None, None, _)
      ))) =>
        labelToken.name should equal("Resolved")
        labelToken.id should equal(Some(LabelId(12)))
    }
  }

  parseTest("match (n) where n:Unresolved return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptLabelId("Unresolved")).thenReturn(None)

    resolver.resolve(query)(semanticTable, planContext)

    query match {
      case Query(_,
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), Seq(), None)))),
          Seq(),
          Some(Where(HasLabels(Variable("n"), Seq(labelToken))))
        ),
        Return(false, ReturnItems(true, Seq()), None, None, None, _)
      ))) =>
        labelToken.name should equal("Unresolved")
        labelToken.id should equal(None)
    }
  }

  parseTest("match ()-[:RESOLVED]->() return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptRelTypeId("RESOLVED")).thenReturn(Some(12))

    resolver.resolve(query)(semanticTable, planContext)

    query match {
      case Query(_,
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(RelationshipChain(
            NodePattern(None, Seq(), None),
            RelationshipPattern(None, false, Seq(relTypeToken), None, None, SemanticDirection.OUTGOING),
            NodePattern(None, Seq(), None)
          )))),
          Seq(),
          None
        ),
        Return(false, ReturnItems(true, Seq()), None, None, None, _)
      ))) =>
        relTypeToken.name should equal("RESOLVED")
        relTypeToken.id should equal(Some(RelTypeId(12)))
    }
  }

  parseTest("match ()-[:UNRESOLVED]->() return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptRelTypeId("UNRESOLVED")).thenReturn(None)

    resolver.resolve(query)(semanticTable, planContext)

    query match {
      case Query(_,
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(RelationshipChain(
            NodePattern(None, Seq(), None),
            RelationshipPattern(None, false, Seq(relTypeToken), None, None, SemanticDirection.OUTGOING),
            NodePattern(None, Seq(), None)
          )))),
          Seq(),
          None
        ),
        Return(false, ReturnItems(true, Seq()), None, None, None, _)
      ))) =>
        relTypeToken.name should equal("UNRESOLVED")
        relTypeToken.id should equal(None)
    }
  }

  def parseTest(queryText: String)(f: Query => Unit) = test(queryText) { parser.parse(queryText) match {
    case query: Query => f(query)
    }
  }
}
