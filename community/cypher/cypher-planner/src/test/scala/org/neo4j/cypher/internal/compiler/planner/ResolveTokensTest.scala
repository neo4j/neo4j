/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ResolveTokensTest extends CypherFunSuite {


  parseTest("match (n) where n.name = 'Resolved' return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(12))

    ResolveTokens.resolve(query)(semanticTable, planContext)

    query match {
      case Query(
        SingleQuery(Seq(
          Match(
            false,
            Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), None, None, None)))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Resolved"))))
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _)
        ))) =>
            pkToken.name should equal("name")
            semanticTable.id(pkToken) should equal(Some(PropertyKeyId(12)))
    }
  }

  parseTest("match (n) where n.name = 'Unresolved' return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptPropertyKeyId("name")).thenReturn(None)

    ResolveTokens.resolve(query)(semanticTable, planContext)

    query match {
      case Query(
        SingleQuery(Seq(
          Match(
            false,
            Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), None, None, None)))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Unresolved"))))
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _)
        ))) =>
            pkToken.name should equal("name")
            semanticTable.id(pkToken) should equal(None)
    }
  }

  parseTest("match (n) where n:Resolved return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptLabelId("Resolved")).thenReturn(Some(12))

    ResolveTokens.resolve(query)(semanticTable, planContext)

    query match {
      case Query(
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), None, None, None)))),
          Seq(),
          Some(Where(HasLabels(Variable("n"), Seq(labelToken))))
        ),
        Return(false, ReturnItems(true, Seq(), _), None, None, None, _)
      ))) =>
        labelToken.name should equal("Resolved")
        semanticTable.id(labelToken) should equal(Some(LabelId(12)))
    }
  }

  parseTest("match (n) where n:Unresolved return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptLabelId("Unresolved")).thenReturn(None)

    ResolveTokens.resolve(query)(semanticTable, planContext)

    query match {
      case Query(
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(NodePattern(Some(Variable("n")), None, None, None)))),
          Seq(),
          Some(Where(HasLabels(Variable("n"), Seq(labelToken))))
        ),
        Return(false, ReturnItems(true, Seq(), _), None, None, None, _)
      ))) =>
        labelToken.name should equal("Unresolved")
        semanticTable.id(labelToken) should equal(None)
    }
  }

  parseTest("match ()-[:RESOLVED]->() return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptRelTypeId("RESOLVED")).thenReturn(Some(12))

    ResolveTokens.resolve(query)(semanticTable, planContext)

    query match {
      case Query(
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(RelationshipChain(
            NodePattern(None, None, None, None),
            RelationshipPattern(None, Seq(relTypeToken), None, None, None, SemanticDirection.OUTGOING, _),
            NodePattern(None, None, None, None)
          )))),
          Seq(),
          None
        ),
        Return(false, ReturnItems(true, Seq(), _), None, None, None, _)
      ))) =>
        relTypeToken.name should equal("RESOLVED")
        semanticTable.id(relTypeToken) should equal(Some(RelTypeId(12)))
    }
  }

  parseTest("match ()-[:UNRESOLVED]->() return *") { query =>
    implicit val semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptRelTypeId("UNRESOLVED")).thenReturn(None)

    ResolveTokens.resolve(query)(semanticTable, planContext)

    query match {
      case Query(
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(RelationshipChain(
            NodePattern(None, None, None, None),
            RelationshipPattern(None, Seq(relTypeToken), None, None, None, SemanticDirection.OUTGOING, _),
            NodePattern(None, None, None, None)
          )))),
          Seq(),
          None
        ),
        Return(false, ReturnItems(true, Seq(), _), None, None, None, _)
      ))) =>
        relTypeToken.name should equal("UNRESOLVED")
        semanticTable.id(relTypeToken) should equal(None)
    }
  }

  def parseTest(queryText: String)(f: Query => Unit): Unit = test(queryText) {
    val parsed = JavaCCParser.parse(queryText, Neo4jCypherExceptionFactory(queryText, None), new AnonymousVariableNameGenerator)
    val rewriter = LabelExpressionPredicateNormalizer andThen
      normalizeHasLabelsAndHasType(SemanticChecker.check(parsed).state)
    rewriter(parsed) match {
      case query: Query => f(query)
    }
  }
}
