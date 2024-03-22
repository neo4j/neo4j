/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
import org.neo4j.cypher.internal.compiler.planner.ResolveTokensTest.AllPathsPattern
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ResolveTokensTest extends CypherFunSuite {

  parseTest("match (n) where n.name = 'Resolved' return *") { query =>
    var semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(12))

    semanticTable = ResolveTokens.resolve(query, semanticTable)(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Resolved"))))
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _, _)
        )) =>
        pkToken.name should equal("name")
        semanticTable.id(pkToken) should equal(Some(PropertyKeyId(12)))
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match (n) where n.name = 'Unresolved' return *") { query =>
    var semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptPropertyKeyId("name")).thenReturn(None)

    semanticTable = ResolveTokens.resolve(query, semanticTable)(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Unresolved"))))
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _, _)
        )) =>
        pkToken.name should equal("name")
        semanticTable.id(pkToken) should equal(None)
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match (n) where n:Resolved return *") { query =>
    var semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptLabelId("Resolved")).thenReturn(Some(12))

    semanticTable = ResolveTokens.resolve(query, semanticTable)(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(HasLabels(Variable("n"), Seq(labelToken))))
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _, _)
        )) =>
        labelToken.name should equal("Resolved")
        semanticTable.id(labelToken) should equal(Some(LabelId(12)))
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match (n) where n:Unresolved return *") { query =>
    var semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptLabelId("Unresolved")).thenReturn(None)

    semanticTable = ResolveTokens.resolve(query, semanticTable)(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(HasLabels(Variable("n"), Seq(labelToken))))
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _, _)
        )) =>
        labelToken.name should equal("Unresolved")
        semanticTable.id(labelToken) should equal(None)
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match ()-[:RESOLVED]->() return *") { query =>
    var semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptRelTypeId("RESOLVED")).thenReturn(Some(12))

    semanticTable = ResolveTokens.resolve(query, semanticTable)(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(
              RelationshipChain(
                NodePattern(None, None, None, None),
                RelationshipPattern(
                  None,
                  Some(Leaf(relTypeToken: RelTypeName, _)),
                  None,
                  None,
                  None,
                  SemanticDirection.OUTGOING
                ),
                NodePattern(None, None, None, None)
              )
            )),
            Seq(),
            None
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _, _)
        )) =>
        relTypeToken.name should equal("RESOLVED")
        semanticTable.id(relTypeToken) should equal(Some(RelTypeId(12)))
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match ()-[:UNRESOLVED]->() return *") { query =>
    var semanticTable = SemanticTable()
    val planContext = mock[PlanContext]
    when(planContext.getOptRelTypeId("UNRESOLVED")).thenReturn(None)

    semanticTable = ResolveTokens.resolve(query, semanticTable)(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(
              RelationshipChain(
                NodePattern(None, None, None, None),
                RelationshipPattern(
                  None,
                  Some(Leaf(relTypeToken: RelTypeName, _)),
                  None,
                  None,
                  None,
                  SemanticDirection.OUTGOING
                ),
                NodePattern(None, None, None, None)
              )
            )),
            Seq(),
            None
          ),
          Return(false, ReturnItems(true, Seq(), _), None, None, None, _, _)
        )) =>
        relTypeToken.name should equal("UNRESOLVED")
        semanticTable.id(relTypeToken) should equal(None)
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  def parseTest(queryText: String)(f: Query => Unit): Unit = test(queryText) {
    val parsed =
      JavaCCParser.parse(queryText, Neo4jCypherExceptionFactory(queryText, None))
    val rewriter = LabelExpressionPredicateNormalizer.instance andThen
      normalizeHasLabelsAndHasType(SemanticChecker.check(parsed).state)
    rewriter(parsed) match {
      case query: Query => f(query)
      case other        => throw new IllegalArgumentException(s"Unexpected value: $other")
    }
  }
}

object ResolveTokensTest {

  object AllPathsPattern {

    def unapply(pattern: Pattern.ForMatch): Option[NonPrefixedPatternPart] = {
      pattern match {
        case Pattern.ForMatch(Seq(PatternPartWithSelector(PatternPart.AllPaths(), part))) => Some(part)
        case _                                                                            => None
      }
    }
  }
}
