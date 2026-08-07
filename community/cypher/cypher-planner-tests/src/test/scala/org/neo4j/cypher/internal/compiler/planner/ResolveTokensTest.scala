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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AdditiveProjection
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.ResolveTokensTest.AllPathsPattern
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.planner.spi.NotImplementedPlanContext
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.internal.schema.EndpointType

class ResolveTokensTest extends CypherPlannerTestSuite with AstConstructionTestSupport {

  parseTest("match (n) where n.name = 'Resolved' return *") { query =>
    val planContext = mockPlanContext(propertyIds = Map("name" -> 12))
    val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Resolved")))),
            None
          ),
          Return(false, ReturnItems(AdditiveProjection, Seq(), _), None, None, None, None, _, _, _)
        )) =>
        pkToken.name should equal("name")
        semanticTable.id(pkToken) should equal(Some(PropertyKeyId(12)))
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match (n) where n.name = 'Unresolved' return *") { query =>
    val planContext = mockPlanContext()
    val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(Equals(Property(Variable("n"), pkToken), StringLiteral("Unresolved")))),
            None
          ),
          Return(false, ReturnItems(AdditiveProjection, Seq(), _), None, None, None, None, _, _, _)
        )) =>
        pkToken.name should equal("name")
        semanticTable.id(pkToken) should equal(None)
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match (n) where n:Resolved return *") { query =>
    val planContext = mockPlanContext(labelIds = Map("Resolved" -> 12))
    val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(HasLabels(Variable("n"), Seq(labelToken)))),
            None
          ),
          Return(false, ReturnItems(AdditiveProjection, Seq(), _), None, None, None, None, _, _, _)
        )) =>
        labelToken.name should equal("Resolved")
        semanticTable.id(labelToken) should equal(Some(LabelId(12)))
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match (n) where n:Unresolved return *") { query =>
    val planContext = mockPlanContext()
    val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

    query match {
      case SingleQuery(Seq(
          Match(
            false,
            _,
            AllPathsPattern(PathPatternPart(NodePattern(Some(Variable("n")), None, None, None))),
            Seq(),
            Some(Where(HasLabels(Variable("n"), Seq(labelToken)))),
            None
          ),
          Return(false, ReturnItems(AdditiveProjection, Seq(), _), None, None, None, None, _, _, _)
        )) =>
        labelToken.name should equal("Unresolved")
        semanticTable.id(labelToken) should equal(None)
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match ()-[:RESOLVED]->() return *") { query =>
    val planContext = mockPlanContext(relTypeIds = Map("RESOLVED" -> 12))
    val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

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
            None,
            None
          ),
          Return(false, ReturnItems(AdditiveProjection, Seq(), _), None, None, None, None, _, _, _)
        )) =>
        relTypeToken.name should equal("RESOLVED")
        semanticTable.id(relTypeToken) should equal(Some(RelTypeId(12)))
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  parseTest("match ()-[:UNRESOLVED]->() return *") { query =>
    val planContext = mockPlanContext()
    val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

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
            None,
            None
          ),
          Return(false, ReturnItems(AdditiveProjection, Seq(), _), None, None, None, None, _, _, _)
        )) =>
        relTypeToken.name should equal("UNRESOLVED")
        semanticTable.id(relTypeToken) should equal(None)
      case _ => throw new IllegalArgumentException(s"Unexpected query: $query")
    }
  }

  test("should resolve implied labels") {
    parse("match (n:X) return *") { query =>
      val planContext = mockPlanContext(
        labelIds = Map("X" -> 123, "Y" -> 321, "Z" -> 42),
        nodeLabelConstraints = Map("X" -> Set("Y", "Z"))
      )

      val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

      semanticTable.id(labelName("X")) shouldEqual Some(LabelId(123))
      semanticTable.id(labelName("Y")) shouldEqual Some(LabelId(321))
      semanticTable.id(labelName("Z")) shouldEqual Some(LabelId(42))
    }
  }

  test("should resolve implied labels from relationship endpoint constraints") {
    parse("match (n)-[r:R]->() return *") { query =>
      val planContext = mockPlanContext(
        labelIds = Map("Z" -> 42),
        relTypeIds = Map("R" -> 123),
        relationshipEndpointLabelConstraints = Map("R" -> Map(EndpointType.START -> "Z"))
      )

      val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

      semanticTable.id(relTypeName("R")) shouldEqual Some(RelTypeId(123))
      semanticTable.id(labelName("Z")) shouldEqual Some(LabelId(42))
    }
  }

  test("should resolve chained implied labels from relationship endpoint constraints") {
    parse("match (n)-[r:R]->() return *") { query =>
      val planContext = mockPlanContext(
        labelIds = Map("Z" -> 42, "Y" -> 321),
        relTypeIds = Map("R" -> 123),
        relationshipEndpointLabelConstraints = Map("R" -> Map(EndpointType.START -> "Z")),
        nodeLabelConstraints = Map("Z" -> Set("Y"))
      )

      val semanticTable = ResolveTokens.resolve(query, SemanticTable())(planContext)

      semanticTable.id(relTypeName("R")) shouldEqual Some(RelTypeId(123))
      semanticTable.id(labelName("Z")) shouldEqual Some(LabelId(42))
      semanticTable.id(labelName("Y")) shouldEqual Some(LabelId(321))
    }
  }

  private def parseTest(
    queryText: String,
    versions: Seq[CypherVersion] = CypherVersion.values()
  )(f: Query => Unit): Unit = {
    test(queryText) {
      parse(queryText, versions)(f)
    }
  }

  private def parse(
    queryText: String,
    versions: Seq[CypherVersion] = CypherVersion.values()
  )(f: Query => Unit): Unit = {
    versions.foreach { version =>
      val parsed =
        AstParserFactory(version)(queryText, Neo4jCypherExceptionFactory(queryText, None), None, Seq())
          .singleStatement()
      val semanticContext = SemanticCheckContext(version, NotImplementedErrorMessageProvider)
      val rewriter = LabelExpressionPredicateNormalizer.instance andThen
        NormalizeHasLabelsAndHasType(SemanticChecker.check(parsed, SemanticState.clean, semanticContext).state)
      rewriter(parsed) match {
        case query: Query => withClue(s"Parser: $version\n")(f(query))
        case other        => throw new IllegalArgumentException(s"Unexpected value with $version: $other")
      }
    }
  }

  private def mockPlanContext(
    labelIds: Map[String, Int] = Map.empty,
    propertyIds: Map[String, Int] = Map.empty,
    relTypeIds: Map[String, Int] = Map.empty,
    nodeLabelConstraints: Map[String, Set[String]] = Map.empty,
    relationshipEndpointLabelConstraints: Map[String, Map[EndpointType, String]] = Map.empty
  ): PlanContext = {
    new NotImplementedPlanContext {
      override def getOptLabelId(labelName: String): Option[Int] = labelIds.get(labelName)
      override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = propertyIds.get(propertyKeyName)
      override def getOptRelTypeId(relTypeName: String): Option[Int] = relTypeIds.get(relTypeName)
      override def getNodeLabelConstraints(constrainedLabel: String): Set[String] =
        nodeLabelConstraints.getOrElse(constrainedLabel, Set.empty)
      override def getRelationshipEndpointLabelConstraints(relTypeName: String): Map[EndpointType, String] =
        relationshipEndpointLabelConstraints.getOrElse(relTypeName, Map.empty)
    }
  }
}

object ResolveTokensTest {

  object AllPathsPattern {

    def unapply(pattern: Pattern.ForMatch): Option[NonPrefixedPatternPart] = {
      pattern match {
        case Pattern.ForMatch(Seq(PrefixedPatternPart(PatternPart.AllPaths(), _, part))) => Some(part)
        case _                                                                           => None
      }
    }
  }
}
