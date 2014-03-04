package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.parser.ParserFixture
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Where
import org.neo4j.cypher.internal.compiler.v2_1.ast.Match
import org.neo4j.cypher.internal.compiler.v2_1.ast.SingleQuery
import org.neo4j.cypher.internal.compiler.v2_1.ast.Query
import org.neo4j.cypher.internal.compiler.v2_1.{RelTypeId, LabelId, PropertyKeyId, ast}
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.mockito.Mockito
import org.neo4j.graphdb.Direction

class SimpleTokenResolverTest extends CypherFunSuite {

  import ParserFixture._

  val resolver = new SimpleTokenResolver
  
  parseTest("match n where n.name = 'David' return *") { query =>
    val planContext = mock[PlanContext]
    Mockito.when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(12))

    resolver.resolve(query)(planContext) match {
      case Query(_,
        SingleQuery(Seq(
          Match(
            false,
            Pattern(Seq(EveryPath(NodePattern(Some(Identifier("n")), Seq(), None, true)))),
            Seq(),
            Some(Where(Equals(Property(Identifier("n"), pkToken), StringLiteral("David"))))
          ),
          Return(false, ReturnAll(), None, None, None)
        ))) =>
            pkToken.name should equal("name")
            pkToken.id should equal(Some(PropertyKeyId(12)))
    }
  }

  parseTest("match n where n:Person return *") { query =>
    val planContext = mock[PlanContext]
    Mockito.when(planContext.getOptLabelId("Person")).thenReturn(Some(12))

    resolver.resolve(query)(planContext) match {
      case Query(_,
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(NodePattern(Some(Identifier("n")), Seq(), None, true)))),
          Seq(),
          Some(Where(HasLabels(Identifier("n"), Seq(labelToken))))
        ),
        Return(false, ReturnAll(), None, None, None)
      ))) =>
        labelToken.name should equal("Person")
        labelToken.id should equal(Some(LabelId(12)))
    }
  }

  parseTest("match ()-[:KNOWS]->() return *") { query =>
    val planContext = mock[PlanContext]
    Mockito.when(planContext.getOptRelTypeId("KNOWS")).thenReturn(Some(12))

    resolver.resolve(query)(planContext) match {
      case Query(_,
      SingleQuery(Seq(
        Match(
          false,
          Pattern(Seq(EveryPath(RelationshipChain(
            NodePattern(None, Seq(), None, false),
            RelationshipPattern(None, false, Seq(relTypeToken), None, None, Direction.OUTGOING),
            NodePattern(None, Seq(), None, false)
          )))),
          Seq(),
          None
        ),
        Return(false, ReturnAll(), None, None, None)
      ))) =>
        relTypeToken.name should equal("KNOWS")
        relTypeToken.id should equal(Some(RelTypeId(12)))
    }
  }

  def parseTest(queryText: String)(f: Query => Unit) = test(queryText) { parser.parse(queryText) match {
    case query: Query => f(query)
    } 
  }
}
