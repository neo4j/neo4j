package org.neo4j.cypher.internal.parser

import org.junit.Test
import org.neo4j.cypher.internal.commands._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.expressions.Identifier
import v2_0.{Predicates, MatchClause, Expressions}
import values.LabelName
import org.neo4j.cypher.internal.commands.PatternPredicate
import org.neo4j.cypher.internal.commands.HasLabel

class PredicatesTest extends Predicates with MatchClause with ParserTest with Expressions {

  @Test def pattern_predicates() {
    implicit val parserToTest = patternPredicate

    parsing("a-->(:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo("a", "  UNNAMED5", "  UNNAMED2", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("  UNNAMED5"), Seq(LabelName("Foo"))))

    parsing("a-->(n:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo("a", "n", "  UNNAMED2", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("n"), Seq(LabelName("Foo"))))

    parsing("a-->(:Bar:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo("a", "  UNNAMED5", "  UNNAMED2", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("  UNNAMED5"), Seq(LabelName("Bar"), LabelName("Foo"))))

    val patterns = Seq(
      RelatedTo("a", "  UNNAMED5", "  UNNAMED2", Seq.empty, Direction.OUTGOING, false),
      RelatedTo("  UNNAMED5", "  UNNAMED16", "  UNNAMED13", Seq.empty, Direction.OUTGOING, false))

    val predicate = And(
      HasLabel(Identifier("  UNNAMED5"), Seq(LabelName("First"))),
      HasLabel(Identifier("  UNNAMED16"), Seq(LabelName("Second"))))

    parsing("a-->(:First)-->(:Second)") shouldGive
      PatternPredicate(patterns, predicate)

    val orPred = Or(
      HasLabel(Identifier("  UNNAMED5"), Seq(LabelName("Bar"))),
      HasLabel(Identifier("  UNNAMED5"), Seq(LabelName("Foo")))
    )

    parsing("a-->(:Bar|:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo("a", "  UNNAMED5", "  UNNAMED2", Seq.empty, Direction.OUTGOING, false)), orPred)
  }

  def createProperty(entity: String, propName: String) = ???
}