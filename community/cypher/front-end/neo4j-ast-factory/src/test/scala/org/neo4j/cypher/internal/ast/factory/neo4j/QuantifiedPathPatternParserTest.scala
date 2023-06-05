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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule.Variable
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternAtom
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class QuantifiedPathPatternParserTest extends CypherFunSuite
    with ParserSyntaxTreeBase[Cst.Pattern, PatternPart]
    with AstConstructionTestSupport {

  implicit val javaccRule = JavaccRule.PatternPart
  implicit val antlrRule = AntlrRule.PatternPart

  test("(n)") {
    givesIncludingPositions {
      PatternPart(
        nodePat(name = Some("n"), position = (1, 1, 0))
      )
    }
  }

  test("(((n)))") {
    gives {
      PatternPart(ParenthesizedPath(PatternPart(ParenthesizedPath(PatternPart(
        nodePat(Some("n"))
      ))(pos)))(pos))
    }
  }

  test("((n)-[r]->(m))*") {
    givesIncludingPositions {
      PatternPart(QuantifiedPath(
        PatternPart(
          RelationshipChain(
            nodePat(Some("n"), position = (1, 2, 1)),
            relPat(Some("r"), direction = SemanticDirection.OUTGOING, position = (1, 5, 4)),
            nodePat(Some("m"), position = (1, 11, 10))
          )((1, 2, 1))
        ),
        StarQuantifier()((1, 15, 14)),
        None
      )((1, 1, 0)))
    }
  }

  test("(p = (n)-[r]->(m))*") {
    gives {
      PatternPart(QuantifiedPath(
        NamedPatternPart(
          Variable("p"),
          PatternPart(
            RelationshipChain(
              nodePat(Some("n")),
              relPat(Some("r"), direction = SemanticDirection.OUTGOING),
              nodePat(Some("m"))
            )(pos)
          )
        )(pos),
        StarQuantifier()(pos),
        None
      )(pos))
    }
  }

  test("(a) ((n)-[r]->(m))*") {
    givesIncludingPositions {
      PatternPart(
        PathConcatenation(Seq(
          nodePat(name = Some("a"), position = (1, 1, 0)),
          QuantifiedPath(
            PatternPart(
              RelationshipChain(
                nodePat(Some("n"), position = (1, 6, 5)),
                relPat(Some("r"), direction = SemanticDirection.OUTGOING, position = (1, 9, 8)),
                nodePat(Some("m"), position = (1, 15, 14))
              )((1, 6, 5))
            ),
            StarQuantifier()((1, 19, 18)),
            None
          )((1, 5, 4))
        ))((1, 1, 0))
      )
    }
  }

  test("((n)-[r]->(m))* (b)") {
    givesIncludingPositions {
      PatternPart(
        PathConcatenation(Seq(
          QuantifiedPath(
            PatternPart(
              RelationshipChain(
                nodePat(Some("n"), position = (1, 2, 1)),
                relPat(Some("r"), direction = SemanticDirection.OUTGOING, position = (1, 5, 4)),
                nodePat(Some("m"), position = (1, 11, 10))
              )((1, 2, 1))
            ),
            StarQuantifier()((1, 15, 14)),
            None
          )((1, 1, 0)),
          nodePat(name = Some("b"), position = (1, 17, 16))
        ))((1, 1, 0))
      )
    }
  }

  test(
    """(a) (p = (n)-[r]->(m)){1,3} (b)""".stripMargin
  ) {
    gives {
      PatternPart(
        PathConcatenation(Seq(
          nodePat(name = Some("a")),
          QuantifiedPath(
            NamedPatternPart(
              Variable("p"),
              PatternPart(
                RelationshipChain(
                  nodePat(Some("n")),
                  relPat(Some("r"), direction = SemanticDirection.OUTGOING),
                  nodePat(Some("m"))
                )(pos)
              )
            )(pos),
            IntervalQuantifier(
              Some(UnsignedDecimalIntegerLiteral("1")(pos)),
              Some(UnsignedDecimalIntegerLiteral("3")(pos))
            )(
              pos
            ),
            None
          )(pos),
          nodePat(name = Some("b"))
        ))(pos)
      )
    }
  }

  // we allow arbitrary juxtaposition in the parser and only disallow it in semantic analysis
  test("(a) ((n)-[r]->(m))* (b) (c) ((p)-[q]->(s))+") {
    givesIncludingPositions {
      PatternPart(
        PathConcatenation(Seq(
          nodePat(name = Some("a"), position = (1, 1, 0)),
          QuantifiedPath(
            PatternPart(
              RelationshipChain(
                nodePat(Some("n"), position = (1, 6, 5)),
                relPat(Some("r"), direction = SemanticDirection.OUTGOING, position = (1, 9, 8)),
                nodePat(Some("m"), position = (1, 15, 14))
              )((1, 6, 5))
            ),
            StarQuantifier()((1, 19, 18)),
            None
          )((1, 5, 4)),
          nodePat(name = Some("b"), position = (1, 21, 20)),
          nodePat(name = Some("c"), position = (1, 25, 24)),
          QuantifiedPath(
            PatternPart(
              RelationshipChain(
                nodePat(Some("p"), position = (1, 30, 29)),
                relPat(Some("q"), direction = SemanticDirection.OUTGOING, position = (1, 33, 32)),
                nodePat(Some("s"), position = (1, 39, 38))
              )((1, 30, 29))
            ),
            PlusQuantifier()((1, 43, 42)),
            None
          )((1, 29, 28))
        ))((1, 1, 0))
      )
    }
  }

  test("p= ( (a)-->(b) )") {
    gives {
      NamedPatternPart(
        varFor("p"),
        PatternPart(ParenthesizedPath(PatternPart(RelationshipChain(
          nodePat(Some("a")),
          relPat(),
          nodePat(Some("b"))
        )(pos)))(pos))
      )(pos)
    }
  }

  // We parse this and fail later in semantic checking
  test("(p = (q = (n)-[r]->(m))*)*") {
    gives {
      PatternPart(
        QuantifiedPath(
          NamedPatternPart(
            varFor("p"),
            PatternPart(QuantifiedPath(
              NamedPatternPart(
                varFor("q"),
                PatternPart(RelationshipChain(
                  nodePat(Some("n")),
                  relPat(Some("r")),
                  nodePat(Some("m"))
                )(pos))
              )(pos),
              StarQuantifier()(pos),
              None
            )(pos))
          )(pos),
          StarQuantifier()(pos),
          None
        )(pos)
      )
    }
  }

  // We parse this and fail later in semantic checking
  test("p = (n) (q = (a)-[]->(b))") {
    gives {
      NamedPatternPart(
        varFor("p"),
        PatternPart(PathConcatenation(Seq(
          nodePat(Some("n")),
          ParenthesizedPath(NamedPatternPart(
            varFor("q"),
            PatternPart(RelationshipChain(nodePat(Some("a")), relPat(), nodePat(Some("b")))(pos))
          )(pos))(pos)
        ))(pos))
      )(pos)
    }
  }

  test("((a)-->(b)) ((x)-->(y))*") {
    gives {
      PatternPart(PathConcatenation(Seq(
        ParenthesizedPath(PatternPart(RelationshipChain(nodePat(Some("a")), relPat(), nodePat(Some("b")))(pos)))(pos),
        QuantifiedPath(
          PatternPart(RelationshipChain(nodePat(Some("x")), relPat(), nodePat(Some("y")))(pos)),
          StarQuantifier()(pos),
          None
        )(pos)
      ))(pos))
    }
  }
}

class QuantifiedPathPatternInMatchParserTest extends CypherFunSuite
    with ParserSyntaxTreeBase[Cst.Clause, ast.Clause]
    with AstConstructionTestSupport {

  implicit val javaccRule = JavaccRule.Clause
  implicit val antlrRule = AntlrRule.Clause

  test("MATCH p= ( (a)-->(b) ) WHERE a.prop") {
    gives {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        Pattern(Seq(NamedPatternPart(
          varFor("p"),
          PatternPart(ParenthesizedPath(PatternPart(RelationshipChain(
            nodePat(Some("a")),
            relPat(),
            nodePat(Some("b"))
          )(pos)))(pos))
        )(pos)))(pos),
        hints = Seq.empty,
        where = Some(where(prop("a", "prop")))
      )(pos)
    }
  }

  test("MATCH (a), (b)--(c) ((d)--(e))* (f)") {
    gives {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        Pattern(Seq(
          PatternPart(nodePat(Some("a"))),
          PatternPart(PathConcatenation(Seq(
            RelationshipChain(
              nodePat(Some("b")),
              relPat(direction = BOTH),
              nodePat(Some("c"))
            )(pos),
            QuantifiedPath(
              PatternPart(RelationshipChain(
                nodePat(Some("d")),
                relPat(direction = BOTH),
                nodePat(Some("e"))
              )(pos)),
              StarQuantifier()(pos),
              None
            )(pos),
            nodePat(Some("f"))
          ))(pos))
        ))(pos),
        hints = Seq.empty,
        where = None
      )(pos)
    }
  }

  test("MATCH (a)-->+(b)") {
    gives {
      match_(pathConcatenation(
        nodePat(Some("a")),
        quantifiedPath(relationshipChain(nodePat(), relPat(), nodePat()), plusQuantifier),
        nodePat(Some("b"))
      ))
    }
  }

  test("MATCH (a)<-[r]-*(b)") {
    gives {
      match_(pathConcatenation(
        nodePat(Some("a")),
        quantifiedPath(
          relationshipChain(nodePat(), relPat(Some("r"), direction = SemanticDirection.INCOMING), nodePat()),
          starQuantifier
        ),
        nodePat(Some("b"))
      ))
    }
  }

  test("MATCH (a)-[r]-+(b)") {
    gives {
      match_(pathConcatenation(
        nodePat(Some("a")),
        quantifiedPath(
          relationshipChain(nodePat(), relPat(Some("r"), direction = SemanticDirection.BOTH), nodePat()),
          plusQuantifier
        ),
        nodePat(Some("b"))
      ))
    }
  }

  test("MATCH (n)-[r:!REL WHERE r.prop > 123]->{2,}(m)") {
    gives {
      match_(pathConcatenation(
        nodePat(Some("n")),
        quantifiedPath(
          relationshipChain(
            nodePat(),
            relPat(
              name = Some("r"),
              labelExpression = Some(labelNegation(labelRelTypeLeaf("REL"))),
              predicates = Some(greaterThan(prop("r", "prop"), literal(123)))
            ),
            nodePat()
          ),
          IntervalQuantifier(Some(literalUnsignedInt(2)), None)(pos)
        ),
        nodePat(Some("m"))
      ))
    }
  }

  // pattern expressions are not implemented, yet
  test("MATCH (n) WITH [ p = (n)--(m) ((a)-->(b))+ | p ] as paths") {
    failsToParse
  }

  // pattern expression are not implemented, yet
  test("MATCH (n), (m) WHERE (n) ((a)-->(b))+ (m)") {
    failsToParse
  }

  // node abbreviations are not implemented, yet
  test("MATCH (n)--((a)-->(b))+") {
    failsToParse
  }
}

class QuantifiedPathParserTest extends CypherFunSuite
    with ParserSyntaxTreeBase[Cst.ParenthesizedPath, PatternAtom]
    with AstConstructionTestSupport {
  implicit val javaccRule: JavaccRule[PatternAtom] = JavaccRule.ParenthesizedPath
  implicit val antlrRule: AntlrRule[Cst.ParenthesizedPath] = AntlrRule.ParenthesizedPath

  test("((n)-[r]->(m))*") {
    gives {
      QuantifiedPath(
        PatternPart(
          RelationshipChain(
            nodePat(Some("n")),
            relPat(Some("r"), direction = SemanticDirection.OUTGOING),
            nodePat(Some("m"))
          )(pos)
        ),
        StarQuantifier()(pos),
        None
      )(pos)
    }
  }

  test("(p = (n)-[r]->(m))*") {
    gives {
      QuantifiedPath(
        NamedPatternPart(
          Variable("p"),
          PatternPart(
            RelationshipChain(
              nodePat(Some("n")),
              relPat(Some("r"), direction = SemanticDirection.OUTGOING),
              nodePat(Some("m"))
            )(pos)
          )
        )(pos),
        StarQuantifier()(pos),
        None
      )(pos)
    }
  }

  test("((n)-[r]->(m) WHERE n.prop = m.prop)*") {
    gives {
      QuantifiedPath(
        PatternPart(
          RelationshipChain(
            nodePat(Some("n")),
            relPat(Some("r"), direction = SemanticDirection.OUTGOING),
            nodePat(Some("m"))
          )(pos)
        ),
        StarQuantifier()(pos),
        Some(equals(prop("n", "prop"), prop("m", "prop")))
      )(pos)
    }
  }

  test("((a)-->(b) WHERE a.prop > b.prop)") {
    gives {
      ParenthesizedPath(
        PatternPart(
          RelationshipChain(
            nodePat(Some("a")),
            relPat(direction = SemanticDirection.OUTGOING),
            nodePat(Some("b"))
          )(pos)
        ),
        Some(greaterThan(prop("a", "prop"), prop("b", "prop")))
      )(pos)
    }
  }

  // combining all previous GPM features
  test("((n:A|B)-[r:REL|LER WHERE r.prop > 0]->(m:% WHERE m.prop IS NOT NULL))*") {
    gives {
      QuantifiedPath(
        PatternPart(
          RelationshipChain(
            nodePat(
              name = Some("n"),
              labelExpression = Some(labelDisjunction(labelLeaf("A"), labelLeaf("B")))
            ),
            relPat(
              Some("r"),
              Some(labelDisjunction(labelRelTypeLeaf("REL"), labelRelTypeLeaf("LER"))),
              predicates = Some(greaterThan(prop("r", "prop"), literalInt(0)))
            ),
            nodePat(
              name = Some("m"),
              labelExpression = Some(labelWildcard()),
              predicates = Some(isNotNull(prop("m", "prop")))
            )
          )(pos)
        ),
        StarQuantifier()(pos),
        None
      )(pos)
    }
  }
}

class QuantifiedPathPatternsQuantifierParserTest extends CypherFunSuite
    with ParserSyntaxTreeBase[Cst.Quantifier, GraphPatternQuantifier]
    with AstConstructionTestSupport {
  implicit val javaccParser: JavaccRule[GraphPatternQuantifier] = JavaccRule.Quantifier
  implicit val antlrParser: AntlrRule[Cst.Quantifier] = AntlrRule.Quantifier

  test("+") {
    givesIncludingPositions {
      PlusQuantifier()((1, 1, 0))
    }
  }

  test("*") {
    givesIncludingPositions {
      StarQuantifier()((1, 1, 0))
    }
  }

  test("{0,3}") {
    givesIncludingPositions {
      IntervalQuantifier(Some(literalUnsignedInt(0)), Some(literalUnsignedInt(3)))((1, 1, 0))
    }
  }

  test("{1,}") {
    givesIncludingPositions {
      IntervalQuantifier(Some(literalUnsignedInt(1)), None)((1, 1, 0))
    }
  }

  test("{,3}") {
    givesIncludingPositions {
      IntervalQuantifier(None, Some(literalUnsignedInt(3)))((1, 1, 0))
    }
  }

  test("{,}") {
    givesIncludingPositions {
      IntervalQuantifier(None, None)((1, 1, 0))
    }
  }

  test("{2}") {
    givesIncludingPositions {
      FixedQuantifier(literalUnsignedInt(2))((1, 1, 0))
    }
  }

  test("{1_000, 1_000_000}") {
    givesIncludingPositions {
      IntervalQuantifier(
        Some(UnsignedDecimalIntegerLiteral("1_000")((1, 2, 1))),
        Some(UnsignedDecimalIntegerLiteral("1_000_000")((1, 9, 8)))
      )((1, 1, 0))
    }
  }
}
