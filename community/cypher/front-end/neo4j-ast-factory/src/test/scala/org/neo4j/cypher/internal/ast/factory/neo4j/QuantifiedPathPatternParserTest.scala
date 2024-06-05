/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral

class QuantifiedPathPatternParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("(n)") {
    parses[PatternPart].toAstPositioned {
      PathPatternPart(
        nodePat(name = Some("n"), position = (1, 1, 0))
      )
    }
  }

  test("(((n)))") {
    parsesTo[PatternPart] {
      PatternPart(ParenthesizedPath(PatternPart(ParenthesizedPath(PatternPart(
        nodePat(Some("n"))
      ))(pos)))(pos))
    }
  }

  test("((n)-[r]->(m))*") {
    parses[PatternPart].toAstPositioned {
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
    parsesTo[PatternPart] {
      PatternPart(QuantifiedPath(
        NamedPatternPart(
          varFor("p"),
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
    parses[PatternPart].toAstPositioned {
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
    parses[PatternPart].toAstPositioned {
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
    parsesTo[PatternPart] {
      PatternPart(
        PathConcatenation(Seq(
          nodePat(name = Some("a")),
          QuantifiedPath(
            NamedPatternPart(
              varFor("p"),
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
    parses[PatternPart].toAstPositioned {
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
    parsesTo[PatternPart] {
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
    parsesTo[PatternPart] {
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
    parsesTo[PatternPart] {
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
    parsesTo[PatternPart] {
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

class QuantifiedPathPatternInMatchParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("MATCH p= ( (a)-->(b) ) WHERE a.prop") {
    parsesTo[Clause] {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        Pattern.ForMatch(Seq(
          PatternPartWithSelector(
            PatternPart.AllPaths()(pos),
            NamedPatternPart(
              varFor("p"),
              PatternPart(ParenthesizedPath(PatternPart(RelationshipChain(
                nodePat(Some("a")),
                relPat(),
                nodePat(Some("b"))
              )(pos)))(pos))
            )(pos)
          )
        ))(pos),
        hints = Seq.empty,
        where = Some(where(prop("a", "prop")))
      )(pos)
    }
  }

  test("MATCH (a), (b)--(c) ((d)--(e))* (f)") {
    parsesTo[Clause] {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        Pattern.ForMatch(Seq(
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
        ).map(_.withAllPathsSelector))(pos),
        hints = Seq.empty,
        where = None
      )(pos)
    }
  }

  test("MATCH (a)-->+(b)") {
    parsesTo[Clause] {
      match_(pathConcatenation(
        nodePat(Some("a")),
        quantifiedPath(relationshipChain(nodePat(), relPat(), nodePat()), plusQuantifier),
        nodePat(Some("b"))
      ))
    }
  }

  test("MATCH (a)<-[r]-*(b)") {
    parsesTo[Clause] {
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
    parsesTo[Clause] {
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
    parsesTo[Clause] {
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
  test("MATCH (n) WITH [ p = (n)--(m) ((a)-->(b))+ | p ] as paths RETURN *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '(': expected\n  \"!=\"\n  \"%\"\n  \"*\"")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected an expression (line 1, column 31 (offset: 30))
            |"MATCH (n) WITH [ p = (n)--(m) ((a)-->(b))+ | p ] as paths RETURN *"
            |                               ^""".stripMargin
        )
    }
  }

  // pattern expression are not implemented, yet
  test("MATCH (n), (m) WHERE (n) ((a)-->(b))+ (m) RETURN *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '('")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected an expression, 'FOREACH', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 26 (offset: 25))
            |"MATCH (n), (m) WHERE (n) ((a)-->(b))+ (m) RETURN *"
            |                          ^""".stripMargin
        )
    }
  }

  // node abbreviations are not implemented, yet
  test("MATCH (n)--((a)-->(b))+") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '(': expected \":\" or an identifier")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected a parameter, a variable name, ')', ':', 'IS', 'WHERE' or '{' (line 1, column 13 (offset: 12))
            |"MATCH (n)--((a)-->(b))+"
            |             ^""".stripMargin
        )
    }
  }
}

class QuantifiedPathParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("((n)-[r]->(m))*") {
    parsesTo[QuantifiedPath] {
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
    parsesTo[QuantifiedPath] {
      QuantifiedPath(
        NamedPatternPart(
          varFor("p"),
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
    parsesTo[QuantifiedPath] {
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
    parsesTo[ParenthesizedPath] {
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
    parsesTo[QuantifiedPath] {
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

class QuantifiedPathPatternsQuantifierParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("+") {
    parses[GraphPatternQuantifier].toAstPositioned {
      PlusQuantifier()((1, 1, 0))
    }
  }

  test("*") {
    parses[GraphPatternQuantifier].toAstPositioned {
      StarQuantifier()((1, 1, 0))
    }
  }

  test("{0,3}") {
    parses[GraphPatternQuantifier].toAstPositioned {
      IntervalQuantifier(Some(literalUnsignedInt(0)), Some(literalUnsignedInt(3)))((1, 1, 0))
    }
  }

  test("{1,}") {
    parses[GraphPatternQuantifier].toAstPositioned {
      IntervalQuantifier(Some(literalUnsignedInt(1)), None)((1, 1, 0))
    }
  }

  test("{,3}") {
    parses[GraphPatternQuantifier].toAstPositioned {
      IntervalQuantifier(None, Some(literalUnsignedInt(3)))((1, 1, 0))
    }
  }

  test("{,}") {
    parses[GraphPatternQuantifier].toAstPositioned {
      IntervalQuantifier(None, None)((1, 1, 0))
    }
  }

  test("{2}") {
    parses[GraphPatternQuantifier].toAstPositioned {
      FixedQuantifier(literalUnsignedInt(2))((1, 1, 0))
    }
  }

  test("{1_000, 1_000_000}") {
    parses[GraphPatternQuantifier].toAstPositioned {
      IntervalQuantifier(
        Some(UnsignedDecimalIntegerLiteral("1_000")((1, 2, 1))),
        Some(UnsignedDecimalIntegerLiteral("1_000_000")((1, 9, 8)))
      )((1, 1, 0))
    }
  }
}
