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
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PatternPartWithSelectorParserTest extends CypherFunSuite
    with JavaccParserAstTestBase[ast.Clause]
    with AstConstructionTestSupport {

  implicit val parser: JavaccRule[ast.Clause] = JavaccRule.Clause

  private val selectors = Map(
    "ALL" -> allPathsSelector(),
    "ALL PATH" -> allPathsSelector(),
    "ALL PATHS" -> allPathsSelector(),
    "ANY" -> anyPathSelector("1"),
    "ANY PATH" -> anyPathSelector("1"),
    "ANY PATHS" -> anyPathSelector("1"),
    "ANY 2" -> anyPathSelector("2"),
    "ANY 2 PATH" -> anyPathSelector("2"),
    "ANY 2 PATHS" -> anyPathSelector("2"),
    "ANY SHORTEST" -> anyShortestPathSelector("1"),
    "ANY SHORTEST PATH" -> anyShortestPathSelector("1"),
    "ANY SHORTEST PATHS" -> anyShortestPathSelector("1"),
    "SHORTEST 2" -> anyShortestPathSelector("2"),
    "SHORTEST 2 PATH" -> anyShortestPathSelector("2"),
    "SHORTEST 2 PATHS" -> anyShortestPathSelector("2"),
    "ALL SHORTEST" -> allShortestPathsSelector(),
    "ALL SHORTEST PATH" -> allShortestPathsSelector(),
    "ALL SHORTEST PATHS" -> allShortestPathsSelector(),
    "SHORTEST 2 GROUP" -> shortestGroups("2"),
    "SHORTEST 2 GROUPS" -> shortestGroups("2"),
    "SHORTEST PATH GROUP" -> shortestGroups("1"),
    "SHORTEST PATHS GROUP" -> shortestGroups("1"),
    "SHORTEST PATH GROUPS" -> shortestGroups("1"),
    "SHORTEST PATHS GROUPS" -> shortestGroups("1"),
    "SHORTEST 2 PATH GROUP" -> shortestGroups("2"),
    "SHORTEST 2 PATH GROUPS" -> shortestGroups("2"),
    "SHORTEST 2 PATHS GROUP" -> shortestGroups("2"),
    "SHORTEST 2 PATHS GROUPS" -> shortestGroups("2")
  )

  test("MATCH $selector (a)-[r]->(b)") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"MATCH $selector (a)-[r]->(b)") shouldGive {
          Match(
            optional = false,
            matchMode = MatchMode.default(pos),
            Pattern(Seq(
              PatternPartWithSelector(
                relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                astNode
              )
            ))(pos),
            Seq(),
            None
          )(pos)
        }
      }
    }
  }

  test("MATCH $selector (() ($selector (a)-[r]->(b))* ()-->())") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"MATCH $selector (() ($selector (a)-[r]->(b))* ()-->())") shouldGive {
          Match(
            optional = false,
            matchMode = MatchMode.default(pos),
            Pattern(Seq(
              PatternPartWithSelector(
                ParenthesizedPath(
                  PatternPartWithSelector(
                    PathConcatenation(Seq(
                      nodePat(),
                      QuantifiedPath(
                        PatternPartWithSelector(
                          relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                          astNode
                        ),
                        StarQuantifier()(pos),
                        None
                      )(pos),
                      relationshipChain(nodePat(), relPat(), nodePat())
                    ))(pos),
                    AllPaths()(pos)
                  ),
                  None
                )(pos),
                astNode
              )
            ))(pos),
            Seq(),
            None
          )(pos)
        }
      }
    }
  }

  test("MATCH path = $selector ((a)-[r]->(b) WHERE a.prop = b.prop)") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"MATCH path = $selector ((a)-[r]->(b) WHERE a.prop = b.prop)") shouldGive {
          Match(
            optional = false,
            matchMode = MatchMode.default(pos),
            Pattern(Seq(NamedPatternPart(
              varFor("path"),
              PatternPartWithSelector(
                parenthesizedPath(
                  relationshipChain(
                    nodePat(Some("a")),
                    relPat(Some("r")),
                    nodePat(Some("b"))
                  ),
                  Some(equals(prop("a", "prop"), prop("b", "prop")))
                ),
                astNode
              )
            )(pos)))(pos),
            List(),
            None
          )(pos)
        }
      }
    }
  }

  test("MATCH $selector ((a)-[r]->(b))+") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"MATCH $selector ((a)-[r]->(b))+") shouldGive {
          Match(
            optional = false,
            matchMode = MatchMode.default(pos),
            Pattern(Seq(
              PatternPartWithSelector(
                QuantifiedPath(
                  PatternPartWithSelector(
                    relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                    AllPaths()(pos)
                  ),
                  PlusQuantifier()(pos),
                  None
                )(pos),
                astNode
              )
            ))(pos),
            Seq(),
            None
          )(pos)
        }
      }
    }
  }

  test("OPTIONAL MATCH $selector (a)-[r]->(b)") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"OPTIONAL MATCH $selector (a)-[r]->(b)") shouldGive {
          Match(
            optional = true,
            matchMode = MatchMode.default(pos),
            Pattern(Seq(
              PatternPartWithSelector(
                relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                astNode
              )
            ))(pos),
            Seq(),
            None
          )(pos)
        }
      }
    }
  }

  // A Create should parse, but will fail semantic checking
  test("CREATE $selector (a)-[r]->(b)") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"CREATE $selector (a)-[r]->(b)") shouldGive {
          Create(
            Pattern(Seq(
              PatternPartWithSelector(
                relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                astNode
              )
            ))(pos)
          )(pos)
        }
      }
    }
  }

  // A Merge should parse, but will fail semantic checking
  test("MERGE $selector (a)-[r]->(b)") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"MERGE $selector (a)-[r]->(b)") shouldGive {
          Merge(
            PatternPartWithSelector(
              relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
              astNode
            ),
            Seq(),
            None
          )(pos)
        }
      }
    }
  }

  test("RETURN COUNT { MATCH $selector (a)-[r]->(b) }") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"RETURN COUNT { MATCH $selector (a)-[r]->(b) }") shouldGive {
          return_(
            returnItem(
              CountExpression(
                singleQuery(
                  Match(
                    optional = false,
                    matchMode = MatchMode.default(pos),
                    Pattern(Seq(
                      PatternPartWithSelector(
                        relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                        astNode
                      )
                    ))(pos),
                    Seq(),
                    None
                  )(pos)
                )
              )(pos, None, None),
              s"COUNT { MATCH $selector (a)-[r]->(b) }"
            )
          )
        }
      }
    }
  }

  test("RETURN EXISTS { MATCH $selector (a)-[r]->(b) }") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"RETURN EXISTS { MATCH $selector (a)-[r]->(b) }") shouldGive {
          return_(
            returnItem(
              ExistsExpression(
                singleQuery(
                  Match(
                    optional = false,
                    matchMode = MatchMode.default(pos),
                    Pattern(Seq(
                      PatternPartWithSelector(
                        relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                        astNode
                      )
                    ))(pos),
                    Seq(),
                    None
                  )(pos)
                )
              )(pos, None, None),
              s"EXISTS { MATCH $selector (a)-[r]->(b) }"
            )
          )
        }
      }
    }
  }

  test("CALL { MATCH $selector (a)-[r]->(b) }") {
    selectors.foreach { case selector -> astNode =>
      withClue(s"selector = $selector") {
        parsing(s"CALL { MATCH $selector (a)-[r]->(b) }") shouldGive {
          SubqueryCall(
            singleQuery(
              Match(
                optional = false,
                matchMode = MatchMode.default(pos),
                Pattern(Seq(
                  PatternPartWithSelector(
                    relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
                    astNode
                  )
                ))(pos),
                Seq(),
                None
              )(pos)
            ),
            None
          )(pos)
        }
      }
    }
  }

  // failing queries
  selectors.foreach { selector =>
    withClue(s"selector = ${selector._1}") {
      test(s"FOREACH (x in [ ${selector._1} (a)-->(b) | a ] | SET x.prop = 12 )") {
        failsToParse
      }
    }
  }

  selectors.foreach { selector =>
    withClue(s"selector = ${selector._1}") {
      test(s"RETURN reduce(sum=0, x IN [${selector._1} (a)-[:r]->(b) | b.prop] | sum + x)") {
        failsToParse
      }
    }
  }

  selectors.foreach { selector =>
    withClue(s"selector = ${selector._1}") {
      test(s"MATCH shortestPath(${selector._1} (a)-[r]->(b))") {
        failsToParse
      }
    }
  }

  selectors.foreach { selector =>
    withClue(s"selector = ${selector._1}") {
      test(s"MATCH allShortestPaths(${selector._1} (a)-[r]->(b))") {
        failsToParse
      }
    }
  }

  selectors.foreach { selector =>
    withClue(s"selector = ${selector._1}") {
      test(s"RETURN [ ${selector._1} (a)-->(b) | a ]") {
        failsToParse
      }
    }
  }

  Seq(
    "ANY ALL",
    "ANY -1",
    "ALL 2",
    "ALL PATHS 2",
    "ALL GROUPS",
    "ANY GROUP",
    "ALL SHORTEST GROUPS",
    "ANY SHORTEST GROUP",
    "SHORTEST -2 PATHS",
    "SHORTEST -0 PATHS",
    "SHORTEST 2 GROUP PATHS",
    "SHORTEST -1 GROUP"
  ).foreach { selector =>
    test(s"MATCH $selector (a)-[r]->(b)") {
      failsToParse
    }
  }
}
