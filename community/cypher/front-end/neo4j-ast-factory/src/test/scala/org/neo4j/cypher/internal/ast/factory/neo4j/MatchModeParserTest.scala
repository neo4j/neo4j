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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MatchModeParserTest extends CypherFunSuite with JavaccParserAstTestBase[Clause]
    with AstConstructionTestSupport {

  implicit val parser: JavaccRule[Clause] = JavaccRule.MatchClause

  test("MATCH DIFFERENT RELATIONSHIPS (n)-->(m)") {
    gives {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.DifferentRelationships()(pos)
      )
    }
  }

  test("MATCH DIFFERENT RELATIONSHIP BINDINGS (n)-->(m)") {
    gives {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.DifferentRelationships()(pos)
      )
    }
  }

  test("MATCH DIFFERENT RELATIONSHIP (n)-->(m)") {
    gives {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.DifferentRelationships()(pos)
      )
    }
  }

  test("MATCH DIFFERENT RELATIONSHIPS BINDINGS (n)-->(m)") {
    failsToParse
  }

  test("MATCH REPEATABLE ELEMENTS (n)-->(m)") {
    gives {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.RepeatableElements()(pos)
      )
    }
  }

  test("MATCH REPEATABLE ELEMENT BINDINGS (n)-->(m)") {
    gives {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.RepeatableElements()(pos)
      )
    }
  }

  test("MATCH REPEATABLE ELEMENT (n)-->(m)") {
    gives {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.RepeatableElements()(pos)
      )
    }
  }

  test("MATCH REPEATABLE ELEMENTS BINDINGS (n)-->(m)") {
    failsToParse
  }

  test("MATCH REPEATABLE ELEMENT ELEMENTS (n)-->(m)") {
    failsToParse
  }

  test("MATCH () WHERE EXISTS {REPEATABLE ELEMENTS (n)-->(m)}") {
    val pattern = Pattern(Seq(PatternPart(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))))(pos)
    gives {
      match_(
        nodePat(),
        where = Some(where(simpleExistsExpression(pattern, None, matchMode = MatchMode.RepeatableElements()(pos))))
      )
    }
  }

  test("MATCH () WHERE EXISTS {MATCH REPEATABLE ELEMENTS (n)-->(m)}") {
    val pattern = Pattern(Seq(PatternPart(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))))(pos)
    gives {
      match_(
        nodePat(),
        where = Some(where(simpleExistsExpression(pattern, None, matchMode = MatchMode.RepeatableElements()(pos))))
      )
    }
  }

  test("MATCH () WHERE EXISTS {(n)-->(m)}") {
    val pattern = Pattern(Seq(PatternPart(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))))(pos)
    gives {
      match_(
        nodePat(),
        where = Some(where(simpleExistsExpression(
          pattern,
          None,
          matchMode = MatchMode.DifferentRelationships(implicitlyCreated = true)(pos)
        )))
      )
    }
  }

  test("MATCH () WHERE COUNT {REPEATABLE ELEMENTS (n)-->(m)}") {
    val pattern = Pattern(Seq(PatternPart(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))))(pos)
    gives {
      match_(
        nodePat(),
        where = Some(where(simpleCountExpression(pattern, None, matchMode = MatchMode.RepeatableElements()(pos))))
      )
    }
  }

  test("MATCH () WHERE COLLECT {MATCH DIFFERENT RELATIONSHIPS (n)-->(m) RETURN *}") {
    val pattern = Pattern(Seq(PatternPart(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))))(pos)
    gives {
      match_(
        nodePat(),
        where = Some(where(simpleCollectExpression(
          pattern,
          None,
          returnAll,
          matchMode = MatchMode.DifferentRelationships()(pos)
        )))
      )
    }
  }

  test("MATCH () WHERE COLLECT {MATCH REPEATABLE ELEMENTS (n)-->(m) RETURN *}") {
    val pattern = Pattern(Seq(PatternPart(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))))(pos)
    gives {
      match_(
        nodePat(),
        where = Some(where(simpleCollectExpression(
          pattern,
          None,
          returnAll,
          matchMode = MatchMode.RepeatableElements()(pos)
        )))
      )
    }
  }

  test("MATCH REPEATABLE ELEMENTS path = (a)-[r]->(b)") {
    gives {
      Match(
        optional = false,
        matchMode = MatchMode.RepeatableElements()(pos),
        hints = List(),
        where = None,
        pattern = Pattern(Seq(NamedPatternPart(
          varFor("path"),
          PatternPartWithSelector(
            relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
            allPathsSelector()
          )
        )(pos)))(pos)
      )(pos)
    }
  }

  test("MATCH REPEATABLE = (a)-[r]->(b)") {
    gives {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        hints = List(),
        where = None,
        pattern = Pattern(Seq(NamedPatternPart(
          varFor("REPEATABLE"),
          PatternPartWithSelector(
            relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b"))),
            allPathsSelector()
          )
        )(pos)))(pos)
      )(pos)
    }
  }

  test("MATCH DIFFERENT RELATIONSHIPS path = (x)-[p]->(y)") {
    gives {
      Match(
        optional = false,
        matchMode = MatchMode.DifferentRelationships()(pos),
        hints = List(),
        where = None,
        pattern = Pattern(Seq(NamedPatternPart(
          varFor("path"),
          PatternPartWithSelector(
            relationshipChain(nodePat(Some("x")), relPat(Some("p")), nodePat(Some("y"))),
            allPathsSelector()
          )
        )(pos)))(pos)
      )(pos)
    }
  }

  test("MATCH DIFFERENT = (x)-[p]->(y)") {
    gives {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        hints = List(),
        where = None,
        pattern = Pattern(Seq(NamedPatternPart(
          varFor("DIFFERENT"),
          PatternPartWithSelector(
            relationshipChain(nodePat(Some("x")), relPat(Some("p")), nodePat(Some("y"))),
            allPathsSelector()
          )
        )(pos)))(pos)
      )(pos)
    }
  }

  test("MATCH () WHERE COLLECT {REPEATABLE ELEMENTS (n)-->(m) RETURN *}") {
    failsToParse
  }

  test("MATCH REPEATABLE ELEMENT BINDINGS = ()-->()") {
    failsToParse
  }

  test("MATCH DIFFERENT RELATIONSHIP BINDINGS = ()-->()") {
    failsToParse
  }
}
