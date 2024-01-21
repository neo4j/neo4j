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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.PathPatternPart

class MatchModeParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("MATCH DIFFERENT RELATIONSHIPS (n)-->(m)") {
    gives[Clause] {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.DifferentRelationships()(pos)
      )
    }
  }

  test("MATCH DIFFERENT RELATIONSHIP BINDINGS (n)-->(m)") {
    gives[Clause] {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.DifferentRelationships()(pos)
      )
    }
  }

  test("MATCH DIFFERENT RELATIONSHIP (n)-->(m)") {
    gives[Clause] {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.DifferentRelationships()(pos)
      )
    }
  }

  test("MATCH DIFFERENT RELATIONSHIPS BINDINGS (n)-->(m)") {
    failsToParse[Clause]()
  }

  test("MATCH REPEATABLE ELEMENTS (n)-->(m)") {
    gives[Clause] {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.RepeatableElements()(pos)
      )
    }
  }

  test("MATCH REPEATABLE ELEMENT BINDINGS (n)-->(m)") {
    gives[Clause] {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.RepeatableElements()(pos)
      )
    }
  }

  test("MATCH REPEATABLE ELEMENT (n)-->(m)") {
    gives[Clause] {
      match_(
        relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))),
        matchMode = MatchMode.RepeatableElements()(pos)
      )
    }
  }

  test("MATCH REPEATABLE ELEMENTS BINDINGS (n)-->(m)") {
    failsToParse[Clause]()
  }

  test("MATCH REPEATABLE ELEMENT ELEMENTS (n)-->(m)") {
    failsToParse[Clause]()
  }

  test("MATCH () WHERE EXISTS {REPEATABLE ELEMENTS (n)-->(m)}") {
    val pattern = patternForMatch(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))
    gives[Clause] {
      match_(
        nodePat(),
        where = Some(where(simpleExistsExpression(pattern, None, matchMode = MatchMode.RepeatableElements()(pos))))
      )
    }
  }

  test("MATCH () WHERE EXISTS {MATCH REPEATABLE ELEMENTS (n)-->(m)}") {
    val pattern = patternForMatch(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))
    gives[Clause] {
      match_(
        nodePat(),
        where = Some(where(simpleExistsExpression(pattern, None, matchMode = MatchMode.RepeatableElements()(pos))))
      )
    }
  }

  test("MATCH () WHERE EXISTS {(n)-->(m)}") {
    val pattern = patternForMatch(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))
    gives[Clause] {
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
    val pattern = patternForMatch(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))
    gives[Clause] {
      match_(
        nodePat(),
        where = Some(where(simpleCountExpression(pattern, None, matchMode = MatchMode.RepeatableElements()(pos))))
      )
    }
  }

  test("MATCH () WHERE COLLECT {MATCH DIFFERENT RELATIONSHIPS (n)-->(m) RETURN *}") {
    val pattern = patternForMatch(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))
    gives[Clause] {
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
    val pattern = patternForMatch(relationshipChain(nodePat(Some("n")), relPat(), nodePat(Some("m"))))
    gives[Clause] {
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
    gives[Clause] {
      Match(
        optional = false,
        matchMode = MatchMode.RepeatableElements()(pos),
        hints = List(),
        where = None,
        pattern = patternForMatch(NamedPatternPart(
          varFor("path"),
          PathPatternPart(
            relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b")))
          )
        )(pos))
      )(pos)
    }
  }

  test("MATCH REPEATABLE = (a)-[r]->(b)") {
    gives[Clause] {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        hints = List(),
        where = None,
        pattern = patternForMatch(NamedPatternPart(
          varFor("REPEATABLE"),
          PathPatternPart(
            relationshipChain(nodePat(Some("a")), relPat(Some("r")), nodePat(Some("b")))
          )
        )(pos))
      )(pos)
    }
  }

  test("MATCH DIFFERENT RELATIONSHIPS path = (x)-[p]->(y)") {
    gives[Clause] {
      Match(
        optional = false,
        matchMode = MatchMode.DifferentRelationships()(pos),
        hints = List(),
        where = None,
        pattern = patternForMatch(NamedPatternPart(
          varFor("path"),
          PathPatternPart(
            relationshipChain(nodePat(Some("x")), relPat(Some("p")), nodePat(Some("y")))
          )
        )(pos))
      )(pos)
    }
  }

  test("MATCH DIFFERENT = (x)-[p]->(y)") {
    gives[Clause] {
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        hints = List(),
        where = None,
        pattern = patternForMatch(NamedPatternPart(
          varFor("DIFFERENT"),
          PathPatternPart(
            relationshipChain(nodePat(Some("x")), relPat(Some("p")), nodePat(Some("y")))
          )
        )(pos))
      )(pos)
    }
  }

  test("MATCH () WHERE COLLECT {REPEATABLE ELEMENTS (n)-->(m) RETURN *}") {
    failsToParse[Clause]()
  }

  test("MATCH REPEATABLE ELEMENT BINDINGS = ()-->()") {
    failsToParseOnlyJavaCC[Clause]
  }

  test("MATCH DIFFERENT RELATIONSHIP BINDINGS = ()-->()") {
    failsToParseOnlyJavaCC[Clause]
  }
}
