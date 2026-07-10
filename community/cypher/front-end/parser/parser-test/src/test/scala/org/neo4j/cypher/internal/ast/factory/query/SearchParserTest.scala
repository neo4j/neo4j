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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.Parsers
import org.neo4j.cypher.internal.ast.test.util.Parses
import org.neo4j.cypher.internal.expressions.MatchMode.RepeatableElements
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.reflect.ClassTag

class SearchParserTest extends AstParsingTestBase {

  def parsesValidSearch[T <: ASTNode : ClassTag](expected: T, pos: InputPosition)(implicit p: Parsers[T]): Parses[T] =
    parsesIn[T] {
      // 42001 and 42I67 will be added later in the stack, so cannot be asserted on in parser test
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SEARCH': expected"
        ).withSyntaxErrorContaining(s"(line ${pos.line}, column ${pos.column} (offset: ${pos.offset}))")
          .withGqlStatus(gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            "Invalid input 'SEARCH'",
            fuzzyStatusDescr = true
          ))
      case _ => _.toAst(expected)
    }

  def parsesValidSearchWith42001[T <: ASTNode : ClassTag](expected: T, pos: InputPosition)(implicit
    p: Parsers[T]): Parses[T] =
    parsesIn[T] {
      // 42I67 will be added later in the stack, so cannot be asserted on in parser test
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SEARCH': expected"
        ).withSyntaxErrorContaining(s"(line ${pos.line}, column ${pos.column} (offset: ${pos.offset}))")
          .withSyntaxErrorGqlStatus(gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            "Invalid input 'SEARCH'",
            fuzzyStatusDescr = true
          ))
      case _ => _.toAst(expected)
    }

  def parsesInvalidSearch[T <: ASTNode : ClassTag](
    expectedError: String,
    searchPos: InputPosition,
    errorPos: InputPosition
  )(implicit p: Parsers[T]): Parses[T] =
    parsesIn[T] {
      // 42001 will be added later in the stack, so cannot be asserted on in parser test
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SEARCH': expected"
        ).withSyntaxErrorContaining(
          s"(line ${searchPos.line}, column ${searchPos.column} (offset: ${searchPos.offset}))"
        )
          .withGqlStatus(gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            "Invalid input 'SEARCH'",
            fuzzyStatusDescr = true
          ))
      case _ => _.withSyntaxErrorContaining(expectedError)
          .withSyntaxErrorContaining(s"(line ${errorPos.line}, column ${errorPos.column} (offset: ${errorPos.offset}))")
          .withSyntaxErrorGqlStatus(gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            expectedError,
            fuzzyStatusDescr = true
          ))
    }

  for {
    (variable, pattern, patternAst, searchPos) <-
      Seq(
        ("n", "(n)", nodePat(Some("n")), InputPosition(12, 2, 3)),
        ("r", "()-[r]->()", relationshipChain(nodePat(), relPat(Some("r")), nodePat()), InputPosition(19, 2, 3))
      )
  } yield {

    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
        ),
        searchPos
      )
    }

    // Without score
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX moviePlots
         |    FOR movie.vector
         |    LIMIT 3
         |  )
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, None, literalString("moviePlots"), prop("movie", "vector"), 3L))
        ),
        searchPos
      )
    }

    // With single-stage filtering
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE $variable.prop > 42
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(greaterThan(prop(variable, "prop"), literalInt(42L))))
          ))
        ),
        searchPos
      )
    }

    // With single-stage filtering with wrong variable - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE m.prop <= 42
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(lessThanOrEqual(prop("m", "prop"), literalInt(42L))))
          ))
        ),
        searchPos
      )
    }

    // With single-stage filtering with wrong type of predicate - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE true
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(trueLiteral))
          ))
        ),
        searchPos
      )
    }

    // With single-stage filtering with wrong type of RHS - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE $variable.prop > [1, 2, 3]
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(greaterThan(prop(variable, "prop"), listOfInt(1L, 2L, 3L))))
          ))
        ),
        searchPos
      )
    }

    // With single-stage filtering with dependent RHS - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE $variable.prop1 > $variable.prop2
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(greaterThan(prop(variable, "prop1"), prop(variable, "prop2"))))
          ))
        ),
        searchPos
      )
    }

    // With single-stage filtering with OR, NOT and <> - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE NOT $variable.prop1 < m.prop OR $variable.prop2 <> 'abc'
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(or(
              not(lessThan(prop(variable, "prop1"), prop("m", "prop"))),
              notEquals(prop(variable, "prop2"), literalString("abc"))
            )))
          ))
        ),
        searchPos
      )
    }

    // With single-stage filtering with valid AND predicate
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE $variable.prop1 >= $$value AND $variable.prop2 <= date('2025-11-21')
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(and(
              greaterThanOrEqual(prop(variable, "prop1"), parameter("value", CTAny)),
              lessThanOrEqual(prop(variable, "prop2"), function("date", literalString("2025-11-21")))
            )))
          ))
        ),
        searchPos
      )
    }

    // With single-stage filtering with invalid AND predicate - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE $variable.prop > 7 AND $variable.prop < 5
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(and(
              greaterThan(prop(variable, "prop"), literalInt(7L)),
              lessThan(prop(variable, "prop"), literalInt(5L))
            )))
          ))
        ),
        searchPos
      )
    }

    // Naming clashes - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX $variable
         |    FOR $variable.$variable
         |    LIMIT 5
         |  ) SCORE AS $variable
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some(variable), literalString(variable), prop(variable, variable), 5L))
        ),
        searchPos
      )
    }

    // Index null - this should parse but will fail in semantic checking unless there is an index named null
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX null
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("null"), prop("m", "embedding"), 5L))
        ),
        searchPos
      )
    }

    // Index name as parameter
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX $$param
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search =
            Some(vectorSearch(
              variable,
              Some("score"),
              parameter("param", CTString),
              prop("m", "embedding"),
              5L
            ))
        ),
        searchPos
      )
    }

    // Literal vector as search query
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR vector([1, 2, 3], 3, INTEGER32)
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            vector(listOfInt(1, 2, 3), 3, Integer32Type(isNullable = false)(pos)),
            5L
          ))
        ),
        searchPos
      )
    }

    // Literal integer list as search query
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR [1, 5, 42, 37]
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), listOfInt(1, 5, 42, 37), 5L))
        ),
        searchPos
      )
    }

    // Literal float list as search query
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR [1.5]
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), listOfFloat(1.5), 5L))
        ),
        searchPos
      )
    }

    // Parameter as search query
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR $$vector
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search =
            Some(vectorSearch(variable, Some("score"), literalString("indexName"), parameter("vector", CTAny), 5L))
        ),
        searchPos
      )
    }

    // Literal null as search query
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR null
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), nullLiteral, 5L))
        ),
        searchPos
      )
    }

    // OPTIONAL MATCH
    test(
      s"""OPTIONAL MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        optionalMatch(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
        ),
        InputPosition(searchPos.offset + 9, 2, 3)
      )
    }

    // Zero limit
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 0
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 0L))
        ),
        searchPos
      )
    }

    // Negative limit - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT -1
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), -1L))
        ),
        searchPos
      )
    }

    // Null limit - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT null
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(Search(
            varFor(variable),
            Some(varFor("score")),
            Search.Vector,
            literalString("indexName"),
            prop("m", "embedding"),
            None,
            None,
            None,
            Limit(nullLiteral)(pos)
          )(pos))
        ),
        searchPos
      )
    }

    // Unbound variable - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH x IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch("x", Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
        ),
        searchPos
      )
    }

    // Variable called null - this should parse but will fail in semantic checking unless there is a variable null in scope
    test(
      s"""MATCH $pattern
         |  SEARCH NULL IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch("NULL", Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
        ),
        searchPos
      )
    }

    // Score variable named null
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS null
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(vectorSearch(variable, Some("null"), literalString("indexName"), prop("m", "embedding"), 5L))
        ),
        searchPos
      )
    }

    // MATCH with WHERE followed by SEARCH
    test(
      s"""MATCH $pattern
         |  WHERE $variable.released > 2000
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          where = Some(where(propGreaterThan(variable, "released", 2000))),
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
        ),
        InputPosition(searchPos.offset + 26, 3, searchPos.column)
      )
    }

    // MATCH with WHERE followed by SEARCH and single stage filtering
    test(
      s"""MATCH $pattern
         |  WHERE $variable.released > 2000
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE $variable.imdbRating = 8.5
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          where = Some(where(propGreaterThan(variable, "released", 2000))),
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(equals(prop(variable, "imdbRating"), literalFloat(8.5))))
          ))
        ),
        InputPosition(searchPos.offset + 26, 3, searchPos.column)
      )
    }

    // MATCH with SEARCH followed by WHERE
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |  WHERE $variable.released > 2000
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          where = Some(where(propGreaterThan(variable, "released", 2000))),
          search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
        ),
        searchPos
      )
    }

    // MATCH with SEARCH followed by WHERE and single-stage filtering
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    WHERE $variable.imdbRating > 8 AND $variable.imdbRating < 9
         |    LIMIT 5
         |  ) SCORE AS score
         |  WHERE $variable.released > 2000
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          where = Some(where(propGreaterThan(variable, "released", 2000))),
          search = Some(vectorSearch(
            variable,
            Some("score"),
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            Some(where(and(
              greaterThan(prop(variable, "imdbRating"), literalInt(8L)),
              lessThan(prop(variable, "imdbRating"), literalInt(9L))
            )))
          ))
        ),
        searchPos
      )
    }

    // SEARCH inside EXISTS
    test(
      s"""MATCH (n)
         |  WHERE EXISTS {
         |    MATCH $pattern
         |      SEARCH $variable IN (
         |        VECTOR INDEX indexName
         |        FOR m.embedding
         |        LIMIT 5
         |      ) SCORE AS score
         |  }
         |""".stripMargin
    ) {
      val existsExpression: ExistsExpression = ExistsExpression(
        singleQuery(
          match_(
            patternAst,
            search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
          )
        )
      )(InputPosition(16, 2, 7), None, None)

      parsesValidSearchWith42001[Clause](
        match_(
          nodePat(Some("n")),
          where = Some(where(existsExpression))
        ),
        InputPosition(searchPos.offset + 35, 4, 7)
      )
    }

    // SEARCH inside COUNT
    test(
      s"""MATCH (n)
         |  WHERE COUNT {
         |    MATCH $pattern
         |      SEARCH $variable IN (
         |        VECTOR INDEX indexName
         |        FOR m.embedding
         |        LIMIT 5
         |      ) SCORE AS score
         |  }
         |""".stripMargin
    ) {
      val countExpression: CountExpression = CountExpression(
        singleQuery(
          match_(
            patternAst,
            search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
          )
        )
      )(InputPosition(16, 2, 7), None, None)

      parsesValidSearchWith42001[Clause](
        match_(
          nodePat(Some("n")),
          where = Some(where(countExpression))
        ),
        InputPosition(searchPos.offset + 34, 4, 7)
      )
    }

    // SEARCH inside COLLECT
    test(
      s"""MATCH (n)
         |  WHERE COLLECT {
         |    MATCH $pattern
         |      SEARCH $variable IN (
         |        VECTOR INDEX indexName
         |        FOR m.embedding
         |        LIMIT 5
         |      ) SCORE AS score
         |    RETURN $variable
         |  }
         |""".stripMargin
    ) {
      val collectExpression: CollectExpression = CollectExpression(
        singleQuery(
          match_(
            patternAst,
            search = Some(vectorSearch(variable, Some("score"), literalString("indexName"), prop("m", "embedding"), 5L))
          ),
          return_(variableReturnItem(variable))
        )
      )(InputPosition(16, 2, 7), None, None)

      parsesValidSearchWith42001[Clause](
        match_(
          nodePat(Some("n")),
          where = Some(where(collectExpression))
        ),
        InputPosition(searchPos.offset + 36, 4, 7)
      )
    }

    // No variables after SEARCH
    test(
      s"""MATCH $pattern
         |  SEARCH IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  )
         |""".stripMargin
    ) {
      parsesInvalidSearch[Clause](
        "Invalid input '('",
        searchPos,
        InputPosition(searchPos.offset + 10, 2, searchPos.column + 10)
      )
    }

    // Too many variables after search
    test(
      s"""MATCH $pattern
         |  SEARCH $variable, x IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  )  SCORE AS score
         |""".stripMargin
    ) {
      parsesInvalidSearch[Clause](
        "Invalid input ','",
        searchPos,
        InputPosition(searchPos.offset + 8, 2, searchPos.column + 8)
      )
    }

    // General string expression as index name
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX "movie" + "Plots"
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesInvalidSearch[Clause](
        "Invalid input '\"movie\"'",
        searchPos,
        InputPosition(searchPos.offset + 31, 3, 18)
      )
    }

    // Wrong type of binding variable
    test(
      s"""MATCH $pattern
         |  SEARCH "x" IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesInvalidSearch[Clause](
        "Invalid input '\"x\"'",
        searchPos,
        InputPosition(searchPos.offset + 7, 2, 10)
      )
    }

    // Missing score variable in SCORE AS
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX moviePlots
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE AS
         |""".stripMargin
    ) {
      parsesInvalidSearch[Clause](
        "Invalid input ''",
        searchPos,
        InputPosition(searchPos.offset + 86, 6, 13)
      )
    }

    // Missing AS in SCORE AS
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX moviePlots
         |    FOR m.embedding
         |    LIMIT 5
         |  ) SCORE score
         |""".stripMargin
    ) {
      parsesInvalidSearch[Clause](
        "Invalid input 'score'",
        searchPos,
        InputPosition(searchPos.offset + 84, 6, 11)
      )
    }

    // ---------- Fulltext SEARCH ----------

    // Fulltext with score
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'green witch'
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(
            fulltextSearch(variable, Some("score"), literalString("moviePlots"), literalString("green witch"), 5L)
          )
        ),
        searchPos
      )
    }

    // Fulltext without score
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR m.plot
         |    LIMIT 3
         |  )
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(fulltextSearch(variable, None, literalString("moviePlots"), prop("m", "plot"), 3L))
        ),
        searchPos
      )
    }

    // Fulltext with WITH ANALYZER
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'such as heaven' WITH ANALYZER 'english'
         |    LIMIT 3
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(fulltextSearch(
            variable,
            Some("score"),
            literalString("moviePlots"),
            literalString("such as heaven"),
            3L,
            analyzer = Some(literalString("english"))
          ))
        ),
        searchPos
      )
    }

    // Fulltext with WITH ANALYZER as parameter
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'such as heaven' WITH ANALYZER $$analyzer
         |    LIMIT 3
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(fulltextSearch(
            variable,
            Some("score"),
            literalString("moviePlots"),
            literalString("such as heaven"),
            3L,
            analyzer = Some(parameter("analyzer", CTAny))
          ))
        ),
        searchPos
      )
    }

    // Fulltext with SKIP
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'matrix'
         |    SKIP 2
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(fulltextSearch(
            variable,
            Some("score"),
            literalString("moviePlots"),
            literalString("matrix"),
            5L,
            skipNbr = Some(2L)
          ))
        ),
        searchPos
      )
    }

    // Fulltext with OFFSET
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'matrix'
         |    OFFSET 2
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(Search(
            varFor(variable),
            Some(varFor("score")),
            Search.Fulltext,
            literalString("moviePlots"),
            literalString("matrix"),
            None,
            None,
            Some(offset(2L)),
            limit(5L)
          )(pos))
        ),
        searchPos
      )
    }

    // Fulltext with SKIP 0, parameter embedding and WITH ANALYZER combined
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR $$queryString WITH ANALYZER 'english'
         |    SKIP 0
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(fulltextSearch(
            variable,
            Some("score"),
            literalString("moviePlots"),
            parameter("queryString", CTAny),
            5L,
            analyzer = Some(literalString("english")),
            skipNbr = Some(0L)
          ))
        ),
        searchPos
      )
    }

    // Fulltext with WHERE - will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'matrix'
         |    WHERE $variable.prop > 42
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(search(
            variable,
            Some("score"),
            Search.Fulltext,
            literalString("moviePlots"),
            literalString("matrix"),
            5L,
            where = Some(where(greaterThan(prop(variable, "prop"), literalInt(42L))))
          ))
        ),
        searchPos
      )
    }

    // Fulltext with negative SKIP - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'matrix'
         |    SKIP -1
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(fulltextSearch(
            variable,
            Some("score"),
            literalString("moviePlots"),
            literalString("matrix"),
            5L,
            skipNbr = Some(-1L)
          ))
        ),
        searchPos
      )
    }

    // Fulltext with null SKIP - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    FULLTEXT INDEX moviePlots
         |    FOR 'matrix'
         |    SKIP null
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(Search(
            varFor(variable),
            Some(varFor("score")),
            Search.Fulltext,
            literalString("moviePlots"),
            literalString("matrix"),
            None,
            None,
            Some(Skip(nullLiteral)(pos)),
            limit(5L)
          )(pos))
        ),
        searchPos
      )
    }

    // Vector with WITH ANALYZER - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding WITH ANALYZER 'english'
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(search(
            variable,
            Some("score"),
            Search.Vector,
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            analyzer = Some(literalString("english"))
          ))
        ),
        searchPos
      )
    }

    // Vector with SKIP - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    SKIP 2
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(search(
            variable,
            Some("score"),
            Search.Vector,
            literalString("indexName"),
            prop("m", "embedding"),
            5L,
            skipNbr = Some(2L)
          ))
        ),
        searchPos
      )
    }

    // Vector with OFFSET - this should parse but will fail in semantic checking
    test(
      s"""MATCH $pattern
         |  SEARCH $variable IN (
         |    VECTOR INDEX indexName
         |    FOR m.embedding
         |    OFFSET 2
         |    LIMIT 5
         |  ) SCORE AS score
         |""".stripMargin
    ) {
      parsesValidSearch[Clause](
        match_(
          patternAst,
          search = Some(Search(
            varFor(variable),
            Some(varFor("score")),
            Search.Vector,
            literalString("indexName"),
            prop("m", "embedding"),
            None,
            None,
            Some(offset(2L)),
            limit(5L)
          )(pos))
        ),
        searchPos
      )
    }
  }

  // With label and property predicates on the MATCH node
  test(
    """MATCH (n: Movie {title:'Matrix, The'})
      |  SEARCH n IN (
      |    VECTOR INDEX moviePlots
      |    FOR m.embedding
      |    LIMIT 5
      |  ) SCORE AS score
      |""".stripMargin
  ) {
    parsesValidSearch[Clause](
      match_(
        nodePat(Some("n"), Some(labelLeaf("Movie")), Some(mapOf("title" -> literalString("Matrix, The")))),
        search = Some(vectorSearch("n", Some("score"), literalString("moviePlots"), prop("m", "embedding"), 5L))
      ),
      InputPosition(41, 2, 3)
    )
  }

  // With relationship type and property predicates on the MATCH rel
  test(
    """MATCH ()-[r:REL {prop: 42}]->()
      |  SEARCH r IN (
      |    VECTOR INDEX moviePlots
      |    FOR m.embedding
      |    LIMIT 5
      |  ) SCORE AS score
      |""".stripMargin
  ) {
    parsesValidSearch[Clause](
      match_(
        relationshipChain(
          nodePat(),
          relPat(Some("r"), Some(labelRelTypeLeaf("REL")), properties = Some(mapOf("prop" -> literalInt(42)))),
          nodePat()
        ),
        search = Some(vectorSearch("r", Some("score"), literalString("moviePlots"), prop("m", "embedding"), 5L))
      ),
      InputPosition(34, 2, 3)
    )
  }

  // MATCH with more than one variable binding - this should parse but will fail in semantic checking
  test(
    """MATCH (n)-[r]->()
      |  SEARCH r IN (
      |    VECTOR INDEX moviePlots
      |    FOR m.embedding
      |    LIMIT 5
      |  ) SCORE AS score
      |""".stripMargin
  ) {
    parsesValidSearch[Clause](
      match_(
        relationshipChain(
          nodePat(Some("n")),
          relPat(Some("r")),
          nodePat()
        ),
        search = Some(vectorSearch("r", Some("score"), literalString("moviePlots"), prop("m", "embedding"), 5L))
      ),
      InputPosition(20, 2, 3)
    )
  }

  // MATCH with predicates on other elements than the bound variable - this should parse but will fail in semantic checking
  test(
    """MATCH (n)-->(:Actor)
      |  SEARCH n IN (
      |    VECTOR INDEX moviePlots
      |    FOR m.embedding
      |    LIMIT 5
      |  ) SCORE AS score
      |""".stripMargin
  ) {
    parsesValidSearch[Clause](
      match_(
        relationshipChain(
          nodePat(Some("n")),
          relPat(),
          nodePat(None, Some(labelLeaf("Actor")))
        ),
        search = Some(vectorSearch("n", Some("score"), literalString("moviePlots"), prop("m", "embedding"), 5L))
      ),
      InputPosition(23, 2, 3)
    )
  }

  // MATCH wih match mode and path pattern prefix
  test(
    """MATCH REPEATABLE ELEMENTS ANY SHORTEST (n)-->()
      |  SEARCH n IN (
      |    VECTOR INDEX moviePlots
      |    FOR m.embedding
      |    LIMIT 5
      |  ) SCORE AS score
      |""".stripMargin
  ) {
    parsesValidSearch[Clause](
      Match(
        optional = false,
        matchMode = RepeatableElements()(pos),
        Pattern.ForMatch(Seq(
          PrefixedPatternPart(
            selector = anyShortestPathSelector(1),
            part = PathPatternPart(relationshipChain(
              nodePat(Some("n")),
              relPat(),
              nodePat()
            ))
          )
        ))(pos),
        where = None,
        search = Some(vectorSearch("n", Some("score"), literalString("moviePlots"), prop("m", "embedding"), 5L)),
        hints = Seq.empty
      )(pos),
      InputPosition(50, 2, 3)
    )
  }
}
