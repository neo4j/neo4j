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

import org.neo4j.cypher.internal.ast.AnyTypeName
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.BooleanTypeName
import org.neo4j.cypher.internal.ast.ClosedDynamicUnionTypeName
import org.neo4j.cypher.internal.ast.CypherTypeName
import org.neo4j.cypher.internal.ast.DateTypeName
import org.neo4j.cypher.internal.ast.DurationTypeName
import org.neo4j.cypher.internal.ast.FloatTypeName
import org.neo4j.cypher.internal.ast.IntegerTypeName
import org.neo4j.cypher.internal.ast.ListTypeName
import org.neo4j.cypher.internal.ast.LocalDateTimeTypeName
import org.neo4j.cypher.internal.ast.LocalTimeTypeName
import org.neo4j.cypher.internal.ast.MapTypeName
import org.neo4j.cypher.internal.ast.NodeTypeName
import org.neo4j.cypher.internal.ast.NothingTypeName
import org.neo4j.cypher.internal.ast.NullTypeName
import org.neo4j.cypher.internal.ast.PathTypeName
import org.neo4j.cypher.internal.ast.PointTypeName
import org.neo4j.cypher.internal.ast.PropertyValueTypeName
import org.neo4j.cypher.internal.ast.RelationshipTypeName
import org.neo4j.cypher.internal.ast.StringTypeName
import org.neo4j.cypher.internal.ast.ZonedDateTimeTypeName
import org.neo4j.cypher.internal.ast.ZonedTimeTypeName
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.parser.CypherParser.ExpressionContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TypePredicateExpressionParserTest extends CypherFunSuite with ParserSyntaxTreeBase[Cst.Expression, Expression]
    with AstConstructionTestSupport {

  implicit val javaccRule: JavaccRule[Expression] = JavaccRule.Expression
  implicit val antlrRule: AntlrRule[ExpressionContext] = AntlrRule.Expression

  private val allNonListTypes = Seq(
    ("NOTHING", NothingTypeName()(pos)),
    ("NOTHING NOT NULL", NothingTypeName()(pos)),
    ("NULL", NullTypeName()(pos)),
    ("NULL NOT NULL", NothingTypeName()(pos)),
    ("BOOL", BooleanTypeName(isNullable = true)(pos)),
    ("BOOL NOT NULL", BooleanTypeName(isNullable = false)(pos)),
    ("BOOLEAN", BooleanTypeName(isNullable = true)(pos)),
    ("BOOLEAN NOT NULL", BooleanTypeName(isNullable = false)(pos)),
    ("VARCHAR", StringTypeName(isNullable = true)(pos)),
    ("VARCHAR NOT NULL", StringTypeName(isNullable = false)(pos)),
    ("STRING", StringTypeName(isNullable = true)(pos)),
    ("STRING NOT NULL", StringTypeName(isNullable = false)(pos)),
    ("INTEGER", IntegerTypeName(isNullable = true)(pos)),
    ("INTEGER NOT NULL", IntegerTypeName(isNullable = false)(pos)),
    ("INT", IntegerTypeName(isNullable = true)(pos)),
    ("INT NOT NULL", IntegerTypeName(isNullable = false)(pos)),
    ("SIGNED INTEGER", IntegerTypeName(isNullable = true)(pos)),
    ("SIGNED INTEGER NOT NULL", IntegerTypeName(isNullable = false)(pos)),
    ("FLOAT", FloatTypeName(isNullable = true)(pos)),
    ("FLOAT NOT NULL", FloatTypeName(isNullable = false)(pos)),
    ("DATE", DateTypeName(isNullable = true)(pos)),
    ("DATE NOT NULL", DateTypeName(isNullable = false)(pos)),
    ("LOCAL TIME", LocalTimeTypeName(isNullable = true)(pos)),
    ("LOCAL TIME NOT NULL", LocalTimeTypeName(isNullable = false)(pos)),
    ("TIME WITHOUT TIMEZONE", LocalTimeTypeName(isNullable = true)(pos)),
    ("TIME WITHOUT TIMEZONE NOT NULL", LocalTimeTypeName(isNullable = false)(pos)),
    ("ZONED TIME", ZonedTimeTypeName(isNullable = true)(pos)),
    ("ZONED TIME NOT NULL", ZonedTimeTypeName(isNullable = false)(pos)),
    ("TIME WITH TIMEZONE", ZonedTimeTypeName(isNullable = true)(pos)),
    ("TIME WITH TIMEZONE NOT NULL", ZonedTimeTypeName(isNullable = false)(pos)),
    ("LOCAL DATETIME", LocalDateTimeTypeName(isNullable = true)(pos)),
    ("LOCAL DATETIME NOT NULL", LocalDateTimeTypeName(isNullable = false)(pos)),
    ("TIMESTAMP WITHOUT TIMEZONE", LocalDateTimeTypeName(isNullable = true)(pos)),
    ("TIMESTAMP WITHOUT TIMEZONE NOT NULL", LocalDateTimeTypeName(isNullable = false)(pos)),
    ("ZONED DATETIME", ZonedDateTimeTypeName(isNullable = true)(pos)),
    ("ZONED DATETIME NOT NULL", ZonedDateTimeTypeName(isNullable = false)(pos)),
    ("TIMESTAMP WITH TIMEZONE", ZonedDateTimeTypeName(isNullable = true)(pos)),
    ("TIMESTAMP WITH TIMEZONE NOT NULL", ZonedDateTimeTypeName(isNullable = false)(pos)),
    ("DURATION", DurationTypeName(isNullable = true)(pos)),
    ("DURATION NOT NULL", DurationTypeName(isNullable = false)(pos)),
    ("POINT", PointTypeName(isNullable = true)(pos)),
    ("POINT NOT NULL", PointTypeName(isNullable = false)(pos)),
    ("NODE", NodeTypeName(isNullable = true)(pos)),
    ("NODE NOT NULL", NodeTypeName(isNullable = false)(pos)),
    ("ANY NODE", NodeTypeName(isNullable = true)(pos)),
    ("ANY NODE NOT NULL", NodeTypeName(isNullable = false)(pos)),
    ("VERTEX", NodeTypeName(isNullable = true)(pos)),
    ("VERTEX NOT NULL", NodeTypeName(isNullable = false)(pos)),
    ("ANY VERTEX", NodeTypeName(isNullable = true)(pos)),
    ("ANY VERTEX NOT NULL", NodeTypeName(isNullable = false)(pos)),
    ("RELATIONSHIP", RelationshipTypeName(isNullable = true)(pos)),
    ("RELATIONSHIP NOT NULL", RelationshipTypeName(isNullable = false)(pos)),
    ("ANY RELATIONSHIP", RelationshipTypeName(isNullable = true)(pos)),
    ("ANY RELATIONSHIP NOT NULL", RelationshipTypeName(isNullable = false)(pos)),
    ("EDGE", RelationshipTypeName(isNullable = true)(pos)),
    ("EDGE NOT NULL", RelationshipTypeName(isNullable = false)(pos)),
    ("ANY EDGE", RelationshipTypeName(isNullable = true)(pos)),
    ("ANY EDGE NOT NULL", RelationshipTypeName(isNullable = false)(pos)),
    ("MAP", MapTypeName(isNullable = true)(pos)),
    ("MAP NOT NULL", MapTypeName(isNullable = false)(pos)),
    ("ANY MAP", MapTypeName(isNullable = true)(pos)),
    ("ANY MAP NOT NULL", MapTypeName(isNullable = false)(pos)),
    ("PATH", PathTypeName(isNullable = true)(pos)),
    ("PATH NOT NULL", PathTypeName(isNullable = false)(pos)),
    ("ANY PROPERTY VALUE", PropertyValueTypeName(isNullable = true)(pos)),
    ("ANY PROPERTY VALUE NOT NULL", PropertyValueTypeName(isNullable = false)(pos)),
    ("PROPERTY VALUE", PropertyValueTypeName(isNullable = true)(pos)),
    ("PROPERTY VALUE NOT NULL", PropertyValueTypeName(isNullable = false)(pos))
  )

  private val superTypes = Seq(
    ("ANY VALUE", AnyTypeName(isNullable = true)(pos)),
    ("ANY VALUE NOT NULL", AnyTypeName(isNullable = false)(pos)),
    ("ANY", AnyTypeName(isNullable = true)(pos)),
    ("ANY NOT NULL", AnyTypeName(isNullable = false)(pos))
  )

  private val listTypes =
    (allNonListTypes ++ superTypes).flatMap { case (innerTypeString, innerTypeExpr: CypherTypeName) =>
      Seq(
        // LIST<type>
        (s"LIST<$innerTypeString>", ListTypeName(innerTypeExpr, isNullable = true)(pos)),
        (s"LIST<$innerTypeString> NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)(pos)),
        (s"ARRAY<$innerTypeString>", ListTypeName(innerTypeExpr, isNullable = true)(pos)),
        (s"ARRAY<$innerTypeString> NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)(pos)),
        (s"$innerTypeString LIST", ListTypeName(innerTypeExpr, isNullable = true)(pos)),
        (s"$innerTypeString LIST NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)(pos)),
        (s"$innerTypeString ARRAY", ListTypeName(innerTypeExpr, isNullable = true)(pos)),
        (s"$innerTypeString ARRAY NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)(pos)),
        // LIST<LIST<type>>
        (
          s"LIST<LIST<$innerTypeString>>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString>> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString> NOT NULL>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString> NOT NULL> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString>>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString>> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString> NOT NULL>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString> NOT NULL> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST NOT NULL>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST NOT NULL> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY NOT NULL>",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY NOT NULL> NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString> LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString> LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString> NOT NULL LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString> NOT NULL LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> NOT NULL LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> NOT NULL LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString LIST LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString LIST LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString ARRAY LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString ARRAY LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString ARRAY NOT NULL LIST",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString ARRAY NOT NULL LIST NOT NULL",
          ListTypeName(ListTypeName(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        // even more nesting lists
        (
          s"LIST<LIST<LIST<LIST<$innerTypeString>> NOT NULL> NOT NULL LIST NOT NULL>",
          ListTypeName(
            ListTypeName(
              ListTypeName(
                ListTypeName(
                  ListTypeName(
                    innerTypeExpr,
                    isNullable = true
                  )(pos),
                  isNullable = false
                )(pos),
                isNullable = false
              )(pos),
              isNullable = false
            )(pos),
            isNullable = true
          )(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST LIST NOT NULL LIST",
          ListTypeName(
            ListTypeName(
              ListTypeName(
                ListTypeName(
                  innerTypeExpr,
                  isNullable = false
                )(pos),
                isNullable = true
              )(pos),
              isNullable = false
            )(pos),
            isNullable = true
          )(pos)
        )
      )
    }

  private val closedUnionOfAllNonListTypes = {
    val innerDescription = allNonListTypes.map(_._1).mkString(" | ")
    Seq(
      (s"ANY<$innerDescription>", ClosedDynamicUnionTypeName(allNonListTypes.map(_._2).toSet)(pos)),
      (s"ANY VALUE<$innerDescription>", ClosedDynamicUnionTypeName(allNonListTypes.map(_._2).toSet)(pos)),
      (s"$innerDescription", ClosedDynamicUnionTypeName(allNonListTypes.map(_._2).toSet)(pos))
    )
  }

  private val otherClosedUnionTypes = Seq(
    ("ANY<BOOLEAN | BOOL | BOOLEAN>", BooleanTypeName(isNullable = true)(pos)),
    (
      "ANY<BOOLEAN | INTEGER>",
      ClosedDynamicUnionTypeName(Set(BooleanTypeName(isNullable = true)(pos), IntegerTypeName(isNullable = true)(pos)))(
        pos
      )
    ),
    ("ANY<ANY<ANY<ANY<ANY<BOOL>>>>>", BooleanTypeName(isNullable = true)(pos)),
    (
      "ANY VALUE<FLOAT NOT NULL | STRING>",
      ClosedDynamicUnionTypeName(Set(FloatTypeName(isNullable = false)(pos), StringTypeName(isNullable = true)(pos)))(
        pos
      )
    ),
    (
      "NODE NOT NULL | RELATIONSHIP NOT NULL | PATH NOT NULL",
      ClosedDynamicUnionTypeName(Set(
        NodeTypeName(isNullable = false)(pos),
        RelationshipTypeName(isNullable = false)(pos),
        PathTypeName(isNullable = false)(pos)
      ))(pos)
    ),
    (
      "LIST<BOOLEAN> NOT NULL | LIST<NOTHING | FLOAT> NOT NULL",
      ClosedDynamicUnionTypeName(Set(
        ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = false)(pos),
        ListTypeName(
          ClosedDynamicUnionTypeName(Set(NothingTypeName()(InputPosition.NONE), FloatTypeName(isNullable = true)(pos)))(
            pos
          ),
          isNullable = false
        )(pos)
      ))(pos)
    ),
    (
      "LIST<LIST<BOOLEAN NOT NULL> | BOOLEAN> NOT NULL | BOOL",
      ClosedDynamicUnionTypeName(Set(
        BooleanTypeName(isNullable = true)(pos),
        ListTypeName(
          ClosedDynamicUnionTypeName(Set(
            ListTypeName(BooleanTypeName(isNullable = false)(pos), isNullable = true)(pos),
            BooleanTypeName(isNullable = true)(pos)
          ))(pos),
          isNullable = false
        )(pos)
      ))(pos)
    )
  )

  (allNonListTypes ++ superTypes ++ listTypes ++ closedUnionOfAllNonListTypes ++ otherClosedUnionTypes).foreach {
    case (typeString, typeExpr: CypherTypeName) =>
      test(s"x IS :: $typeString") {
        gives {
          isTyped(varFor("x"), typeExpr)
        }
      }

      test(s"n.prop IS TYPED $typeString") {
        gives {
          isTyped(prop("n", "prop"), typeExpr)
        }
      }

      test(s"5 :: $typeString") {
        gives {
          isTyped(literalInt(5L), typeExpr)
        }
      }

      test(s"x + y IS NOT :: $typeString") {
        gives {
          isNotTyped(add(varFor("x"), varFor("y")), typeExpr)
        }
      }

      test(s"['a', 'b', 'c'] IS NOT TYPED $typeString") {
        gives {
          isNotTyped(listOfString("a", "b", "c"), typeExpr)
        }
      }

      // This should not be supported according to CIP-87
      test(s"x NOT :: $typeString") {
        failsToParse
      }
  }

  test("x :: BOOLEAN NOT NULL NOT NULL") {
    failsToParse
  }

  test("x :: LIST<BOOLEAN> NOT NULL NOT NULL") {
    failsToParse
  }

  test("x :: BOOLEAN LIST NOT NULL NOT NULL") {
    failsToParse
  }

  // The code that throws these next 2 errors is not inside of Cypher.jj, so the ANTLR parser doesn't know about it
  test("x :: ANY<BOOLEAN> NOT NULL") {
    assertFailsWithMessage(
      testName,
      "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead. (line 1, column 6 (offset: 5))",
      failsOnlyJavaCC = true
    )
  }

  test("x :: ANY VALUE<BOOLEAN> NOT NULL") {
    assertFailsWithMessage(
      testName,
      "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead. (line 1, column 6 (offset: 5))",
      failsOnlyJavaCC = true
    )
  }

  test("x :: ANY VALUE<>") {
    failsToParse
  }

  test("x :: ANY <>") {
    failsToParse
  }

  test("x :: ") {
    failsToParse
  }

  test("x :: ANY VALUE<> NOT NULL") {
    failsToParse
  }

  test("x :: ANY <> NOT NULL") {
    failsToParse
  }

  test("x :: NOT NULL") {
    failsToParse
  }

  test("x :: LIST<>") {
    failsToParse
  }

  test("x :: LIST") {
    failsToParse
  }

  test("x :: ARRAY") {
    failsToParse
  }

  test("x :: ARRAY<>") {
    failsToParse
  }
}
