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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.parser.CypherParser.ExpressionContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.NullType
import org.neo4j.cypher.internal.util.symbols.PathType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TypePredicateExpressionParserTest extends CypherFunSuite with ParserSyntaxTreeBase[Cst.Expression, Expression]
    with AstConstructionTestSupport {

  implicit val javaccRule: JavaccRule[Expression] = JavaccRule.Expression
  implicit val antlrRule: AntlrRule[ExpressionContext] = AntlrRule.Expression

  private val allNonListTypes = Seq(
    ("NOTHING", NothingType()(pos)),
    ("NOTHING NOT NULL", NothingType()(pos)),
    ("NULL", NullType()(pos)),
    ("NULL NOT NULL", NothingType()(pos)),
    ("BOOL", BooleanType(isNullable = true)(pos)),
    ("BOOL NOT NULL", BooleanType(isNullable = false)(pos)),
    ("BOOLEAN", BooleanType(isNullable = true)(pos)),
    ("BOOLEAN NOT NULL", BooleanType(isNullable = false)(pos)),
    ("VARCHAR", StringType(isNullable = true)(pos)),
    ("VARCHAR NOT NULL", StringType(isNullable = false)(pos)),
    ("STRING", StringType(isNullable = true)(pos)),
    ("STRING NOT NULL", StringType(isNullable = false)(pos)),
    ("INTEGER", IntegerType(isNullable = true)(pos)),
    ("INTEGER NOT NULL", IntegerType(isNullable = false)(pos)),
    ("INT", IntegerType(isNullable = true)(pos)),
    ("INT NOT NULL", IntegerType(isNullable = false)(pos)),
    ("SIGNED INTEGER", IntegerType(isNullable = true)(pos)),
    ("SIGNED INTEGER NOT NULL", IntegerType(isNullable = false)(pos)),
    ("FLOAT", FloatType(isNullable = true)(pos)),
    ("FLOAT NOT NULL", FloatType(isNullable = false)(pos)),
    ("DATE", DateType(isNullable = true)(pos)),
    ("DATE NOT NULL", DateType(isNullable = false)(pos)),
    ("LOCAL TIME", LocalTimeType(isNullable = true)(pos)),
    ("LOCAL TIME NOT NULL", LocalTimeType(isNullable = false)(pos)),
    ("TIME WITHOUT TIMEZONE", LocalTimeType(isNullable = true)(pos)),
    ("TIME WITHOUT TIMEZONE NOT NULL", LocalTimeType(isNullable = false)(pos)),
    ("ZONED TIME", ZonedTimeType(isNullable = true)(pos)),
    ("ZONED TIME NOT NULL", ZonedTimeType(isNullable = false)(pos)),
    ("TIME WITH TIMEZONE", ZonedTimeType(isNullable = true)(pos)),
    ("TIME WITH TIMEZONE NOT NULL", ZonedTimeType(isNullable = false)(pos)),
    ("LOCAL DATETIME", LocalDateTimeType(isNullable = true)(pos)),
    ("LOCAL DATETIME NOT NULL", LocalDateTimeType(isNullable = false)(pos)),
    ("TIMESTAMP WITHOUT TIMEZONE", LocalDateTimeType(isNullable = true)(pos)),
    ("TIMESTAMP WITHOUT TIMEZONE NOT NULL", LocalDateTimeType(isNullable = false)(pos)),
    ("ZONED DATETIME", ZonedDateTimeType(isNullable = true)(pos)),
    ("ZONED DATETIME NOT NULL", ZonedDateTimeType(isNullable = false)(pos)),
    ("TIMESTAMP WITH TIMEZONE", ZonedDateTimeType(isNullable = true)(pos)),
    ("TIMESTAMP WITH TIMEZONE NOT NULL", ZonedDateTimeType(isNullable = false)(pos)),
    ("DURATION", DurationType(isNullable = true)(pos)),
    ("DURATION NOT NULL", DurationType(isNullable = false)(pos)),
    ("POINT", PointType(isNullable = true)(pos)),
    ("POINT NOT NULL", PointType(isNullable = false)(pos)),
    ("NODE", NodeType(isNullable = true)(pos)),
    ("NODE NOT NULL", NodeType(isNullable = false)(pos)),
    ("ANY NODE", NodeType(isNullable = true)(pos)),
    ("ANY NODE NOT NULL", NodeType(isNullable = false)(pos)),
    ("VERTEX", NodeType(isNullable = true)(pos)),
    ("VERTEX NOT NULL", NodeType(isNullable = false)(pos)),
    ("ANY VERTEX", NodeType(isNullable = true)(pos)),
    ("ANY VERTEX NOT NULL", NodeType(isNullable = false)(pos)),
    ("RELATIONSHIP", RelationshipType(isNullable = true)(pos)),
    ("RELATIONSHIP NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("ANY RELATIONSHIP", RelationshipType(isNullable = true)(pos)),
    ("ANY RELATIONSHIP NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("EDGE", RelationshipType(isNullable = true)(pos)),
    ("EDGE NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("ANY EDGE", RelationshipType(isNullable = true)(pos)),
    ("ANY EDGE NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("MAP", MapType(isNullable = true)(pos)),
    ("MAP NOT NULL", MapType(isNullable = false)(pos)),
    ("ANY MAP", MapType(isNullable = true)(pos)),
    ("ANY MAP NOT NULL", MapType(isNullable = false)(pos)),
    ("PATH", PathType(isNullable = true)(pos)),
    ("PATH NOT NULL", PathType(isNullable = false)(pos)),
    ("ANY PROPERTY VALUE", PropertyValueType(isNullable = true)(pos)),
    ("ANY PROPERTY VALUE NOT NULL", PropertyValueType(isNullable = false)(pos)),
    ("PROPERTY VALUE", PropertyValueType(isNullable = true)(pos)),
    ("PROPERTY VALUE NOT NULL", PropertyValueType(isNullable = false)(pos))
  )

  private val superTypes = Seq(
    ("ANY VALUE", AnyType(isNullable = true)(pos)),
    ("ANY VALUE NOT NULL", AnyType(isNullable = false)(pos)),
    ("ANY", AnyType(isNullable = true)(pos)),
    ("ANY NOT NULL", AnyType(isNullable = false)(pos))
  )

  private val listTypes =
    (allNonListTypes ++ superTypes).flatMap { case (innerTypeString, innerTypeExpr: CypherType) =>
      Seq(
        // LIST<type>
        (s"LIST<$innerTypeString>", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"LIST<$innerTypeString> NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        (s"ARRAY<$innerTypeString>", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"ARRAY<$innerTypeString> NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        (s"$innerTypeString LIST", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"$innerTypeString LIST NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        (s"$innerTypeString ARRAY", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"$innerTypeString ARRAY NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        // LIST<LIST<type>>
        (
          s"LIST<LIST<$innerTypeString>>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString>> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString> NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString> NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString>>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString>> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString> NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString> NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString> LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString> LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString> NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString> NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString LIST LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString LIST LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString ARRAY LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString ARRAY LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString ARRAY NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString ARRAY NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        // even more nesting lists
        (
          s"LIST<LIST<LIST<LIST<$innerTypeString>> NOT NULL> NOT NULL LIST NOT NULL>",
          ListType(
            ListType(
              ListType(
                ListType(
                  ListType(
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
          ListType(
            ListType(
              ListType(
                ListType(
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
      (s"ANY<$innerDescription>", ClosedDynamicUnionType(allNonListTypes.map(_._2).toSet)(pos)),
      (s"ANY VALUE<$innerDescription>", ClosedDynamicUnionType(allNonListTypes.map(_._2).toSet)(pos)),
      (s"$innerDescription", ClosedDynamicUnionType(allNonListTypes.map(_._2).toSet)(pos))
    )
  }

  private val otherClosedUnionTypes = Seq(
    ("ANY<BOOLEAN | BOOL | BOOLEAN>", BooleanType(isNullable = true)(pos)),
    (
      "ANY<BOOLEAN | INTEGER>",
      ClosedDynamicUnionType(Set(BooleanType(isNullable = true)(pos), IntegerType(isNullable = true)(pos)))(
        pos
      )
    ),
    ("ANY<ANY<ANY<ANY<ANY<BOOL>>>>>", BooleanType(isNullable = true)(pos)),
    (
      "ANY VALUE<FLOAT NOT NULL | STRING>",
      ClosedDynamicUnionType(Set(FloatType(isNullable = false)(pos), StringType(isNullable = true)(pos)))(
        pos
      )
    ),
    (
      "NODE NOT NULL | RELATIONSHIP NOT NULL | PATH NOT NULL",
      ClosedDynamicUnionType(Set(
        NodeType(isNullable = false)(pos),
        RelationshipType(isNullable = false)(pos),
        PathType(isNullable = false)(pos)
      ))(pos)
    ),
    (
      "LIST<BOOLEAN> NOT NULL | LIST<NOTHING | FLOAT> NOT NULL",
      ClosedDynamicUnionType(Set(
        ListType(BooleanType(isNullable = true)(pos), isNullable = false)(pos),
        ListType(
          ClosedDynamicUnionType(Set(NothingType()(InputPosition.NONE), FloatType(isNullable = true)(pos)))(
            pos
          ),
          isNullable = false
        )(pos)
      ))(pos)
    ),
    (
      "LIST<LIST<BOOLEAN NOT NULL> | BOOLEAN> NOT NULL | BOOL",
      ClosedDynamicUnionType(Set(
        BooleanType(isNullable = true)(pos),
        ListType(
          ClosedDynamicUnionType(Set(
            ListType(BooleanType(isNullable = false)(pos), isNullable = true)(pos),
            BooleanType(isNullable = true)(pos)
          ))(pos),
          isNullable = false
        )(pos)
      ))(pos)
    )
  )

  (allNonListTypes ++ superTypes ++ listTypes ++ closedUnionOfAllNonListTypes ++ otherClosedUnionTypes).foreach {
    case (typeString, typeExpr: CypherType) =>
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
