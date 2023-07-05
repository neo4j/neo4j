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
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.AnyTypeName
import org.neo4j.cypher.internal.expressions.BooleanTypeName
import org.neo4j.cypher.internal.expressions.CypherTypeName
import org.neo4j.cypher.internal.expressions.DateTypeName
import org.neo4j.cypher.internal.expressions.DurationTypeName
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FloatTypeName
import org.neo4j.cypher.internal.expressions.IntegerTypeName
import org.neo4j.cypher.internal.expressions.ListTypeName
import org.neo4j.cypher.internal.expressions.LocalDateTimeTypeName
import org.neo4j.cypher.internal.expressions.LocalTimeTypeName
import org.neo4j.cypher.internal.expressions.MapTypeName
import org.neo4j.cypher.internal.expressions.NodeTypeName
import org.neo4j.cypher.internal.expressions.NothingTypeName
import org.neo4j.cypher.internal.expressions.NullTypeName
import org.neo4j.cypher.internal.expressions.PathTypeName
import org.neo4j.cypher.internal.expressions.PointTypeName
import org.neo4j.cypher.internal.expressions.PropertyValueTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeName
import org.neo4j.cypher.internal.expressions.StringTypeName
import org.neo4j.cypher.internal.expressions.ZonedDateTimeTypeName
import org.neo4j.cypher.internal.expressions.ZonedTimeTypeName
import org.neo4j.cypher.internal.parser.CypherParser.ExpressionContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TypePredicateExpressionParserTest extends CypherFunSuite with ParserSyntaxTreeBase[Cst.Expression, Expression]
    with AstConstructionTestSupport {

  implicit val javaccRule: JavaccRule[Expression] = JavaccRule.Expression
  implicit val antlrRule: AntlrRule[ExpressionContext] = AntlrRule.Expression

  private val allNonListTypes = Seq(
    ("NOTHING", NothingTypeName()),
    ("NOTHING NOT NULL", NothingTypeName()),
    ("NULL", NullTypeName()),
    ("NULL NOT NULL", NothingTypeName()),
    ("BOOL", BooleanTypeName(true)),
    ("BOOL NOT NULL", BooleanTypeName(false)),
    ("BOOLEAN", BooleanTypeName(true)),
    ("BOOLEAN NOT NULL", BooleanTypeName(false)),
    ("VARCHAR", StringTypeName(true)),
    ("VARCHAR NOT NULL", StringTypeName(false)),
    ("STRING", StringTypeName(true)),
    ("STRING NOT NULL", StringTypeName(false)),
    ("INTEGER", IntegerTypeName(true)),
    ("INTEGER NOT NULL", IntegerTypeName(false)),
    ("INT", IntegerTypeName(true)),
    ("INT NOT NULL", IntegerTypeName(false)),
    ("SIGNED INTEGER", IntegerTypeName(true)),
    ("SIGNED INTEGER NOT NULL", IntegerTypeName(false)),
    ("FLOAT", FloatTypeName(true)),
    ("FLOAT NOT NULL", FloatTypeName(false)),
    ("DATE", DateTypeName(true)),
    ("DATE NOT NULL", DateTypeName(false)),
    ("LOCAL TIME", LocalTimeTypeName(true)),
    ("LOCAL TIME NOT NULL", LocalTimeTypeName(false)),
    ("TIME WITHOUT TIMEZONE", LocalTimeTypeName(true)),
    ("TIME WITHOUT TIMEZONE NOT NULL", LocalTimeTypeName(false)),
    ("ZONED TIME", ZonedTimeTypeName(true)),
    ("ZONED TIME NOT NULL", ZonedTimeTypeName(false)),
    ("TIME WITH TIMEZONE", ZonedTimeTypeName(true)),
    ("TIME WITH TIMEZONE NOT NULL", ZonedTimeTypeName(false)),
    ("LOCAL DATETIME", LocalDateTimeTypeName(true)),
    ("LOCAL DATETIME NOT NULL", LocalDateTimeTypeName(false)),
    ("TIMESTAMP WITHOUT TIMEZONE", LocalDateTimeTypeName(true)),
    ("TIMESTAMP WITHOUT TIMEZONE NOT NULL", LocalDateTimeTypeName(false)),
    ("ZONED DATETIME", ZonedDateTimeTypeName(true)),
    ("ZONED DATETIME NOT NULL", ZonedDateTimeTypeName(false)),
    ("TIMESTAMP WITH TIMEZONE", ZonedDateTimeTypeName(true)),
    ("TIMESTAMP WITH TIMEZONE NOT NULL", ZonedDateTimeTypeName(false)),
    ("DURATION", DurationTypeName(true)),
    ("DURATION NOT NULL", DurationTypeName(false)),
    ("POINT", PointTypeName(true)),
    ("POINT NOT NULL", PointTypeName(false)),
    ("NODE", NodeTypeName(true)),
    ("NODE NOT NULL", NodeTypeName(false)),
    ("ANY NODE", NodeTypeName(true)),
    ("ANY NODE NOT NULL", NodeTypeName(false)),
    ("VERTEX", NodeTypeName(true)),
    ("VERTEX NOT NULL", NodeTypeName(false)),
    ("ANY VERTEX", NodeTypeName(true)),
    ("ANY VERTEX NOT NULL", NodeTypeName(false)),
    ("RELATIONSHIP", RelationshipTypeName(true)),
    ("RELATIONSHIP NOT NULL", RelationshipTypeName(false)),
    ("ANY RELATIONSHIP", RelationshipTypeName(true)),
    ("ANY RELATIONSHIP NOT NULL", RelationshipTypeName(false)),
    ("EDGE", RelationshipTypeName(true)),
    ("EDGE NOT NULL", RelationshipTypeName(false)),
    ("ANY EDGE", RelationshipTypeName(true)),
    ("ANY EDGE NOT NULL", RelationshipTypeName(false)),
    ("MAP", MapTypeName(true)),
    ("MAP NOT NULL", MapTypeName(false)),
    ("ANY MAP", MapTypeName(true)),
    ("ANY MAP NOT NULL", MapTypeName(false)),
    ("PATH", PathTypeName(true)),
    ("PATH NOT NULL", PathTypeName(false)),
    ("ANY PROPERTY VALUE", PropertyValueTypeName(true)),
    ("ANY PROPERTY VALUE NOT NULL", PropertyValueTypeName(false)),
    ("PROPERTY VALUE", PropertyValueTypeName(true)),
    ("PROPERTY VALUE NOT NULL", PropertyValueTypeName(false)),
    ("ANY VALUE", AnyTypeName(true)),
    ("ANY VALUE NOT NULL", AnyTypeName(false)),
    ("ANY", AnyTypeName(true)),
    ("ANY NOT NULL", AnyTypeName(false))
  )

  private val listTypes = allNonListTypes.flatMap { case (innerTypeString, innerTypeExpr: CypherTypeName) =>
    Seq(
      // LIST<type>
      (s"LIST<$innerTypeString>", ListTypeName(innerTypeExpr, isNullable = true)),
      (s"LIST<$innerTypeString> NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)),
      (s"ARRAY<$innerTypeString>", ListTypeName(innerTypeExpr, isNullable = true)),
      (s"ARRAY<$innerTypeString> NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)),
      (s"$innerTypeString LIST", ListTypeName(innerTypeExpr, isNullable = true)),
      (s"$innerTypeString LIST NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)),
      (s"$innerTypeString ARRAY", ListTypeName(innerTypeExpr, isNullable = true)),
      (s"$innerTypeString ARRAY NOT NULL", ListTypeName(innerTypeExpr, isNullable = false)),
      // LIST<LIST<type>>
      (
        s"LIST<LIST<$innerTypeString>>",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)
      ),
      (
        s"LIST<LIST<$innerTypeString>> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"LIST<LIST<$innerTypeString> NOT NULL>",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"LIST<LIST<$innerTypeString> NOT NULL> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
      ),
      (
        s"LIST<ARRAY<$innerTypeString>>",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)
      ),
      (
        s"LIST<ARRAY<$innerTypeString>> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"LIST<ARRAY<$innerTypeString> NOT NULL>",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"LIST<ARRAY<$innerTypeString> NOT NULL> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
      ),
      (s"LIST<$innerTypeString LIST>", ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)),
      (
        s"LIST<$innerTypeString LIST> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"LIST<$innerTypeString LIST NOT NULL>",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"LIST<$innerTypeString LIST NOT NULL> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
      ),
      (
        s"LIST<$innerTypeString ARRAY>",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)
      ),
      (
        s"LIST<$innerTypeString ARRAY> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"LIST<$innerTypeString ARRAY NOT NULL>",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"LIST<$innerTypeString ARRAY NOT NULL> NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
      ),
      (s"LIST<$innerTypeString> LIST", ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)),
      (
        s"LIST<$innerTypeString> LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"LIST<$innerTypeString> NOT NULL LIST",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"LIST<$innerTypeString> NOT NULL LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
      ),
      (
        s"ARRAY<$innerTypeString> LIST",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)
      ),
      (
        s"ARRAY<$innerTypeString> LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"ARRAY<$innerTypeString> NOT NULL LIST",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"ARRAY<$innerTypeString> NOT NULL LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
      ),
      (s"$innerTypeString LIST LIST", ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)),
      (
        s"$innerTypeString LIST LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"$innerTypeString LIST NOT NULL LIST",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"$innerTypeString LIST NOT NULL LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
      ),
      (s"$innerTypeString ARRAY LIST", ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = true)),
      (
        s"$innerTypeString ARRAY LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = true), isNullable = false)
      ),
      (
        s"$innerTypeString ARRAY NOT NULL LIST",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = true)
      ),
      (
        s"$innerTypeString ARRAY NOT NULL LIST NOT NULL",
        ListTypeName(ListTypeName(innerTypeExpr, isNullable = false), isNullable = false)
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
                ),
                isNullable = false
              ),
              isNullable = false
            ),
            isNullable = false
          ),
          isNullable = true
        )
      ),
      (
        s"$innerTypeString LIST NOT NULL LIST LIST NOT NULL LIST",
        ListTypeName(
          ListTypeName(
            ListTypeName(
              ListTypeName(
                innerTypeExpr,
                isNullable = false
              ),
              isNullable = true
            ),
            isNullable = false
          ),
          isNullable = true
        )
      )
    )
  }

  (allNonListTypes ++ listTypes).foreach { case (typeString, typeExpr: CypherTypeName) =>
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
}
