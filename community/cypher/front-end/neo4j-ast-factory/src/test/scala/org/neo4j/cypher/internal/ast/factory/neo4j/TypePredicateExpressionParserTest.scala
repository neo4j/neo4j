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
import org.neo4j.cypher.internal.expressions.BooleanTypeName
import org.neo4j.cypher.internal.expressions.CypherTypeName
import org.neo4j.cypher.internal.expressions.DateTypeName
import org.neo4j.cypher.internal.expressions.DurationTypeName
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FloatTypeName
import org.neo4j.cypher.internal.expressions.IntegerTypeName
import org.neo4j.cypher.internal.expressions.LocalDateTimeTypeName
import org.neo4j.cypher.internal.expressions.LocalTimeTypeName
import org.neo4j.cypher.internal.expressions.PointTypeName
import org.neo4j.cypher.internal.expressions.StringTypeName
import org.neo4j.cypher.internal.expressions.ZonedDateTimeTypeName
import org.neo4j.cypher.internal.expressions.ZonedTimeTypeName
import org.neo4j.cypher.internal.parser.CypherParser.ExpressionContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TypePredicateExpressionParserTest extends CypherFunSuite with ParserSyntaxTreeBase[Cst.Expression, Expression]
    with AstConstructionTestSupport {

  implicit val javaccRule: JavaccRule[Expression] = JavaccRule.Expression
  implicit val antlrRule: AntlrRule[ExpressionContext] = AntlrRule.Expression

  Seq(
    ("BOOL", BooleanTypeName()),
    ("BOOLEAN", BooleanTypeName()),
    ("VARCHAR", StringTypeName()),
    ("STRING", StringTypeName()),
    ("INTEGER", IntegerTypeName()),
    ("INT", IntegerTypeName()),
    ("SIGNED INTEGER", IntegerTypeName()),
    ("FLOAT", FloatTypeName()),
    ("DATE", DateTypeName()),
    ("LOCAL TIME", LocalTimeTypeName()),
    ("TIME WITHOUT TIMEZONE", LocalTimeTypeName()),
    ("ZONED TIME", ZonedTimeTypeName()),
    ("TIME WITH TIMEZONE", ZonedTimeTypeName()),
    ("LOCAL DATETIME", LocalDateTimeTypeName()),
    ("TIMESTAMP WITHOUT TIMEZONE", LocalDateTimeTypeName()),
    ("ZONED DATETIME", ZonedDateTimeTypeName()),
    ("TIMESTAMP WITH TIMEZONE", ZonedDateTimeTypeName()),
    ("DURATION", DurationTypeName()),
    ("POINT", PointTypeName())
  ).foreach { case (typeString, typeExpr: CypherTypeName) =>
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
}
