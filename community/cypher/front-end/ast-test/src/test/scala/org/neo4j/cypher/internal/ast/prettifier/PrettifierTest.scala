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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllIndexActions
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DefaultWith
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLimit
import org.neo4j.cypher.internal.ast.ParsedAsOrderBy
import org.neo4j.cypher.internal.ast.ParsedAsSkip
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ArraySeq

class PrettifierTest extends CypherFunSuite with AstConstructionTestSupport {
  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  test("stringify deny privilege") {
    val commandArraySeq = DenyPrivilege(
      DatabasePrivilege(AllIndexActions, NamedDatabasesScope(ArraySeq(NamespacedName("hej")(pos)))(pos))(pos),
      immutable = false,
      None,
      List(AllDatabasesQualifier()(pos)),
      ArraySeq(StringLiteral("hej")(pos.withInputLength(0)))
    )(pos)

    val commandList = DenyPrivilege(
      DatabasePrivilege(AllIndexActions, NamedDatabasesScope(List(NamespacedName("hej")(pos)))(pos))(pos),
      immutable = false,
      None,
      List(AllDatabasesQualifier()(pos)),
      List(StringLiteral("hej")(pos.withInputLength(0)))
    )(pos)

    commandArraySeq shouldBe commandList
    prettifier.asString(commandArraySeq) shouldBe "DENY INDEX MANAGEMENT ON DATABASE hej TO hej"
    prettifier.asString(commandArraySeq) shouldBe prettifier.asString(commandList)
  }

  private val orderBys: Seq[Option[(String, OrderBy)]] = Seq(
    Some(("ORDER BY n ASCENDING", orderBy(sortItem(varFor("n"))))),
    None
  )

  private val skips: Seq[Option[(String, Skip)]] = Seq(
    Some(("SKIP 1", skip(1L))),
    None
  )

  private val limits: Seq[Option[(String, Limit)]] = Seq(
    Some(("LIMIT 1", limit(1L))),
    None
  )

  private val wheres: Seq[Option[(String, Where)]] = Seq(
    Some(("WHERE n IS NOT NULL", where(isNotNull(varFor("n"))))),
    None
  )
  private val withStar: String = "WITH *"
  private val filter: String = "FILTER"

  /**
   * clause separator: line break + indent
   */
  private val SEP: String =
    s"""
       |  """.stripMargin

  private val clauses: Seq[(String, Clause)] =
    (for {
      o <- orderBys
      s <- skips
      l <- limits
      w <- wheres
    } yield {
      (o, s, l, w) match {
        case (None, None, None, None) => Seq(
            (withStar, withAllTyped(None, None, None, None, DefaultWith))
          )
        // starting with ORDER BY
        case (Some((oCy, oAst)), None, None, None) => Seq(
            (s"$withStar$SEP$oCy".stripMargin, withAllTyped(Some(oAst), None, None, None, DefaultWith)),
            (s"$oCy", withAllTyped(Some(oAst), None, None, None, ParsedAsOrderBy))
          )
        case (Some((oCy, oAst)), Some((sCy, sAst)), None, None) => Seq(
            (s"$withStar$SEP$oCy$SEP$sCy".stripMargin, withAllTyped(Some(oAst), Some(sAst), None, None, DefaultWith)),
            (s"$oCy$SEP$sCy", withAllTyped(Some(oAst), Some(sAst), None, None, ParsedAsOrderBy))
          )
        case (Some((oCy, oAst)), None, Some((lCy, lAst)), None) => Seq(
            (s"$withStar$SEP$oCy$SEP$lCy", withAllTyped(Some(oAst), None, Some(lAst), None, DefaultWith)),
            (s"$oCy$SEP$lCy", withAllTyped(Some(oAst), None, Some(lAst), None, ParsedAsOrderBy))
          )
        case (Some((oCy, oAst)), Some((sCy, sAst)), Some((lCy, lAst)), None) => Seq(
            (s"$withStar$SEP$oCy$SEP$sCy$SEP$lCy", withAllTyped(Some(oAst), Some(sAst), Some(lAst), None, DefaultWith)),
            (s"$oCy$SEP$sCy$SEP$lCy", withAllTyped(Some(oAst), Some(sAst), Some(lAst), None, ParsedAsOrderBy))
          )
        case (Some((oCy, oAst)), None, None, Some((wCy, wAst))) => Seq(
            (s"$withStar$SEP$oCy$SEP$wCy", withAllTyped(Some(oAst), None, None, Some(wAst), DefaultWith))
          )
        case (Some((oCy, oAst)), Some((sCy, sAst)), None, Some((wCy, wAst))) => Seq(
            (s"$withStar$SEP$oCy$SEP$sCy$SEP$wCy", withAllTyped(Some(oAst), Some(sAst), None, Some(wAst), DefaultWith))
          )
        case (Some((oCy, oAst)), None, Some((lCy, lAst)), Some((wCy, wAst))) => Seq(
            (s"$withStar$SEP$oCy$SEP$lCy$SEP$wCy", withAllTyped(Some(oAst), None, Some(lAst), Some(wAst), DefaultWith))
          )
        case (Some((oCy, oAst)), Some((sCy, sAst)), Some((lCy, lAst)), Some((wCy, wAst))) => Seq(
            (
              s"$withStar$SEP$oCy$SEP$sCy$SEP$lCy$SEP$wCy",
              withAllTyped(Some(oAst), Some(sAst), Some(lAst), Some(wAst), DefaultWith)
            )
          )
        // starting with SKIP
        case (None, Some((sCy, sAst)), None, None) => Seq(
            (s"$withStar$SEP$sCy", withAllTyped(None, Some(sAst), None, None, DefaultWith)),
            (s"$sCy", withAllTyped(None, Some(sAst), None, None, ParsedAsSkip))
          )
        case (None, Some((sCy, sAst)), Some((lCy, lAst)), None) => Seq(
            (s"$withStar$SEP$sCy$SEP$lCy", withAllTyped(None, Some(sAst), Some(lAst), None, DefaultWith)),
            (s"$sCy$SEP$lCy", withAllTyped(None, Some(sAst), Some(lAst), None, ParsedAsSkip))
          )
        case (None, Some((sCy, sAst)), None, Some((wCy, wAst))) => Seq(
            (s"$withStar$SEP$sCy$SEP$wCy", withAllTyped(None, Some(sAst), None, Some(wAst), DefaultWith))
          )
        case (None, Some((sCy, sAst)), Some((lCy, lAst)), Some((wCy, wAst))) => Seq(
            (s"$withStar$SEP$sCy$SEP$lCy$SEP$wCy", withAllTyped(None, Some(sAst), Some(lAst), Some(wAst), DefaultWith))
          )
        // starting with LIMIT
        case (None, None, Some((lCy, lAst)), None) => Seq(
            (s"$withStar$SEP$lCy", withAllTyped(None, None, Some(lAst), None, DefaultWith)),
            (s"$lCy", withAllTyped(None, None, Some(lAst), None, ParsedAsLimit))
          )
        case (None, None, Some((lCy, lAst)), Some((wCy, wAst))) => Seq(
            (s"$withStar$SEP$lCy$SEP$wCy", withAllTyped(None, None, Some(lAst), Some(wAst), DefaultWith))
          )
        // starting with WHERE
        case (None, None, None, Some((wCy, wAst))) => Seq(
            (s"$withStar$SEP$wCy", withAllTyped(None, None, None, Some(wAst), DefaultWith)),
            (s"$filter $wCy", withAllTyped(None, None, None, Some(wAst), ParsedAsFilter))
          )
        case _ => Seq()
      }
    }).flatten

  for ((cypher, ast) <- clauses) {
    test(cypher) {
      prettifier.asString(singleQuery(ast)) shouldBe cypher
    }
  }
}
