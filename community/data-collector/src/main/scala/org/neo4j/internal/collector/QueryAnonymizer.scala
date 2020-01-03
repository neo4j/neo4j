/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.collector

import org.neo4j.cypher.{CypherExpressionEngineOption, CypherPlannerOption, CypherRuntimeOption, CypherVersion}
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.v3_5.ast.Statement
import org.neo4j.cypher.internal.v3_5.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.parser.CypherParser
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.anonymizeQuery
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.values.ValueMapper
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

trait QueryAnonymizer {
  def queryText(queryText: String): String
  def queryParams(params: MapValue): Object
}

case class PlainText(valueMapper: ValueMapper.JavaMapper) extends QueryAnonymizer {
  def queryText(queryText: String): String = queryText
  def queryParams(params: MapValue): Object = params.map(valueMapper)
}

object IdAnonymizer {
  private val preParser = new PreParser(CypherVersion.default, CypherPlannerOption.default, CypherRuntimeOption.default, CypherExpressionEngineOption.default, 0)
}

case class IdAnonymizer(tokens: TokenRead) extends QueryAnonymizer {

  private val parser = new CypherParser()
  private val prettifier = Prettifier(ExpressionStringifier(_.asCanonicalStringVal))

  override def queryText(queryText: String): String = {
    val preParsedQuery = IdAnonymizer.preParser.preParseQuery(queryText, false)
    val originalAst = parser.parse(preParsedQuery.statement, None)
    val anonymizer = anonymizeQuery(new IdAnonymizerState(tokens, prettifier))
    val rewrittenAst = anonymizer(originalAst).asInstanceOf[Statement]
    preParsedQuery.rawPreparserOptions ++ prettifier.asString(rewrittenAst)
  }

  override def queryParams(params: MapValue): AnyRef = {
    params.hashCode().formatted("%x")
  }
}

class IdAnonymizerState(tokens: TokenRead, prettifier: Prettifier) extends org.neo4j.cypher.internal.v3_5.rewriting.rewriters.Anonymizer {

  private val variables = mutable.Map[String, String]()
  private val parameters = mutable.Map[String, String]()
  private val unknownTokens = mutable.Map[String, String]()

  override def variable(name: String): String =
    variables.getOrElseUpdate(name, "var" + variables.size)

  override def unaliasedReturnItemName(anonymizedExpression: Expression, input: String): String =
    prettifier.mkStringOf(anonymizedExpression)

  override def label(name: String): String =
    tokenName("L", name, tokens.nodeLabel(name))

  override def relationshipType(name: String): String =
    tokenName("R", name, tokens.relationshipType(name))

  override def propertyKey(name: String): String =
    tokenName("p", name, tokens.propertyKey(name))

  override def parameter(name: String): String =
    parameters.getOrElseUpdate(name, "param" + parameters.size)

  override def literal(value: String): String =
    s"string[${value.length}]"

  private def tokenName(prefix: String, name: String, id: Int): String =
    id match {
      case -1 => unknownTokens.getOrElseUpdate(name, "UNKNOWN"+unknownTokens.size)
      case x => prefix + x
    }
}

