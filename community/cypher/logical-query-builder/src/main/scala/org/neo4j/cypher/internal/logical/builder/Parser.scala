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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.v4_0.ast.UnresolvedCall
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.parser.{Expressions, ProcedureCalls}
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, Rewriter, topDown}
import org.parboiled.scala.{ReportingParseRunner, Rule1}

object Parser {
  val injectCachedNodeProperties: Rewriter = topDown(Rewriter.lift {
    case ContainerIndex(Variable("cache"), Property(v@Variable(node), pkn:PropertyKeyName)) =>
      CachedProperty(node, v, pkn, NODE_TYPE)(AbstractLogicalPlanBuilder.pos)
  })
  val invalidateInputPositions: Rewriter = topDown(Rewriter.lift {
    case a:ASTNode => a.dup(a.children.toSeq :+ AbstractLogicalPlanBuilder.pos)
  })

  def cleanup[T <: ASTNode](in: T): T =
    injectCachedNodeProperties.andThen(invalidateInputPositions)(in).asInstanceOf[T]

  private val regex = s"(.+) [Aa][Ss] ([a-zA-Z0-9` @]+)".r
  private val parser = new Parser

  def parseProjections(projections: String*): Map[String, Expression] = {
    projections.map {
      case regex(Parser(expression), alias) => (VariableParser.unescaped(alias), expression)
      case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as a projection")
    }.toMap
  }

  def parseExpression(text: String): Expression = parser.parseExpression(text)

  def parseProcedureCall(text: String): UnresolvedCall = parser.parseProcedureCall(text)

  def unapply(arg: String): Option[Expression] = Some(parser.parseExpression(arg))
}

private class Parser extends Expressions with ProcedureCalls {
  private val expressionParser: Rule1[Expression] = Expression
  private val procedureCallParser: Rule1[UnresolvedCall] = Call

  def parseExpression(text: String): Expression = {
    val res = ReportingParseRunner(expressionParser).run(text)
    res.result match {
      case Some(e) => Parser.cleanup(e)
      case None => throw new IllegalArgumentException(s"Could not parse expression: ${res.parseErrors}")
    }
  }

  def parseProcedureCall(text: String): UnresolvedCall = {
    val res = ReportingParseRunner(procedureCallParser).run(s"CALL $text")
    res.result match {
      case Some(e) => Parser.cleanup(e)
      case None => throw new IllegalArgumentException(s"Could not parse procedure call: ${res.parseErrors}")
    }
  }
}
