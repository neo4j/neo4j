/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commands.{Predicate => CommandPredicate}
import org.neo4j.cypher.internal.commands.expressions.{Expression => CommandExpression}

object Function {
  private val knownFunctions = Seq(
    functions.Abs,
    functions.Acos,
    functions.Add,
    functions.And,
    functions.Asin,
    functions.Atan,
    functions.Atan2,
    functions.Avg,
    functions.Ceil,
    functions.Coalesce,
    functions.Collect,
    functions.Ceil,
    functions.Cos,
    functions.Cot,
    functions.Count,
    functions.Degrees,
    functions.Divide,
    functions.E,
    functions.Equals,
    functions.EndNode,
    functions.Exp,
    functions.Floor,
    functions.GreaterThan,
    functions.GreaterThanOrEqual,
    functions.Has,
    functions.Head,
    functions.Id,
    functions.In,
    functions.InvalidNotEquals,
    functions.IsNotNull,
    functions.IsNull,
    functions.Labels,
    functions.Last,
    functions.Left,
    functions.Length,
    functions.LessThan,
    functions.LessThanOrEqual,
    functions.Log,
    functions.Log10,
    functions.Lower,
    functions.LTrim,
    functions.Max,
    functions.Min,
    functions.Modulo,
    functions.Multiply,
    functions.Nodes,
    functions.Not,
    functions.NotEquals,
    functions.Or,
    functions.Pi,
    functions.PercentileCont,
    functions.PercentileDisc,
    functions.Pow,
    functions.Radians,
    functions.Rand,
    functions.Range,
    functions.RegularExpression,
    functions.Relationships,
    functions.Rels,
    functions.Replace,
    functions.Right,
    functions.Round,
    functions.RTrim,
    functions.Sign,
    functions.Sin,
    functions.Sqrt,
    functions.StartNode,
    functions.StdDev,
    functions.StdDevP,
    functions.Str,
    functions.Substring,
    functions.Subtract,
    functions.Sum,
    functions.Tail,
    functions.Tan,
    functions.Timestamp,
    functions.Trim,
    functions.Type,
    functions.Upper,
    functions.Xor
  )

  val lookup : Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase, f) }.toMap
}

abstract class Function extends SemanticChecking {
  def name : String
  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) : SemanticCheck = {
    when(invocation.distinct) {
      SemanticError(s"Invalid use of DISTINCT with function '$name'", invocation.token)
    }
  }

  protected def checkArgs(invocation: ast.FunctionInvocation, n: Int) : Option[SemanticError] = {
    Seq(checkMinArgs(invocation, n), checkMaxArgs(invocation, n)).flatten.headOption
  }

  protected def checkMaxArgs(invocation: ast.FunctionInvocation, n: Int) : Option[SemanticError] = {
    if (invocation.arguments.length > n)
      Some(SemanticError(s"Too many parameters for function '$name'", invocation.token))
    else
      None
  }

  protected def checkMinArgs(invocation: ast.FunctionInvocation, n: Int) : Option[SemanticError] = {
    if (invocation.arguments.length < n)
      Some(SemanticError(s"Insufficient parameters for function '$name'", invocation.token))
    else
      None
  }

  def toCommand(invocation: ast.FunctionInvocation) : CommandExpression
}


abstract class AggregatingFunction extends Function {
  override def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) : SemanticCheck = {
    when(ctx == ast.Expression.SemanticContext.Simple) {
      SemanticError(s"Invalid use of aggregating function ${name} in this context", invocation.token)
    }
  }
}


trait LegacyPredicate { self: Function =>
  protected def constructCommandPredicate(arguments: Seq[ast.Expression])(constructor: => (IndexedSeq[CommandPredicate] => CommandPredicate)) = {
    val commands = arguments.map(e => (e, e.toCommand))
    commands.find(!_._2.isInstanceOf[CommandPredicate]) match {
      case Some((arg, _)) => throw new SyntaxException(s"Argument to function '$name' is not a predicate (${arg.token.startPosition})")
      case None => constructor(commands.map(_._2.asInstanceOf[CommandPredicate]).toIndexedSeq)
    }
  }
}
