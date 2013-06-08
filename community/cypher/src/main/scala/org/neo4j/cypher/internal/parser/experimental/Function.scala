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
package org.neo4j.cypher.internal.parser.experimental

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{Predicate => CommandPredicate}
import org.neo4j.cypher.internal.commands.expressions.{Expression => CommandExpression}

object Function {
  private val knownFunctions = Seq(
    functions.Has,
    functions.Add,
    functions.Subtract,
    functions.Multiply,
    functions.Divide,
    functions.Modulo,
    functions.Pow,
    functions.And,
    functions.Xor,
    functions.Or,
    functions.Not,
    functions.Equals,
    functions.NotEquals,
    functions.InvalidNotEquals,
    functions.LessThan,
    functions.GreaterThan,
    functions.LessThanOrEqual,
    functions.GreaterThanOrEqual,
    functions.RegularExpression,
    functions.In,
    functions.IsNull,
    functions.IsNotNull,
    functions.Labels,
    functions.Type,
    functions.Id,
    functions.Length,
    functions.Nodes,
    functions.Relationships,
    functions.Rels,
    functions.Abs,
    functions.Round,
    functions.Sqrt,
    functions.Sign,
    functions.Head,
    functions.Last,
    functions.Tail,
    functions.Replace,
    functions.Left,
    functions.Right,
    functions.Substring,
    functions.Lower,
    functions.Upper,
    functions.LTrim,
    functions.RTrim,
    functions.Trim,
    functions.Str,
    functions.Range,
    functions.Count,
    functions.Sum,
    functions.Avg,
    functions.Min,
    functions.Max,
    functions.Collect,
    functions.Coalesce,
    functions.Timestamp
  )

  val lookup : Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase, f) }.toMap
}

abstract class Function {
  def name : String
  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) : SemanticCheck

  def toCommand(invocation: ast.FunctionInvocation) : CommandExpression

  protected def checkArgs(invocation: ast.FunctionInvocation, n: Int) : Option[SemanticError] = {
    Seq(checkMinArgs(invocation, n), checkMaxArgs(invocation, n)).flatten.headOption
  }

  protected def checkArgsThen(invocation: ast.FunctionInvocation, n: Int)(check: => SemanticCheck) : SemanticCheck = {
    checkArgs(invocation, n) >>= when(invocation.arguments.length == n)(check)
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

  protected def when(pred: Boolean)(check: => SemanticCheck) : SemanticCheck = state => {
    if (pred)
      check(state)
    else
      SemanticCheckResult.success(state)
  }
}


trait AggregatingFunction { self: Function =>
  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) : SemanticCheck = {
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

  protected def nullable(invocation: ast.FunctionInvocation, predicate: CommandPredicate) = {
    val map = invocation.arguments.filter(_.isInstanceOf[ast.Nullable]).map(e => (e.toCommand, e.isInstanceOf[ast.DefaultTrue]))
    if (map.isEmpty)
      predicate
    else
      commands.NullablePredicate(predicate, map)
  }
}
