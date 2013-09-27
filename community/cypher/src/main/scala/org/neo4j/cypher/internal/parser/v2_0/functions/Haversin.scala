package org.neo4j.cypher.internal.parser.v2_0.functions

import org.neo4j.cypher.internal.parser.v2_0._
import org.neo4j.cypher.internal.symbols.{DoubleType, NumberType}
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions}

case object Haversin extends Function {
  def name = "haversin"

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) : SemanticCheck =
    checkArgs(invocation, 1) then
      invocation.arguments.constrainType(NumberType()) then
      invocation.specifyType(DoubleType())

  def toCommand(invocation: ast.FunctionInvocation) =
    commandexpressions.HaversinFunction(invocation.arguments(0).toCommand)
}
