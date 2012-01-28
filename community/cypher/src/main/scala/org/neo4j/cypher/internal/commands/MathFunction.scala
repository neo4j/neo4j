package org.neo4j.cypher.internal.commands

import org.neo4j.cypher.internal.symbols.{AnyType, Identifier, NumberType}
import java.lang.Math
import org.neo4j.cypher.CypherTypeException

abstract class MathFunction(arguments: Expression*) extends Expression {
  protected def asDouble(a: Any) = try {
    a.asInstanceOf[Number].doubleValue()
  }
  catch {
    case x: ClassCastException => throw new CypherTypeException("Expected a numeric value for " + toString() + ", but got: " + a.toString)
  }

  protected def asInt(a: Any) = asDouble(a).round

  def innerExpectedType = NumberType()

  def identifier = Identifier(toString(), NumberType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = arguments.flatMap(_.dependencies(AnyType()))

  protected def name: String

  private def argumentsString: String = arguments.map(_.toString()).mkString(",")

  override def toString() = name + "(" + argumentsString + ")"
}

case class AbsFunction(argument: Expression) extends MathFunction(argument) {
  def apply(m: Map[String, Any]): Any = Math.abs(asDouble(argument(m)))

  protected def name = "abs"

  def rewrite(f: (Expression) => Expression) = f(AbsFunction(argument.rewrite(f)))
}

case class SignFunction(argument: Expression) extends MathFunction(argument) {
  def apply(m: Map[String, Any]): Any = Math.signum(asDouble(argument(m)))

  protected def name = "sign"

  def rewrite(f: (Expression) => Expression) = f(SignFunction(argument.rewrite(f)))
}

case class RoundFunction(expression: Expression) extends MathFunction(expression) {
  def apply(m: Map[String, Any]): Any = math.round(asDouble(expression(m)))

  protected def name = "round"

  def rewrite(f: (Expression) => Expression) = f(RoundFunction(expression.rewrite(f)))
}

case class SqrtFunction(argument: Expression) extends MathFunction(argument) {
  def apply(m: Map[String, Any]): Any = Math.sqrt(asDouble(argument(m)))

  protected def name = "sqrt"

  def rewrite(f: (Expression) => Expression) = f(SqrtFunction(argument.rewrite(f)))
}