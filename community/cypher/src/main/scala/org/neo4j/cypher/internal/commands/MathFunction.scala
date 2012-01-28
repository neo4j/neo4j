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