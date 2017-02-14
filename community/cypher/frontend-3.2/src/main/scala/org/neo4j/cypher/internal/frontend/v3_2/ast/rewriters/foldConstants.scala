package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

case object foldConstants extends Rewriter {
  def apply(that: AnyRef): AnyRef =
  try {
    instance.apply(that)
  } catch {
    case e: java.lang.ArithmeticException => throw new v3_2.ArithmeticException(e.getMessage, e)
  }
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case e@Add(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedDecimalIntegerLiteral((lhs.value + rhs.value).toString)(e.position)
    case e@Add(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
      DecimalDoubleLiteral((lhs.value + rhs.value).toString)(e.position)
    case e@Add(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value + rhs.value).toString)(e.position)
    case e@Add(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value + rhs.value).toString)(e.position)

    case e@Subtract(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedDecimalIntegerLiteral((lhs.value - rhs.value).toString)(e.position)
    case e@Subtract(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
      DecimalDoubleLiteral((lhs.value - rhs.value).toString)(e.position)
    case e@Subtract(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value - rhs.value).toString)(e.position)
    case e@Subtract(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value - rhs.value).toString)(e.position)

    case e@Multiply(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedDecimalIntegerLiteral((lhs.value * rhs.value).toString)(e.position)
    case e@Multiply(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
      DecimalDoubleLiteral((lhs.value * rhs.value).toString)(e.position)
    case e@Multiply(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value * rhs.value).toString)(e.position)
    case e@Multiply(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value * rhs.value).toString)(e.position)

    case e@Multiply(lhs: NumberLiteral, rhs: NumberLiteral) =>
      e
    case e@Multiply(lhs: NumberLiteral, rhs) =>
      Multiply(rhs, lhs)(e.position).rewrite(instance)
    case e@Multiply(lhs@Multiply(innerLhs, innerRhs: NumberLiteral), rhs: NumberLiteral) =>
      Multiply(Multiply(innerRhs, rhs)(lhs.position), innerLhs)(e.position).rewrite(instance)
    case e@Multiply(lhs@Multiply(innerLhs: NumberLiteral, innerRhs), rhs: NumberLiteral) =>
      Multiply(Multiply(innerLhs, rhs)(lhs.position), innerRhs)(e.position).rewrite(instance)

    case e@Divide(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedDecimalIntegerLiteral((lhs.value / rhs.value).toString)(e.position)
    case e@Divide(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
      DecimalDoubleLiteral((lhs.value / rhs.value).toString)(e.position)
    case e@Divide(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value / rhs.value).toString)(e.position)
    case e@Divide(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value / rhs.value).toString)(e.position)

    case e@Modulo(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedDecimalIntegerLiteral((lhs.value % rhs.value).toString)(e.position)
    case e@Modulo(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
      DecimalDoubleLiteral((lhs.value % rhs.value).toString)(e.position)
    case e@Modulo(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value % rhs.value).toString)(e.position)
    case e@Modulo(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral((lhs.value % rhs.value).toString)(e.position)

    case e@Pow(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      DecimalDoubleLiteral(Math.pow(lhs.value.toDouble, rhs.value.toDouble).toString)(e.position)
    case e@Pow(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
      DecimalDoubleLiteral(Math.pow(lhs.value, rhs.value.toDouble).toString)(e.position)
    case e@Pow(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral(Math.pow(lhs.value.toDouble, rhs.value).toString)(e.position)
    case e@Pow(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
      DecimalDoubleLiteral(Math.pow(lhs.value, rhs.value).toString)(e.position)

    case e: UnaryAdd =>
      e.rhs

    case e@UnarySubtract(rhs: SignedIntegerLiteral) =>
      SignedDecimalIntegerLiteral((-rhs.value).toString)(e.position)
    case e: UnarySubtract =>
      Subtract(SignedDecimalIntegerLiteral("0")(e.position), e.rhs)(e.position)

    case e@Equals(lhs: IntegerLiteral, rhs: IntegerLiteral) => asAst(lhs.value == rhs.value, e)
    case e@Equals(lhs: DoubleLiteral, rhs: DoubleLiteral) => asAst(lhs.value == rhs.value, e)
    case e@Equals(lhs: IntegerLiteral, rhs: DoubleLiteral) => asAst(lhs.value.doubleValue() == rhs.value, e)
    case e@Equals(lhs: DoubleLiteral, rhs: IntegerLiteral) => asAst(lhs.value == rhs.value.doubleValue(), e)

    case e@LessThan(lhs: IntegerLiteral, rhs: IntegerLiteral) => asAst(lhs.value < rhs.value, e)
    case e@LessThan(lhs: DoubleLiteral, rhs: DoubleLiteral) => asAst(lhs.value < rhs.value, e)
    case e@LessThan(lhs: IntegerLiteral, rhs: DoubleLiteral) => asAst(lhs.value.doubleValue() < rhs.value, e)
    case e@LessThan(lhs: DoubleLiteral, rhs: IntegerLiteral) => asAst(lhs.value < rhs.value.doubleValue(), e)

    case e@GreaterThan(lhs: IntegerLiteral, rhs: IntegerLiteral) => asAst(lhs.value > rhs.value, e)
    case e@GreaterThan(lhs: DoubleLiteral, rhs: DoubleLiteral) => asAst(lhs.value > rhs.value, e)
    case e@GreaterThan(lhs: IntegerLiteral, rhs: DoubleLiteral) => asAst(lhs.value.doubleValue() > rhs.value, e)
    case e@GreaterThan(lhs: DoubleLiteral, rhs: IntegerLiteral) => asAst(lhs.value > rhs.value.doubleValue(), e)
  })

  private def asAst(b: Boolean, e: Expression) = if (b) True()(e.position) else False()(e.position)
}
