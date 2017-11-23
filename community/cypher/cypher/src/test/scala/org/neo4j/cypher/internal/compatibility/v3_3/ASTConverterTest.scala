package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition => InputPositionV3_3, ast => astV3_3}
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.{expressions => expressionsV3_4}
import org.scalatest.{FunSuite, Matchers}

class ASTConverterTest extends FunSuite with Matchers {

  val pos3_3 = InputPositionV3_3(0,0,0)
  val pos3_4 = InputPosition(0,0,0)

  test("should convert an IntegerLiteral with its position") {
    val i3_3 = astV3_3.SignedDecimalIntegerLiteral("5")(InputPositionV3_3(1, 2, 3))
    val i3_4 = expressionsV3_4.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 3))

    val rewritten = ASTConverter.convertExpression[expressionsV3_4.SignedDecimalIntegerLiteral](i3_3)
    rewritten should be(i3_4)
    rewritten.position should be(i3_4.position)
  }

  test("should convert an Add with its position (recursively)") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(InputPositionV3_3(1, 2, 3))
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(InputPositionV3_3(1, 2, 5))
    val add3_3 = astV3_3.Add(i3_3a, i3_3b)(InputPositionV3_3(1,2,3))
    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(InputPosition(1, 2, 3))
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 5))
    val add3_4 = expressionsV3_4.Add(i3_4a, i3_4b)(InputPosition(1,2,3))

    val rewritten = ASTConverter.convertExpression[expressionsV3_4.Add](add3_3)
    rewritten should be(add3_4)
    rewritten.position should equal(add3_4.position)
    rewritten.lhs.position should equal(i3_4a.position)
    rewritten.rhs.position should equal(i3_4b.position)
  }

  test("should convert Expression with Seq") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val l3_3 = astV3_3.ListLiteral(Seq(i3_3a, i3_3b))(pos3_3)
    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val l3_4 = expressionsV3_4.ListLiteral(Seq(i3_4a, i3_4b))(pos3_4)

    ASTConverter.convertExpression[expressionsV3_4.ListLiteral](l3_3) should be(l3_4)
  }

  test("should convert Expression with Option") {
    val i3_3 = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val v3_3 = astV3_3.Variable("var")(pos3_3)
    val f3_3 = astV3_3.FilterScope(v3_3, Some(i3_3))(pos3_3)
    val f3_3b = astV3_3.FilterScope(v3_3, None)(pos3_3)

    val i3_4 = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val v3_4 = expressionsV3_4.Variable("var")(pos3_4)
    val f3_4 = expressionsV3_4.FilterScope(v3_4, Some(i3_4))(pos3_4)
    val f3_4b = expressionsV3_4.FilterScope(v3_4, None)(pos3_4)

    ASTConverter.convertExpression[expressionsV3_4.FilterScope](f3_3) should be(f3_4)
    ASTConverter.convertExpression[expressionsV3_4.FilterScope](f3_3b) should be(f3_4b)
  }

  test("should convert Expression with Set") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val l3_3 = astV3_3.Ands(Set(i3_3a, i3_3b))(pos3_3)
    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val l3_4 = expressionsV3_4.Ands(Set(i3_4a, i3_4b))(pos3_4)

    ASTConverter.convertExpression[expressionsV3_4.Ands](l3_3) should be(l3_4)
  }

  test("should convert Expression with Seq of Tuple") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val i3_3c = astV3_3.SignedDecimalIntegerLiteral("10")(pos3_3)
    val i3_3d = astV3_3.SignedDecimalIntegerLiteral("11")(pos3_3)
    val c3_3 = astV3_3.CaseExpression(None, List((i3_3a, i3_3b), (i3_3c, i3_3d)), None)(pos3_3)

    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val i3_4c = expressionsV3_4.SignedDecimalIntegerLiteral("10")(pos3_4)
    val i3_4d = expressionsV3_4.SignedDecimalIntegerLiteral("11")(pos3_4)
    val c3_4 = expressionsV3_4.CaseExpression(None, List((i3_4a, i3_4b), (i3_4c, i3_4d)), None)(pos3_4)

    ASTConverter.convertExpression[expressionsV3_4.CaseExpression](c3_3) should be(c3_4)
  }

  test("should convert Expression with Seq of Tuple (MapExpression)") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val p3_3a = astV3_3.PropertyKeyName("a")(pos3_3)
    val p3_3b = astV3_3.PropertyKeyName("b")(pos3_3)
    val m3_3 = astV3_3.MapExpression(Seq((p3_3a, i3_3a),(p3_3b, i3_3b)))(pos3_3)

    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val p3_4a = expressionsV3_4.PropertyKeyName("a")(pos3_4)
    val p3_4b = expressionsV3_4.PropertyKeyName("b")(pos3_4)
    val m3_4 = expressionsV3_4.MapExpression(Seq((p3_4a, i3_4a),(p3_4b, i3_4b)))(pos3_4)

    ASTConverter.convertExpression[expressionsV3_4.CaseExpression](m3_3) should be(m3_4)
  }
}
