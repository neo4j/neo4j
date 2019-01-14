/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.v3_5.logical.plans.{ManySeekableArgs, PrefixRange, SingleSeekableArg}
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.NonEmptyList
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class SargableTest extends CypherFunSuite with AstConstructionTestSupport {

  val expr1 = mock[Expression]
  val expr2 = mock[Expression]

  val nodeA = varFor("a")

  test("StringRangeSeekable finds n.prop STARTS WITH 'prefix'") {
    val propKey: PropertyKeyName = PropertyKeyName("prop") _
    val leftExpr: Property = Property(nodeA, propKey) _
    val startsWith: StartsWith = StartsWith(leftExpr, StringLiteral("prefix") _) _
    assertMatches(startsWith) {
      case AsStringRangeSeekable(PrefixRangeSeekable(range, expr, ident, propertyKey)) =>
        range should equal(PrefixRange(StringLiteral("prefix")(pos)))
        expr should equal(startsWith)
        ident should equal(nodeA)
        propertyKey should equal(propKey)
    }
  }

  test("Seekable finds Equals") {
    assertMatches(Equals(expr1, expr2)_) {
      case WithSeekableArgs(lhs, rhs) =>
        lhs should equal(expr1)
        rhs should equal(SingleSeekableArg(expr2))
    }
  }

  test("Seekable finds In") {
    assertMatches(In(expr1, expr2)_) {
      case WithSeekableArgs(lhs, rhs) =>
        lhs should equal(expr1)
        rhs should equal(ManySeekableArgs(expr2))
    }
  }

  test("ManySeekableArgs has size hint for collections") {
    ManySeekableArgs(ListLiteral(Seq(expr1, expr2))_).sizeHint should equal(Some(2))
  }

  test("IdSeekable works") {
    val leftExpr: FunctionInvocation = FunctionInvocation(FunctionName("id") _, nodeA)_
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])
    val expr: Equals = Equals(leftExpr, expr2) _

    assertMatches(expr) {
      case AsIdSeekable(seekable) =>
        seekable.ident should equal(nodeA)
        seekable.expr should equal(leftExpr)
        seekable.name should equal(nodeA.name)
        seekable.args.expr should equal(expr2)
        seekable.args.sizeHint should equal(Some(1))
    }
  }

  test("IdSeekable does not match if rhs depends on lhs variable") {
    val leftExpr: FunctionInvocation = FunctionInvocation(FunctionName("id") _, nodeA)_
    when(expr2.dependencies).thenReturn(Set[LogicalVariable](nodeA))
    val expr: Equals = Equals(leftExpr, expr2) _

    assertDoesNotMatch(expr) {
      case AsIdSeekable(_) => (/* oh noes */)
    }
  }

  test("IdSeekable does not match if function is not the id function") {
    val leftExpr: FunctionInvocation = FunctionInvocation(FunctionName("rand") _, nodeA)_
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])
    val expr: Equals = Equals(leftExpr, expr2) _

    assertDoesNotMatch(expr) {
      case AsIdSeekable(_) => (/* oh noes */)
    }
  }

  test("PropertySeekable works with plain expressions") {
    val leftExpr: Property = Property(nodeA, PropertyKeyName("id")_)_
    val expr: Expression = In(leftExpr, expr2)_
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertMatches(expr) {
      case AsPropertySeekable(seekable) =>
        seekable.ident should equal(nodeA)
        seekable.expr should equal(leftExpr)
        seekable.name should equal(nodeA.name)
        seekable.args.expr should equal(expr2)
        seekable.args.sizeHint should equal(None)
    }
  }

  test("PropertySeekable works with collection expressions") {
    val leftExpr: Property = Property(nodeA, PropertyKeyName("id")_)_
    val rightExpr: ListLiteral = ListLiteral(Seq(expr1, expr2))_
    val expr: Expression = In(leftExpr, rightExpr)_
    when(expr1.dependencies).thenReturn(Set.empty[LogicalVariable])
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertMatches(expr) {
      case AsPropertySeekable(seekable) =>
        seekable.ident should equal(nodeA)
        seekable.expr should equal(leftExpr)
        seekable.name should equal(nodeA.name)
        seekable.args.expr should equal(rightExpr)
        seekable.args.sizeHint should equal(Some(2))
    }
  }

  test("PropertySeekable propertyValueType with ListLiteral") {
    val leftExpr: Property = Property(nodeA, PropertyKeyName("id")_)_
    val rightExpr: ListLiteral = ListLiteral(Seq(expr1, expr2))_
    val expr: Expression = In(leftExpr, rightExpr)_
    when(expr1.dependencies).thenReturn(Set.empty[LogicalVariable])
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    val types = ASTAnnotationMap[Expression, ExpressionTypeInfo]()
        .updated(expr1, ExpressionTypeInfo(TypeSpec.exact(CTFloat)))
        .updated(expr2, ExpressionTypeInfo(TypeSpec.exact(CTInteger)))

    val AsPropertySeekable(seekable) = expr

    seekable.propertyValueType(SemanticTable(types)) should be(CTNumber)
  }

  test("PropertySeekable propertyValueType with equals") {
    val propExpr = Property(nodeA, PropertyKeyName("id")_)_
    val expr: Expression = Equals(propExpr, expr1)_
    when(expr1.dependencies).thenReturn(Set.empty[LogicalVariable])

    val types = ASTAnnotationMap[Expression, ExpressionTypeInfo]()
      .updated(expr1, ExpressionTypeInfo(TypeSpec.exact(CTFloat)))

    val AsPropertySeekable(seekable) = expr

    seekable.propertyValueType(SemanticTable(types)) should be(CTFloat)
  }

  test("PropertySeekable propertyValueType with Parameter") {
    val leftExpr: Property = Property(nodeA, PropertyKeyName("id")_)_
    val rightExpr: Parameter = Parameter("foo", CTString)(pos)
    val expr: Expression = In(leftExpr, rightExpr)_

    val types = ASTAnnotationMap[Expression, ExpressionTypeInfo]()
      .updated(rightExpr, ExpressionTypeInfo(TypeSpec.exact(CTList(CTString))))

    val AsPropertySeekable(seekable) = expr

    seekable.propertyValueType(SemanticTable(types)) should be(CTString)
  }

  test("InequalityRangeSeekable propertyValueType") {
    val leftExpr: Property = Property(nodeA, PropertyKeyName("id")_)_
    val min = SignedDecimalIntegerLiteral("10")(pos)
    val max = DecimalDoubleLiteral("20.5")(pos)
    val expr: Expression = AndedPropertyInequalities(nodeA, leftExpr, NonEmptyList(GreaterThan(leftExpr, min)(pos), LessThanOrEqual(leftExpr, max)(pos)))

    val table = mock[SemanticTable]
    when(table.getActualTypeFor(min)).thenReturn(CTInteger.invariant)
    when(table.getActualTypeFor(max)).thenReturn(CTFloat.invariant)
    val AsValueRangeSeekable(seekable) = expr

    seekable.propertyValueType(table) should be(CTNumber)
  }

  test("PropertySeekable does not match if rhs depends on lhs variable") {
    val leftExpr: Property = Property(nodeA, PropertyKeyName("id")_)_
    when(expr2.dependencies).thenReturn(Set[LogicalVariable](nodeA))
    val expr: Expression = In(leftExpr, expr2)_

    assertDoesNotMatch(expr) {
      case AsPropertySeekable(_) => (/* oh noes */)
    }
  }

  test("PropertyScannable works") {
    val propertyExpr: Property = Property(nodeA, PropertyKeyName("name")_)_
    val expr: FunctionInvocation = FunctionInvocation(FunctionName("exists") _, propertyExpr)_

    assertMatches(expr) {
      case AsPropertyScannable(scannable) =>
        scannable.expr should equal(expr)
        scannable.property should equal(propertyExpr)
        scannable.ident should equal(nodeA)
        scannable.propertyKey should equal(propertyExpr.propertyKey)
    }
  }

  // Testing Seekable.combineMultipleTypeSpecs

  test("combines empty TypeSpec to any") {
    // Given
    val specs = Seq.empty[TypeSpec]

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTAny)
  }

  test("combines int to int") {
    // Given
    val specs = Seq(CTInteger.invariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTInteger)
  }

  test("combines int and int to int") {
    // Given
    val specs = Seq(CTInteger.invariant, CTInteger.invariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTInteger)
  }

  test("combines float and int to number") {
    // Given
    val specs = Seq(CTFloat.invariant, CTInteger.invariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines float and point to any") {
    // Given
    val specs = Seq(CTFloat.invariant, CTPoint.invariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTAny)
  }

  test("combines covariant types") {
    // Given
    val specs = Seq(CTFloat.invariant, CTNumber.covariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines union types") {
    // Given
    val specs = Seq(CTFloat.invariant union CTInteger.invariant, CTNumber.covariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines same union types") {
    // Given
    val specs = Seq(CTFloat.invariant union CTInteger.invariant, CTFloat.invariant union CTInteger.invariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines unrelated union types") {
    // Given
    val specs = Seq(CTFloat.invariant union CTInteger.invariant, CTString.invariant union CTInteger.invariant)

    // when
    val spec = Seekable.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTAny)
  }

  // Testing Seekable.cypherTypeForTypeSpec

  test("converts any to CTAny") {
    // Given
    val spec = CTAny.invariant

    // when
    val typ = Seekable.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts T to CTAny") {
    // Given
    val spec = CTAny.covariant

    // when
    val typ = Seekable.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts contravariant type to CTAny") {
    // Given
    val spec = CTFloat.contravariant

    // when
    val typ = Seekable.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts union type to CTAny") {
    // Given
    val spec = CTFloat.invariant union CTString.invariant

    // when
    val typ = Seekable.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts intersection type to CTAny") {
    // Given
    val spec = CTFloat.invariant intersect CTString.invariant

    // when
    val typ = Seekable.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  def assertMatches[T](item: Expression)(pf: PartialFunction[Expression, T]) =
    if (pf.isDefinedAt(item)) pf(item) else fail(s"Failed to match: $item")

  def assertDoesNotMatch[T](item: Expression)(pf: PartialFunction[Expression, T]) =
    if (pf.isDefinedAt(item)) fail(s"Erroneously matched: $item")
}
