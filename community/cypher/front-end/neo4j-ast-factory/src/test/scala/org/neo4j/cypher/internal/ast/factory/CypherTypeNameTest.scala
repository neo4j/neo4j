/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory

import org.neo4j.cypher.internal.ast.AnyTypeName
import org.neo4j.cypher.internal.ast.BooleanTypeName
import org.neo4j.cypher.internal.ast.ClosedDynamicUnionTypeName
import org.neo4j.cypher.internal.ast.CypherTypeName
import org.neo4j.cypher.internal.ast.DateTypeName
import org.neo4j.cypher.internal.ast.DurationTypeName
import org.neo4j.cypher.internal.ast.FloatTypeName
import org.neo4j.cypher.internal.ast.IntegerTypeName
import org.neo4j.cypher.internal.ast.ListTypeName
import org.neo4j.cypher.internal.ast.LocalDateTimeTypeName
import org.neo4j.cypher.internal.ast.LocalTimeTypeName
import org.neo4j.cypher.internal.ast.MapTypeName
import org.neo4j.cypher.internal.ast.NodeTypeName
import org.neo4j.cypher.internal.ast.NothingTypeName
import org.neo4j.cypher.internal.ast.NullTypeName
import org.neo4j.cypher.internal.ast.PathTypeName
import org.neo4j.cypher.internal.ast.PointTypeName
import org.neo4j.cypher.internal.ast.PropertyValueTypeName
import org.neo4j.cypher.internal.ast.RelationshipTypeName
import org.neo4j.cypher.internal.ast.StringTypeName
import org.neo4j.cypher.internal.ast.ZonedDateTimeTypeName
import org.neo4j.cypher.internal.ast.ZonedTimeTypeName
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

class CypherTypeNameTest extends CypherFunSuite {

  /**
   * To make it easier to see which normalization rule is being tested, each rule here is given an arbitrary number
   * which is referred to by each test.
   * Normalization Rules:
   *  Type Name Normalization: This is done in parsing e.g. BOOL -> BOOLEAN (Not tested here, see TypePredicateExpressionParserTest
   *  1.  Duplicate types are removed (done initially in parsing; see previous comment on where to find more tests)
   *  2.  NULL NOT NULL and NOTHING NOT NULL -> NOTHING.
   *  3.  NOTHING is absorbed if any other type is present
   *  4.  NULL is absorbed into types e.g ANY<BOOLEAN | NULL> is BOOLEAN
   *  5.  NULL is propagated through other types e.g ANY<BOOLEAN | INTEGER NOT NULL> is ANY<BOOLEAN | INTEGER>
   *  6.  Dynamic unions are always flattened e.g ANY<ANY<BOOLEAN | FLOAT>> is ANY<BOOLEAN | FLOAT>
   *  7.  If all types are present, then it is simplified to ANY
   *  8.  If ANY is present, then other types may be absorbed e.g ANY<BOOLEAN | ANY > is ANY
   *  9.  PROPERTY VALUE is simplified to the Closed Dynamic Union of all property value types
   *  10. Encapsulated LISTs are absorbed e.g LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> = LIST<BOOLEAN | INTEGER>
   *  11. Ordering of types
   */

  private val pos: InputPosition = DummyPosition(0)

  private val setOfAllPropertyTypes: Set[CypherTypeName] = Set(
    BooleanTypeName(isNullable = true)(pos),
    StringTypeName(isNullable = true)(pos),
    IntegerTypeName(isNullable = true)(pos),
    FloatTypeName(isNullable = true)(pos),
    DateTypeName(isNullable = true)(pos),
    LocalTimeTypeName(isNullable = true)(pos),
    ZonedTimeTypeName(isNullable = true)(pos),
    LocalDateTimeTypeName(isNullable = true)(pos),
    ZonedDateTimeTypeName(isNullable = true)(pos),
    DurationTypeName(isNullable = true)(pos),
    PointTypeName(isNullable = true)(pos),
    ListTypeName(BooleanTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(StringTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(IntegerTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(FloatTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(DateTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(LocalTimeTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(ZonedTimeTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(LocalDateTimeTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(ZonedDateTimeTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(DurationTypeName(isNullable = false)(pos), isNullable = true)(pos),
    ListTypeName(PointTypeName(isNullable = false)(pos), isNullable = true)(pos)
  )

  private def normalizeTypeList(types: Set[CypherTypeName]): List[CypherTypeName] = {
    val closedDynamicUnionTypeName: ClosedDynamicUnionTypeName = ClosedDynamicUnionTypeName(types)(pos)
    val normalizedType = CypherTypeName.normalizeTypes(closedDynamicUnionTypeName)

    normalizedType match {
      case cut: ClosedDynamicUnionTypeName => cut.sortedInnerTypes
      case _                               => List(normalizedType)
    }
  }

  private val typesWithoutInnerTypes: List[(String, Boolean => CypherTypeName)] =
    List[(String, Boolean => CypherTypeName)](
      ("BOOLEAN", BooleanTypeName(_)(pos)),
      ("STRING", StringTypeName(_)(pos)),
      ("INTEGER", IntegerTypeName(_)(pos)),
      ("FLOAT", FloatTypeName(_)(pos)),
      ("DATE", DateTypeName(_)(pos)),
      ("LOCAL TIME", LocalTimeTypeName(_)(pos)),
      ("ZONED TIME", ZonedTimeTypeName(_)(pos)),
      ("LOCAL DATETIME", LocalDateTimeTypeName(_)(pos)),
      ("ZONED DATETIME", ZonedDateTimeTypeName(_)(pos)),
      ("DURATION", DurationTypeName(_)(pos)),
      ("POINT", PointTypeName(_)(pos)),
      ("NODE", NodeTypeName(_)(pos)),
      ("RELATIONSHIP", RelationshipTypeName(_)(pos)),
      ("MAP", MapTypeName(_)(pos)),
      ("PATH", PathTypeName(_)(pos))
    )

  private val typesWithoutInnerTypesPlusSimpleList: Seq[(String, Boolean => CypherTypeName)] =
    typesWithoutInnerTypes.dropRight(1) ++ List(
      ("LIST<BOOLEAN>", ListTypeName(BooleanTypeName(isNullable = true)(pos), _)(pos)),
      ("PATH", PathTypeName(_)(pos))
    )

  private val typesWithoutInnerTypesPlusSimpleListPlusAny: Seq[(String, Boolean => CypherTypeName)] =
    typesWithoutInnerTypesPlusSimpleList ++ List(
      ("ANY", AnyTypeName(_)(pos))
    )

  // Test updateIsNullable
  // RULES TESTED: 2
  test("NULL update nullable false should return NOTHING") {
    val nullType = NullTypeName()(pos)
    val nothingTypeName = NothingTypeName()(pos)
    nullType.updateIsNullable(false) should be(nothingTypeName)
  }

  test("NULL update nullable true should return NULL") {
    val nullType = NullTypeName()(pos)
    nullType.updateIsNullable(true) should be(nullType)
  }

  // RULES TESTED: 2
  test("NOTHING update nullable false should return NOTHING") {
    val nothingTypeName = NothingTypeName()(pos)
    nothingTypeName.updateIsNullable(false) should be(nothingTypeName)
  }

  test("NOTHING update nullable true should return NOTHING") {
    val nothingTypeName = NothingTypeName()(pos)
    nothingTypeName.updateIsNullable(true) should be(nothingTypeName)
  }

  test("ANY<> update nullable false should return ANY<> with no changes") {
    val closedDynamicUnionType = ClosedDynamicUnionTypeName(Set(
      IntegerTypeName(isNullable = true)(pos),
      BooleanTypeName(isNullable = true)(pos)
    ))(pos)
    closedDynamicUnionType.updateIsNullable(false) should be(closedDynamicUnionType)
  }

  test("ANY<> update nullable true should return ANY<> with no changes") {
    val closedDynamicUnionType = ClosedDynamicUnionTypeName(Set(
      IntegerTypeName(isNullable = false)(pos),
      BooleanTypeName(isNullable = false)(pos)
    ))(pos)
    closedDynamicUnionType.updateIsNullable(true) should be(closedDynamicUnionType)
  }

  typesWithoutInnerTypesPlusSimpleListPlusAny.foreach { case (typeName, typeExpr) =>
    test(s"$typeName update nullable true should return that type as nullable") {
      val expr = typeExpr(false)
      expr.updateIsNullable(true) should be(typeExpr(true))
    }

    test(s"$typeName update nullable false should return that type as not nullable") {
      val expr = typeExpr(true)
      expr.updateIsNullable(false) should be(typeExpr(false))
    }
  }

  // Test simplify and type normalization

  test("NOTHING should simplify to itself") {
    val expr = NothingTypeName()(pos)
    expr.simplify should be(expr)
  }

  test("NULL should simplify to itself") {
    val expr = NullTypeName()(pos)
    expr.simplify should be(expr)
  }

  typesWithoutInnerTypes.foreach { case (typeName, typeExpr) =>
    test(s"$typeName should simplify to itself without NOT NULL") {
      val expr = typeExpr(false)
      expr.simplify should be(expr)
    }

    test(s"$typeName should simplify to itself with NOT NULL") {
      val expr = typeExpr(true)
      expr.simplify should be(expr)
    }

    test(s"LIST<$typeName> should simplify to itself") {
      val exprInnerNotNull = ListTypeName(typeExpr(false), isNullable = true)(pos)
      exprInnerNotNull.simplify should be(exprInnerNotNull)

      val exprAllNull = ListTypeName(typeExpr(true), isNullable = true)(pos)
      exprAllNull.simplify should be(exprAllNull)

      val exprNoNull = ListTypeName(typeExpr(false), isNullable = false)(pos)
      exprNoNull.simplify should be(exprNoNull)

      val exprInnerNull = ListTypeName(typeExpr(true), isNullable = false)(pos)
      exprInnerNull.simplify should be(exprInnerNull)
    }

    test(s"ANY<$typeName> should simplify to $typeName") {
      ClosedDynamicUnionTypeName(Set(typeExpr(false)))(pos).simplify should be(typeExpr(false))

      ClosedDynamicUnionTypeName(Set(typeExpr(true)))(pos).simplify should be(typeExpr(true))
    }
  }

  // RULES TESTED: 5
  test("ANY<t1 NOT NULL, t2, ...> should make all inner types nullable") {
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      IntegerTypeName(isNullable = true)(pos),
      DateTypeName(isNullable = false)(pos),
      StringTypeName(isNullable = true)(pos),
      BooleanTypeName(isNullable = true)(pos),
      StringTypeName(isNullable = false)(pos),
      DurationTypeName(isNullable = false)(pos)
    ))(pos)) should be(ClosedDynamicUnionTypeName(Set(
      BooleanTypeName(isNullable = true)(pos),
      StringTypeName(isNullable = true)(pos),
      IntegerTypeName(isNullable = true)(pos),
      DateTypeName(isNullable = true)(pos),
      DurationTypeName(isNullable = true)(pos)
    ))(pos))
  }

  // RULES TESTED: 3
  test("NOTHING should be absorbed if any other type is present") {
    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        NothingTypeName()(pos),
        NullTypeName()(pos)
      ))(pos)
    ) should be(
      NullTypeName()(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        NothingTypeName()(pos),
        BooleanTypeName(isNullable = true)(pos)
      ))(pos)
    ) should be(
      BooleanTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        NothingTypeName()(pos),
        ClosedDynamicUnionTypeName(Set(IntegerTypeName(isNullable = false)(pos)))(pos)
      ))(pos)
    ) should be(
      IntegerTypeName(isNullable = false)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        NothingTypeName()(pos),
        ListTypeName(
          ClosedDynamicUnionTypeName(Set(NothingTypeName()(pos), IntegerTypeName(isNullable = false)(pos)))(pos),
          isNullable = true
        )(pos)
      ))(pos)
    ) should be(
      ListTypeName(IntegerTypeName(isNullable = false)(pos), isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        NothingTypeName()(pos),
        IntegerTypeName(isNullable = true)(pos),
        FloatTypeName(isNullable = true)(pos),
        ListTypeName(NothingTypeName()(pos), isNullable = true)(pos)
      ))(pos)
    ) should be(
      ClosedDynamicUnionTypeName(Set(
        IntegerTypeName(isNullable = true)(pos),
        FloatTypeName(isNullable = true)(pos),
        ListTypeName(NothingTypeName()(pos), isNullable = true)(pos)
      ))(pos)
    )
  }

  // RULES TESTED: 4
  test("NULL should be absorbed if any other type is present (excl. NOTHING)") {
    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        BooleanTypeName(isNullable = true)(pos),
        NullTypeName()(pos)
      ))(pos)
    ) should be(
      BooleanTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        BooleanTypeName(isNullable = false)(pos),
        NullTypeName()(pos)
      ))(pos)
    ) should be(
      BooleanTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        ClosedDynamicUnionTypeName(Set(BooleanTypeName(isNullable = false)(pos)))(pos),
        NullTypeName()(pos)
      ))(pos)
    ) should be(
      BooleanTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        NullTypeName()(pos),
        ListTypeName(
          ClosedDynamicUnionTypeName(Set(NullTypeName()(pos), IntegerTypeName(isNullable = false)(pos)))(pos),
          isNullable = false
        )(pos)
      ))(pos)
    ) should be(
      ListTypeName(IntegerTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )
  }

  // RULES TESTED: 8
  test("Any absorbs all other types") {
    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        AnyTypeName(isNullable = true)(pos),
        BooleanTypeName(isNullable = true)(pos)
      ))(pos)
    ) should be(
      AnyTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        AnyTypeName(isNullable = false)(pos),
        PropertyValueTypeName(isNullable = true)(pos)
      ))(pos)
    ) should be(
      AnyTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        AnyTypeName(isNullable = false)(pos),
        ListTypeName(BooleanTypeName(isNullable = false)(pos), isNullable = true)(pos),
        ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = false)(pos)))(pos)
      ))(pos)
    ) should be(
      AnyTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          AnyTypeName(isNullable = true)(pos),
          BooleanTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ) should be(
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )
  }

  // RULES TESTED: 7
  test("If all types are present, they should normalize to ANY") {
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      PropertyValueTypeName(isNullable = true)(pos),
      NodeTypeName(isNullable = true)(pos),
      RelationshipTypeName(isNullable = true)(pos),
      MapTypeName(isNullable = true)(pos),
      PathTypeName(isNullable = true)(pos),
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      AnyTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      PropertyValueTypeName(isNullable = false)(pos),
      NodeTypeName(isNullable = false)(pos),
      RelationshipTypeName(isNullable = false)(pos),
      MapTypeName(isNullable = false)(pos),
      PathTypeName(isNullable = false)(pos),
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = false)(pos)
    ))(pos)) should be(
      AnyTypeName(isNullable = false)(pos)
    )

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(setOfAllPropertyTypes ++ Set(
      NodeTypeName(isNullable = true)(pos),
      RelationshipTypeName(isNullable = true)(pos),
      MapTypeName(isNullable = true)(pos),
      PathTypeName(isNullable = true)(pos),
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      AnyTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(setOfAllPropertyTypes.map(_.updateIsNullable(false)) ++ Set(
        NodeTypeName(isNullable = false)(pos),
        RelationshipTypeName(isNullable = false)(pos),
        MapTypeName(isNullable = false)(pos),
        PathTypeName(isNullable = false)(pos),
        ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = false)(pos)
      ))(pos)
    ) should be(
      AnyTypeName(isNullable = false)(pos)
    )

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(setOfAllPropertyTypes ++ Set(
      NodeTypeName(isNullable = true)(pos),
      RelationshipTypeName(isNullable = true)(pos),
      MapTypeName(isNullable = true)(pos),
      PathTypeName(isNullable = true)(pos),
      ListTypeName(
        ClosedDynamicUnionTypeName(setOfAllPropertyTypes ++ Set(
          NodeTypeName(isNullable = true)(pos),
          RelationshipTypeName(isNullable = true)(pos),
          MapTypeName(isNullable = true)(pos),
          PathTypeName(isNullable = true)(pos),
          ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(
      AnyTypeName(isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ListTypeName(
        ClosedDynamicUnionTypeName(setOfAllPropertyTypes ++ Set(
          NodeTypeName(isNullable = true)(pos),
          RelationshipTypeName(isNullable = true)(pos),
          MapTypeName(isNullable = true)(pos),
          PathTypeName(isNullable = true)(pos),
          ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ) should be(
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )
  }

  // RULES TESTED: 9
  test("Property value type normalizes to all storable types") {
    CypherTypeName.normalizeTypes(PropertyValueTypeName(isNullable = true)(pos)) should be(
      ClosedDynamicUnionTypeName(setOfAllPropertyTypes)(pos)
    )

    CypherTypeName.normalizeTypes(PropertyValueTypeName(isNullable = false)(pos)) should be(
      ClosedDynamicUnionTypeName(setOfAllPropertyTypes.map(_.updateIsNullable(false)))(pos)
    )

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        PropertyValueTypeName(isNullable = true)(pos),
        BooleanTypeName(isNullable = true)(pos),
        NodeTypeName(isNullable = true)(pos)
      )
    )(pos)) should be(
      ClosedDynamicUnionTypeName(setOfAllPropertyTypes ++ Set(NodeTypeName(isNullable = true)(pos)))(pos)
    )
  }

  // RULES TESTED: 6
  test("closed dynamic unions should be flattened when inside lists") {
    val union1 = ClosedDynamicUnionTypeName(Set(
      BooleanTypeName(isNullable = false)(pos),
      StringTypeName(isNullable = true)(pos)
    ))(pos)
    val union2 = ClosedDynamicUnionTypeName(Set(
      IntegerTypeName(isNullable = false)(pos),
      ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = false)(pos)))(pos)
    ))(pos)
    val union3 = ClosedDynamicUnionTypeName(Set(
      ClosedDynamicUnionTypeName(Set(
        ClosedDynamicUnionTypeName(Set(
          ClosedDynamicUnionTypeName(Set(DateTypeName(isNullable = true)(pos)))(pos)
        ))(pos),
        DurationTypeName(isNullable = true)(pos)
      ))(pos)
    ))(pos)

    CypherTypeName.normalizeTypes(ListTypeName(
      ClosedDynamicUnionTypeName(Set(union1, union2, union3))(pos),
      isNullable = true
    )(
      pos
    )) should be(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          StringTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos),
          DateTypeName(isNullable = true)(pos),
          DurationTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    )
  }

  // RULES TESTED: 6
  test("closed dynamic unions should be flattened when mixed with other types") {
    val union1 = ClosedDynamicUnionTypeName(Set(BooleanTypeName(isNullable = false)(pos)))(pos)
    val union2 = ClosedDynamicUnionTypeName(Set(
      BooleanTypeName(isNullable = false)(pos),
      StringTypeName(isNullable = false)(pos)
    ))(pos)
    val union3 = ClosedDynamicUnionTypeName(Set(IntegerTypeName(isNullable = false)(pos)))(pos)
    val union4 = ClosedDynamicUnionTypeName(Set(
      FloatTypeName(isNullable = false)(pos),
      StringTypeName(isNullable = false)(pos)
    ))(pos)
    val union14 = ClosedDynamicUnionTypeName(Set(union1, union4))(pos)
    val notAUnionDate = DateTypeName(isNullable = false)(pos)
    val notAUnionList =
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = false)(pos),
          BooleanTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)

    val inputTypeSet = Set[CypherTypeName](union3, notAUnionList, union2, notAUnionDate, union14)
    val outputTypeSet = Set[CypherTypeName](
      BooleanTypeName(isNullable = true)(pos),
      StringTypeName(isNullable = true)(pos),
      IntegerTypeName(isNullable = true)(pos),
      FloatTypeName(isNullable = true)(pos),
      DateTypeName(isNullable = true)(pos),
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )

    CypherTypeName.normalizeTypes(
      ListTypeName(ClosedDynamicUnionTypeName(inputTypeSet)(pos), isNullable = false)(pos)
    ) should be(
      ListTypeName(ClosedDynamicUnionTypeName(outputTypeSet)(pos), isNullable = false)(pos)
    )

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(inputTypeSet)(pos)) should be(
      ClosedDynamicUnionTypeName(outputTypeSet)(pos)
    )
  }

  // RULES TESTED: 6, 10
  test("Lists that are encapsulated fully by another list should be removed") {
    // LIST<BOOLEAN> | LIST<BOOLEAN> == LIST<BOOLEAN>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | ANY<ANY<LIST<ANY<BOOLEAN>>>> == LIST<BOOLEAN>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ClosedDynamicUnionTypeName(Set(
        ClosedDynamicUnionTypeName(Set(
          ListTypeName(
            ClosedDynamicUnionTypeName(Set(
              BooleanTypeName(isNullable = true)(pos)
            ))(pos),
            isNullable = true
          )(pos)
        ))(pos)
      ))(pos)
    ))(pos)) should be(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> == LIST<BOOLEAN | INTEGER>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    )

    // LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> | LIST<BOOLEAN | INTEGER | FLOAT> == LIST<BOOLEAN | INTEGER | FLOAT>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos),
          FloatTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos),
          FloatTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    )

    // LIST<BOOLEAN> | LIST<BOOLEAN NOT NULL> == LIST<BOOLEAN>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ListTypeName(BooleanTypeName(isNullable = false)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | LIST<ANY> == LIST<ANY>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | LIST<NULL> == LIST<BOOLEAN>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ListTypeName(NullTypeName()(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<STRING> NOT NULL | LIST<STRING NOT NULL> == LIST<STRING>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        ListTypeName(StringTypeName(isNullable = true)(pos), isNullable = false)(pos),
        ListTypeName(StringTypeName(isNullable = false)(pos), isNullable = true)(pos)
      )
    )(pos)) should be(
      ListTypeName(StringTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )

    // Not normalized: LIST<BOOLEAN NOT NULL> | LIST<NULL>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = false)(pos), isNullable = true)(pos),
      ListTypeName(NullTypeName()(pos), isNullable = true)(pos)
    ))(pos)) should be(ClosedDynamicUnionTypeName(Set(
      ListTypeName(BooleanTypeName(isNullable = false)(pos), isNullable = true)(pos),
      ListTypeName(NullTypeName()(pos), isNullable = true)(pos)
    ))(pos))

    // Not completely overlapping types, not normalized: LIST<BOOLEAN | INTEGER> | LIST<BOOLEAN | FLOAT>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          FloatTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(ClosedDynamicUnionTypeName(Set(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          FloatTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos))

    // Nullable unions: LIST<BOOLEAN NOT NULL | INTEGER NOT NULL> | LIST<BOOLEAN | INTEGER> = LIST<BOOLEAN | INTEGER>
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(Set(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = false)(pos),
          IntegerTypeName(isNullable = false)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          IntegerTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        BooleanTypeName(isNullable = true)(pos),
        IntegerTypeName(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos))
  }

  typesWithoutInnerTypesPlusSimpleList.foreach { case (typeName, typeExpr) =>
    test(s"Remove duplicated types when one has NOT NULL: $typeName") {
      normalizeTypeList(Set(typeExpr(true), typeExpr(false))) should be(
        List(typeExpr(true))
      )
    }

    // RULES TESTED: 1
    test(s"Remove duplicated types that appears in earlier normalization step: $typeName") {
      // LIST<TYPE | ANY<TYPE>> -> LIST<TYPE>
      normalizeTypeList(Set(
        ListTypeName(
          ClosedDynamicUnionTypeName(Set(
            typeExpr(true),
            ClosedDynamicUnionTypeName(Set(typeExpr(true)))(pos)
          ))(pos),
          isNullable = true
        )(pos)
      )) should be(
        List(ListTypeName(typeExpr(true), isNullable = true)(pos))
      )
    }

    test(s"Remove NOT NULL from all types if one type is without: $typeName") {
      val allRelevantTypes = typesWithoutInnerTypesPlusSimpleList.map(_._2)
      val noNull = allRelevantTypes.map(_(false))

      val randomizedInput = Random.shuffle(noNull :+ typeExpr(true)).toSet
      normalizeTypeList(randomizedInput) should be(allRelevantTypes.map(_(true)))
    }
  }

  // RULES TESTED: 1, 6
  test("Remove duplicated ANY<...>") {
    val union = ClosedDynamicUnionTypeName(Set(
      BooleanTypeName(isNullable = false)(pos),
      StringTypeName(isNullable = false)(pos)
    ))(pos)

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(
        union,
        ClosedDynamicUnionTypeName(Set(union))(pos)
      ))(pos)
    ) should be(union)
  }

  // Test sorting
  // RULES TESTED: 11
  test("Single types should be ordered correctly") {
    // NOTHING, NULL, BOOLEAN NOT NULL, BOOLEAN, ... (remove PATH as it comes after list and add back in later)
    val noInnerTypes: List[CypherTypeName] =
      List(NothingTypeName()(pos), NullTypeName()(pos)) ++ typesWithoutInnerTypes.dropRight(1).flatMap {
        case (_, typeExpr) => List(typeExpr(false), typeExpr(true))
      }
    // LIST<NOTHING> NOT NULL, LIST<NOTHING>, LIST<NULL> NOT NULL, LIST<NULL>, LIST<BOOLEAN NOT NULL> NOT NULL,  ...
    val singleTypeLists = noInnerTypes.flatMap(type_ => {
      List(
        ListTypeName(type_, isNullable = false)(pos),
        ListTypeName(type_, isNullable = true)(pos)
      )
    })

    val singleTypePathAndAnyLists = List(
      ListTypeName(PathTypeName(isNullable = false)(pos), isNullable = false)(pos),
      ListTypeName(PathTypeName(isNullable = false)(pos), isNullable = true)(pos),
      ListTypeName(PathTypeName(isNullable = true)(pos), isNullable = false)(pos),
      ListTypeName(PathTypeName(isNullable = true)(pos), isNullable = true)(pos),
      ListTypeName(AnyTypeName(isNullable = false)(pos), isNullable = false)(pos),
      ListTypeName(AnyTypeName(isNullable = false)(pos), isNullable = true)(pos),
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = false)(pos),
      ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
    )
    // LIST<LIST<NOTHING> NOT NULL> NOT NULL, LIST<LIST<NOTHING> NOT NULL>, LIST<LIST<NOTHING>> NOT NULL, ...
    val listOfSingleTypeLists = (singleTypeLists ++ singleTypePathAndAnyLists).flatMap(type_ => {
      List(
        ListTypeName(type_, isNullable = false)(pos),
        ListTypeName(type_, isNullable = true)(pos)
      )
    }) ++ singleTypePathAndAnyLists
    // all of the above
    val allNonUnionSingleTypes =
      noInnerTypes ++ singleTypeLists ++ listOfSingleTypeLists ++ List(
        PathTypeName(isNullable = false)(pos),
        PathTypeName(isNullable = true)(pos)
      )
    // ANY<NOTHING>, ANY<NULL>, ANY<BOOLEAN NOT NULL>, ANY<BOOLEAN>, ...
    val unionsOfSingleType = allNonUnionSingleTypes.map(t => ClosedDynamicUnionTypeName(Set(t))(pos))

    // All single types
    val all = allNonUnionSingleTypes ++ unionsOfSingleType ++ List(
      AnyTypeName(isNullable = false)(pos),
      AnyTypeName(isNullable = true)(pos)
    )

    Random.shuffle(all).sorted should be(all)
  }

  // RULES TESTED: 11
  test("Types without inner types should be ordered correctly") {
    val outputNoNull = typesWithoutInnerTypes.map(_._2(false))
    normalizeTypeList(Random.shuffle(outputNoNull).toSet) should be(outputNoNull)

    val outputWithNull = typesWithoutInnerTypes.map(_._2(true))
    normalizeTypeList(Random.shuffle(outputWithNull).toSet) should be(outputWithNull)
  }

  // RULES TESTED: 11
  typesWithoutInnerTypesPlusSimpleList.foreach { case (typeName, typeExpr) =>
    test(s"Ordering of the same type with and without NOT NULL: $typeName") {
      // Needs to be in lists with other types to be kept and not normalized away
      // List1: LIST<TYPE | LIST<STRING NOT NULL>>
      // List2: LIST<TYPE NOT NULL | LIST<INTEGER NOT NULL> NOT NULL>
      // List2 should always be sorted before LIST1
      val secondType1 = ListTypeName(StringTypeName(isNullable = false)(pos), isNullable = true)(pos)
      val secondType2 = ListTypeName(IntegerTypeName(isNullable = false)(pos), isNullable = false)(pos)

      val List1 =
        ListTypeName(ClosedDynamicUnionTypeName(Set(typeExpr(true), secondType1))(pos), isNullable = true)(pos)
      val List2 =
        ListTypeName(ClosedDynamicUnionTypeName(Set(typeExpr(false), secondType2))(pos), isNullable = true)(pos)

      val listToSort: List[CypherTypeName] = List(List1, List2)
      if (typeName != "PATH") {
        listToSort.sorted should be(
          List(List2, List1)
        )
      } else {
        // PATH comes after LIST, so ordering sorts on LIST first
        listToSort.sorted should be(
          List(List1, List2)
        )

      }
    }
  }

  // RULES TESTED: 11
  test("PATH NOT NULL should be ordered before PATH") {
    // LIST<BOOLEAN | PATH NOT NULL>
    // LIST<BOOLEAN | PATH>
    val List1 =
      ListTypeName(
        ClosedDynamicUnionTypeName(
          Set(
            BooleanTypeName(isNullable = true)(pos),
            PathTypeName(isNullable = true)(pos)
          )
        )(pos),
        isNullable = true
      )(pos)
    val List2 =
      ListTypeName(
        ClosedDynamicUnionTypeName(
          Set(
            BooleanTypeName(isNullable = true)(pos),
            PathTypeName(isNullable = false)(pos)
          )
        )(pos),
        isNullable = true
      )(pos)

    val listToSort: List[CypherTypeName] = List(List1, List2)
    listToSort.sorted should be(
      List(List2, List1)
    )
  }

  // RULES TESTED: 11
  test("Ordering of Lists of different sizes") {
    // LIST<NODE>
    val list1Type = ListTypeName(NodeTypeName(isNullable = true)(pos), isNullable = true)(pos)
    // LIST<POINT | MAP>
    val list2Type =
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          PointTypeName(isNullable = true)(pos),
          MapTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    // LIST<BOOLEAN | INTEGER | FLOAT>
    val list3Type = ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        BooleanTypeName(isNullable = true)(pos),
        IntegerTypeName(isNullable = true)(pos),
        FloatTypeName(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)
    // LIST<BOOLEAN | STRING | DATE | MAP>
    val list4Type = ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        BooleanTypeName(isNullable = true)(pos),
        StringTypeName(isNullable = true)(pos),
        DateTypeName(isNullable = true)(pos),
        MapTypeName(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)

    val unionOfAllLists = ClosedDynamicUnionTypeName(Set(list1Type, list3Type, list2Type, list4Type))(pos)

    CypherTypeName.normalizeTypes(unionOfAllLists).description should be(
      "LIST<NODE> | LIST<POINT | MAP> | LIST<BOOLEAN | INTEGER | FLOAT> | LIST<BOOLEAN | STRING | DATE | MAP>"
    )
  }

  // RULES TESTED: 11
  test("Ordering of lists with LIST<ANY>, ANY is bigger than any union") {
    // LIST<POINT | MAP>
    val listType =
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          PointTypeName(isNullable = true)(pos),
          MapTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    // LIST<POINT NOT NULL | MAP NOT NULL>
    val listNotNullType =
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          PointTypeName(isNullable = false)(pos),
          MapTypeName(isNullable = false)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    // LIST<ANY NOT NULL>
    val listAnyNotNullType = ListTypeName(AnyTypeName(isNullable = false)(pos), isNullable = true)(pos)
    // LIST<ANY>
    val listAnyType = ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)

    val unionWithNotNullAnyLists = ClosedDynamicUnionTypeName(Set(listType, listAnyNotNullType))(pos)
    CypherTypeName.normalizeTypes(unionWithNotNullAnyLists).description should be(
      "LIST<POINT | MAP> | LIST<ANY NOT NULL>"
    )

    val unionWithAnyLists = ClosedDynamicUnionTypeName(Set(listNotNullType, listAnyType))(pos)
    CypherTypeName.normalizeTypes(unionWithAnyLists).description should be(
      "LIST<POINT NOT NULL | MAP NOT NULL> | LIST<ANY>"
    )
  }

  // Test description

  typesWithoutInnerTypes.foreach { case (typeName, typeExpr) =>
    test(s"$typeName should have correct description without NOT NULL") {
      typeExpr(true).description should be(typeName)
    }

    test(s"$typeName should have correct description with NOT NULL") {
      typeExpr(false).description should be(s"$typeName NOT NULL")
    }

    test(s"LIST<$typeName> should have correct descriptions") {
      ListTypeName(typeExpr(true), isNullable = true)(pos).description should be(s"LIST<$typeName>")
      ListTypeName(typeExpr(false), isNullable = true)(pos).description should be(s"LIST<$typeName NOT NULL>")
      ListTypeName(typeExpr(true), isNullable = false)(pos).description should be(s"LIST<$typeName> NOT NULL")
      ListTypeName(typeExpr(false), isNullable = false)(pos).description should be(s"LIST<$typeName NOT NULL> NOT NULL")
    }

    test(s"ANY<$typeName> should have correct descriptions") {
      ClosedDynamicUnionTypeName(Set(typeExpr(true)))(pos).description should be(s"$typeName")
      ClosedDynamicUnionTypeName(Set(typeExpr(false)))(pos).description should be(s"$typeName NOT NULL")
    }
  }

  test(s"NOTHING should have correct description (no not null)") {
    NothingTypeName()(pos).description should be("NOTHING")
  }

  test(s"LIST<NOTHING> should have correct descriptions") {
    ListTypeName(NothingTypeName()(pos), isNullable = true)(pos).description should be(s"LIST<NOTHING>")
    ListTypeName(NothingTypeName()(pos), isNullable = false)(pos).description should be(s"LIST<NOTHING> NOT NULL")
  }

  test(s"ANY<NOTHING> should have correct descriptions") {
    ClosedDynamicUnionTypeName(Set(NothingTypeName()(pos)))(pos).description should be(s"NOTHING")
  }

  test(s"NULL should have correct description (no not null)") {
    NullTypeName()(pos).description should be("NULL")
  }

  test(s"NULL NOT NULL should have correct description of NOTHING") {
    NullTypeName()(pos).updateIsNullable(false).description should be("NOTHING")
  }

  test(s"LIST<NULL> should have correct descriptions") {
    ListTypeName(NullTypeName()(pos), isNullable = true)(pos).description should be(s"LIST<NULL>")
    ListTypeName(NullTypeName()(pos).updateIsNullable(false), isNullable = true)(pos).description should be(
      s"LIST<NOTHING>"
    )
    ListTypeName(NullTypeName()(pos), isNullable = false)(pos).description should be(s"LIST<NULL> NOT NULL")
    ListTypeName(NullTypeName()(pos).updateIsNullable(false), isNullable = false)(pos).description should be(
      s"LIST<NOTHING> NOT NULL"
    )
  }

  test(s"ANY<NULL> should have correct descriptions") {
    ClosedDynamicUnionTypeName(Set(NullTypeName()(pos)))(pos).description should be(s"NULL")
    ClosedDynamicUnionTypeName(Set(NullTypeName()(pos).updateIsNullable(false)))(pos).description should be(s"NOTHING")
  }

  test("lists with multiple types should have correct descriptions") {
    CypherTypeName.normalizeTypes(ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        BooleanTypeName(isNullable = true)(pos),
        IntegerTypeName(isNullable = true)(pos),
        StringTypeName(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<BOOLEAN | STRING | INTEGER>")

    CypherTypeName.normalizeTypes(ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        DateTypeName(isNullable = false)(pos),
        StringTypeName(isNullable = false)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<STRING NOT NULL | DATE NOT NULL>")

    CypherTypeName.normalizeTypes(ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        DateTypeName(isNullable = false)(pos),
        IntegerTypeName(isNullable = true)(pos),
        StringTypeName(isNullable = false)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<STRING | INTEGER | DATE>")

    CypherTypeName.normalizeTypes(ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        ListTypeName(StringTypeName(isNullable = true)(pos), isNullable = false)(pos),
        ListTypeName(StringTypeName(isNullable = true)(pos), isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<LIST<STRING>>")

    CypherTypeName.normalizeTypes(ListTypeName(
      ClosedDynamicUnionTypeName(Set(
        ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = true)(pos)))(pos),
        ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = true)(pos)))(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<STRING>")
  }

  test("closed dynamic union with multiple types should have correct descriptions") {
    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        BooleanTypeName(isNullable = true)(pos),
        IntegerTypeName(isNullable = true)(pos),
        StringTypeName(isNullable = true)(pos)
      )
    )(pos)).description should be("BOOLEAN | STRING | INTEGER")

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(DateTypeName(isNullable = false)(pos), StringTypeName(isNullable = false)(pos))
    )(pos)).description should be("STRING NOT NULL | DATE NOT NULL")

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        DateTypeName(isNullable = false)(pos),
        IntegerTypeName(isNullable = true)(pos),
        StringTypeName(isNullable = false)(pos)
      )
    )(pos)).description should be("STRING | INTEGER | DATE")

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        ListTypeName(StringTypeName(isNullable = true)(pos), isNullable = false)(pos),
        ListTypeName(StringTypeName(isNullable = false)(pos), isNullable = true)(pos)
      )
    )(pos)).description should be("LIST<STRING>")

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = true)(pos)))(pos),
        ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = false)(pos)))(pos)
      )
    )(pos)).description should be("STRING")

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = true)(pos)))(pos),
        ClosedDynamicUnionTypeName(Set(StringTypeName(isNullable = false)(pos)))(pos)
      )
    )(pos)).simplify.description should be("STRING")
  }

  test("Ordering of different lists should be correct in descriptions") {
    val list1 = ListTypeName(BooleanTypeName(isNullable = true)(pos), isNullable = true)(pos)
    val list2 =
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          BooleanTypeName(isNullable = true)(pos),
          StringTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    val list3 = ListTypeName(IntegerTypeName(isNullable = true)(pos), isNullable = true)(pos)
    val list4 =
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(
          FloatTypeName(isNullable = true)(pos),
          StringTypeName(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    val notAList = DateTypeName(isNullable = true)(pos)

    CypherTypeName.normalizeTypes(ListTypeName(
      ClosedDynamicUnionTypeName(Set(list1, list2, list3, list4, notAList))(pos),
      isNullable = true
    )(pos)).description should be(
      "LIST<DATE | LIST<INTEGER> | LIST<BOOLEAN | STRING> | LIST<STRING | FLOAT>>"
    )

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(
        ListTypeName(ClosedDynamicUnionTypeName(Set(list1, list4))(pos), isNullable = true)(pos),
        notAList,
        list3,
        list2
      )
    )(pos)).description should be(
      "DATE | LIST<INTEGER> | LIST<BOOLEAN | STRING> | LIST<LIST<BOOLEAN> | LIST<STRING | FLOAT>>"
    )
  }

  test("Ordering of different closed dynamic unions should be correct in descriptions") {
    val union1 = ClosedDynamicUnionTypeName(Set(BooleanTypeName(isNullable = true)(pos)))(pos)
    val union2 = ClosedDynamicUnionTypeName(Set(
      BooleanTypeName(isNullable = true)(pos),
      StringTypeName(isNullable = true)(pos)
    ))(pos)
    val union3 = ClosedDynamicUnionTypeName(Set(IntegerTypeName(isNullable = true)(pos)))(pos)
    val union4 = ClosedDynamicUnionTypeName(Set(
      FloatTypeName(isNullable = true)(pos),
      StringTypeName(isNullable = true)(pos)
    ))(pos)
    val notAUnion = DateTypeName(isNullable = true)(pos)

    CypherTypeName.normalizeTypes(
      ListTypeName(
        ClosedDynamicUnionTypeName(Set(union1, union2, union3, union4, notAUnion))(pos),
        isNullable = true
      )(pos)
    ).description should be("LIST<BOOLEAN | STRING | INTEGER | FLOAT | DATE>")

    CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(
      Set(ClosedDynamicUnionTypeName(Set(union1, union4))(pos), notAUnion, union3, union2)
    )(pos)).description should be("BOOLEAN | STRING | INTEGER | FLOAT | DATE")
  }

  // Rule combination tests
  // For more in depth testing of each individual type see above tests.

  // Use a second closedDynamicUnion as a set doesn't need to be tested
  private val rule1 = ClosedDynamicUnionTypeName(
    Set(
      BooleanTypeName(isNullable = true)(pos),
      ClosedDynamicUnionTypeName(Set(BooleanTypeName(isNullable = true)(pos)))(pos)
    )
  )(pos)
  private val rule1Normalized = BooleanTypeName(isNullable = true)(pos)

  private val rule2 = NullTypeName()(pos).updateIsNullable(false)
  private val rule2Normalized = NothingTypeName()(pos)

  private val rule3 = ClosedDynamicUnionTypeName(
    Set(
      StringTypeName(isNullable = true)(pos),
      NothingTypeName()(pos)
    )
  )(pos)
  private val rule3Normalized = StringTypeName(isNullable = true)(pos)

  private val rule4 = ClosedDynamicUnionTypeName(
    Set(
      IntegerTypeName(isNullable = true)(pos),
      NullTypeName()(pos)
    )
  )(pos)
  private val rule4Normalized = IntegerTypeName(isNullable = true)(pos)

  private val rule5 = ClosedDynamicUnionTypeName(
    Set(
      IntegerTypeName(isNullable = true)(pos),
      FloatTypeName(isNullable = false)(pos)
    )
  )(pos)

  private val rule5Normalized = ClosedDynamicUnionTypeName(
    Set(
      IntegerTypeName(isNullable = true)(pos),
      FloatTypeName(isNullable = true)(pos)
    )
  )(pos)

  private val rule6 = ClosedDynamicUnionTypeName(
    Set(
      ClosedDynamicUnionTypeName(Set(NodeTypeName(isNullable = false)(pos)))(pos)
    )
  )(pos)
  private val rule6Normalized = NodeTypeName(isNullable = false)(pos)

  private val rule7 = ClosedDynamicUnionTypeName(setOfAllPropertyTypes ++ Set(
    NodeTypeName(isNullable = true)(pos),
    RelationshipTypeName(isNullable = true)(pos),
    MapTypeName(isNullable = true)(pos),
    PathTypeName(isNullable = true)(pos),
    ListTypeName(AnyTypeName(isNullable = true)(pos), isNullable = true)(pos)
  ))(pos)
  private val rule7Normalized = AnyTypeName(isNullable = true)(pos)

  private val rule8 = ClosedDynamicUnionTypeName(
    Set(
      IntegerTypeName(isNullable = true)(pos),
      FloatTypeName(isNullable = true)(pos),
      AnyTypeName(isNullable = true)(pos)
    )
  )(pos)
  private val rule8Normalized = AnyTypeName(isNullable = true)(pos)

  private val rule9 = PropertyValueTypeName(isNullable = true)(pos)
  private val rule9Normalized = ClosedDynamicUnionTypeName(setOfAllPropertyTypes)(pos)

  private val rule10 = ClosedDynamicUnionTypeName(Set(
    ListTypeName(IntegerTypeName(isNullable = true)(pos), isNullable = true)(pos),
    ListTypeName(
      ClosedDynamicUnionTypeName(
        Set(
          IntegerTypeName(isNullable = true)(pos),
          FloatTypeName(isNullable = true)(pos)
        )
      )(pos),
      isNullable = true
    )(pos)
  ))(pos)

  private val rule10Normalized = ListTypeName(
    ClosedDynamicUnionTypeName(
      Set(
        IntegerTypeName(isNullable = true)(pos),
        FloatTypeName(isNullable = true)(pos)
      )
    )(pos),
    isNullable = true
  )(pos)

  private def checkRulesNormalizeTheSame(
    listRules: Set[CypherTypeName],
    listNormalizedRules: Set[CypherTypeName]
  ): Unit = {
    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(listRules)(pos)
    ) should be(CypherTypeName.normalizeTypes(ClosedDynamicUnionTypeName(listNormalizedRules)(pos)))
  }

  // RULES TESTED: 1,2,3
  test("RULES 1, 2 and 3") {
    // ANY<BOOLEAN | ANY<BOOLEAN>> | NULL NOT NULL | ANY<STRING | NOTHING>
    // BOOLEAN | NOTHING | STRING
    // BOOLEAN | STRING
    checkRulesNormalizeTheSame(
      Set(rule1, rule2, rule3),
      Set(rule1Normalized, rule2Normalized, rule3Normalized)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(rule1, rule2, rule3))(pos)
    ).description should be("BOOLEAN | STRING")
  }

  // RULES TESTED: 4,5,6
  test("RULES 4, 5 and 6") {
    // ANY<INTEGER | NULL> | ANY<INTEGER | FLOAT NOT NULL> | ANY<ANY<NODE NOT NULL>>
    // INTEGER | ANY<INTEGER | FLOAT> | NODE NOT NULL
    // INTEGER | FLOAT | NODE
    checkRulesNormalizeTheSame(
      Set(rule4, rule5, rule6),
      Set(rule4Normalized, rule5Normalized, rule6Normalized)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(rule4, rule5, rule6))(pos)
    ).description should be("INTEGER | FLOAT | NODE")
  }

  // RULES TESTED: 7,8,9,10
  test("RULES 7, 8, 9 and 10") {
    // ANY<BOOLEAN …. LIST<ANY>> | ANY<INTEGER | FLOAT | ANY> | PROPERTY VALUE | ANY<LIST<INTEGER> | LIST<INTEGER | FLOAT>>
    // ANY | ANY | BOOLEAN …. LIST<POINT NOT NULL> | LIST<INTEGER | FLOAT>
    // ANY
    checkRulesNormalizeTheSame(
      Set(rule7, rule8, rule9, rule10),
      Set(rule7Normalized, rule8Normalized, rule9Normalized, rule10Normalized)
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(rule7, rule8, rule9, rule10))(pos)
    ).description should be("ANY")
  }

  // RULES TESTED: 1,2,3,4,5,6,7,8,9,10
  test("RULES 1, 2, 3, 4, 5, 6, 7, 8, 9 and 10") {
    // 1. ANY<BOOLEAN | ANY<BOOLEAN>> = BOOLEAN
    // 2. NULL NOT NULL = NOTHING
    // 3. ANY<STRING | NOTHING> = NOTHING
    // 4. ANY<INTEGER | NULL> = INTEGER
    // 5. ANY<INTEGER | FLOAT NOT NULL> = ANY<INTEGER | FLOAT>
    // 6. ANY<ANY<NODE NOT NULL>> = NODE NOT NULL
    // 7. ANY<BOOLEAN …. LIST<ANY>> = ANY
    // 8. ANY<INTEGER | FLOAT | ANY> = ANY
    // 9. PROPERTY VALUE = BOOLEAN …. LIST<POINT NOT NULL>
    // 10. ANY<LIST<INTEGER> | LIST<INTEGER | FLOAT>> = LIST<INTEGER | FLOAT>
    // Should normalized to: ANY
    checkRulesNormalizeTheSame(
      Set(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10),
      Set(
        rule1Normalized,
        rule2Normalized,
        rule3Normalized,
        rule4Normalized,
        rule5Normalized,
        rule6Normalized,
        rule7Normalized,
        rule8Normalized,
        rule9Normalized,
        rule10Normalized
      )
    )

    CypherTypeName.normalizeTypes(
      ClosedDynamicUnionTypeName(Set(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10))(pos)
    ).description should be("ANY")
  }

}
