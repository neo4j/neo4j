/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory

import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.NullType
import org.neo4j.cypher.internal.util.symbols.PathType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
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

  private val setOfAllPropertyTypes: Set[CypherType] = Set(
    BooleanType(isNullable = true)(pos),
    StringType(isNullable = true)(pos),
    IntegerType(isNullable = true)(pos),
    FloatType(isNullable = true)(pos),
    DateType(isNullable = true)(pos),
    LocalTimeType(isNullable = true)(pos),
    ZonedTimeType(isNullable = true)(pos),
    LocalDateTimeType(isNullable = true)(pos),
    ZonedDateTimeType(isNullable = true)(pos),
    DurationType(isNullable = true)(pos),
    PointType(isNullable = true)(pos),
    ListType(BooleanType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(StringType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(DateType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(LocalTimeType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(ZonedTimeType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(LocalDateTimeType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(ZonedDateTimeType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(DurationType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(PointType(isNullable = false)(pos), isNullable = true)(pos),
    ListType(
      ClosedDynamicUnionType(Set(
        IntegerType(isNullable = false)(pos),
        FloatType(isNullable = false)(pos)
      ))(pos),
      isNullable = true
    )(pos)
  )

  private def normalizeTypeList(types: Set[CypherType]): List[CypherType] = {
    val closedDynamicUnionType: ClosedDynamicUnionType = ClosedDynamicUnionType(types)(pos)
    val normalizedType = CypherType.normalizeTypes(closedDynamicUnionType)

    normalizedType match {
      case cut: ClosedDynamicUnionType => cut.sortedInnerTypes
      case _                           => List(normalizedType)
    }
  }

  private val typesWithoutInnerTypes: List[(String, Boolean => CypherType)] =
    List[(String, Boolean => CypherType)](
      ("BOOLEAN", BooleanType(_)(pos)),
      ("STRING", StringType(_)(pos)),
      ("INTEGER", IntegerType(_)(pos)),
      ("FLOAT", FloatType(_)(pos)),
      ("DATE", DateType(_)(pos)),
      ("LOCAL TIME", LocalTimeType(_)(pos)),
      ("ZONED TIME", ZonedTimeType(_)(pos)),
      ("LOCAL DATETIME", LocalDateTimeType(_)(pos)),
      ("ZONED DATETIME", ZonedDateTimeType(_)(pos)),
      ("DURATION", DurationType(_)(pos)),
      ("POINT", PointType(_)(pos)),
      ("NODE", NodeType(_)(pos)),
      ("RELATIONSHIP", RelationshipType(_)(pos)),
      ("MAP", MapType(_)(pos)),
      ("PATH", PathType(_)(pos))
    )

  private val typesWithoutInnerTypesPlusSimpleList: Seq[(String, Boolean => CypherType)] =
    typesWithoutInnerTypes.dropRight(1) ++ List(
      ("LIST<BOOLEAN>", ListType(BooleanType(isNullable = true)(pos), _)(pos)),
      ("PATH", PathType(_)(pos))
    )

  private val typesWithoutInnerTypesPlusSimpleListPlusAny: Seq[(String, Boolean => CypherType)] =
    typesWithoutInnerTypesPlusSimpleList ++ List(
      ("ANY", AnyType(_)(pos))
    )

  // Test updateIsNullable
  // RULES TESTED: 2
  test("NULL update nullable false should return NOTHING") {
    val nullType = NullType()(pos)
    val nothingTypeName = NothingType()(pos)
    nullType.withIsNullable(false) should be(nothingTypeName)
  }

  test("NULL update nullable true should return NULL") {
    val nullType = NullType()(pos)
    nullType.withIsNullable(true) should be(nullType)
  }

  // RULES TESTED: 2
  test("NOTHING update nullable false should return NOTHING") {
    val nothingTypeName = NothingType()(pos)
    nothingTypeName.withIsNullable(false) should be(nothingTypeName)
  }

  test("NOTHING update nullable true should return NOTHING") {
    val nothingTypeName = NothingType()(pos)
    nothingTypeName.withIsNullable(true) should be(nothingTypeName)
  }

  test("ANY<> update nullable false should return ANY<> with all values changed") {
    val closedDynamicUnionType = ClosedDynamicUnionType(Set(
      IntegerType(isNullable = true)(pos),
      BooleanType(isNullable = false)(pos)
    ))(pos)
    val closedDynamicUnionTypeUpdated = ClosedDynamicUnionType(Set(
      IntegerType(isNullable = false)(pos),
      BooleanType(isNullable = false)(pos)
    ))(pos)
    closedDynamicUnionType.withIsNullable(false) should be(closedDynamicUnionTypeUpdated)
  }

  test("ANY<> update nullable true should return ANY<> with all values changed") {
    val closedDynamicUnionType = ClosedDynamicUnionType(Set(
      IntegerType(isNullable = true)(pos),
      BooleanType(isNullable = false)(pos)
    ))(pos)
    val closedDynamicUnionTypeUpdated = ClosedDynamicUnionType(Set(
      IntegerType(isNullable = true)(pos),
      BooleanType(isNullable = true)(pos)
    ))(pos)
    closedDynamicUnionType.withIsNullable(true) should be(closedDynamicUnionTypeUpdated)
  }

  typesWithoutInnerTypesPlusSimpleListPlusAny.foreach { case (typeName, typeExpr) =>
    test(s"$typeName update nullable true should return that type as nullable") {
      val expr = typeExpr(false)
      expr.withIsNullable(true) should be(typeExpr(true))
    }

    test(s"$typeName update nullable false should return that type as not nullable") {
      val expr = typeExpr(true)
      expr.withIsNullable(false) should be(typeExpr(false))
    }
  }

  // Test simplify and type normalization

  test("NOTHING should simplify to itself") {
    val expr = NothingType()(pos)
    expr.simplify should be(expr)
  }

  test("NULL should simplify to itself") {
    val expr = NullType()(pos)
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
      val exprInnerNotNull = ListType(typeExpr(false), isNullable = true)(pos)
      exprInnerNotNull.simplify should be(exprInnerNotNull)

      val exprAllNull = ListType(typeExpr(true), isNullable = true)(pos)
      exprAllNull.simplify should be(exprAllNull)

      val exprNoNull = ListType(typeExpr(false), isNullable = false)(pos)
      exprNoNull.simplify should be(exprNoNull)

      val exprInnerNull = ListType(typeExpr(true), isNullable = false)(pos)
      exprInnerNull.simplify should be(exprInnerNull)
    }

    test(s"ANY<$typeName> should simplify to $typeName") {
      ClosedDynamicUnionType(Set(typeExpr(false)))(pos).simplify should be(typeExpr(false))

      ClosedDynamicUnionType(Set(typeExpr(true)))(pos).simplify should be(typeExpr(true))
    }
  }

  // RULES TESTED: 5
  test("ANY<t1 NOT NULL, t2, ...> should make all inner types nullable") {
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      IntegerType(isNullable = true)(pos),
      DateType(isNullable = false)(pos),
      StringType(isNullable = true)(pos),
      BooleanType(isNullable = true)(pos),
      StringType(isNullable = false)(pos),
      DurationType(isNullable = false)(pos)
    ))(pos)) should be(ClosedDynamicUnionType(Set(
      BooleanType(isNullable = true)(pos),
      StringType(isNullable = true)(pos),
      IntegerType(isNullable = true)(pos),
      DateType(isNullable = true)(pos),
      DurationType(isNullable = true)(pos)
    ))(pos))
  }

  // RULES TESTED: 3
  test("NOTHING should be absorbed if any other type is present") {
    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        NothingType()(pos),
        NullType()(pos)
      ))(pos)
    ) should be(
      NullType()(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        NothingType()(pos),
        BooleanType(isNullable = true)(pos)
      ))(pos)
    ) should be(
      BooleanType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        NothingType()(pos),
        ClosedDynamicUnionType(Set(IntegerType(isNullable = false)(pos)))(pos)
      ))(pos)
    ) should be(
      IntegerType(isNullable = false)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        NothingType()(pos),
        ListType(
          ClosedDynamicUnionType(Set(NothingType()(pos), IntegerType(isNullable = false)(pos)))(pos),
          isNullable = true
        )(pos)
      ))(pos)
    ) should be(
      ListType(IntegerType(isNullable = false)(pos), isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        NothingType()(pos),
        IntegerType(isNullable = true)(pos),
        FloatType(isNullable = true)(pos),
        ListType(NothingType()(pos), isNullable = true)(pos)
      ))(pos)
    ) should be(
      ClosedDynamicUnionType(Set(
        IntegerType(isNullable = true)(pos),
        FloatType(isNullable = true)(pos),
        ListType(NothingType()(pos), isNullable = true)(pos)
      ))(pos)
    )
  }

  // RULES TESTED: 4
  test("NULL should be absorbed if any other type is present (excl. NOTHING)") {
    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        BooleanType(isNullable = true)(pos),
        NullType()(pos)
      ))(pos)
    ) should be(
      BooleanType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        BooleanType(isNullable = false)(pos),
        NullType()(pos)
      ))(pos)
    ) should be(
      BooleanType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        ClosedDynamicUnionType(Set(BooleanType(isNullable = false)(pos)))(pos),
        NullType()(pos)
      ))(pos)
    ) should be(
      BooleanType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        NullType()(pos),
        ListType(
          ClosedDynamicUnionType(Set(NullType()(pos), IntegerType(isNullable = false)(pos)))(pos),
          isNullable = false
        )(pos)
      ))(pos)
    ) should be(
      ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos)
    )
  }

  // RULES TESTED: 8
  test("Any absorbs all other types") {
    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        AnyType(isNullable = true)(pos),
        BooleanType(isNullable = true)(pos)
      ))(pos)
    ) should be(
      AnyType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        AnyType(isNullable = false)(pos),
        PropertyValueType(isNullable = true)(pos)
      ))(pos)
    ) should be(
      AnyType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        AnyType(isNullable = false)(pos),
        ListType(BooleanType(isNullable = false)(pos), isNullable = true)(pos),
        ClosedDynamicUnionType(Set(StringType(isNullable = false)(pos)))(pos)
      ))(pos)
    ) should be(
      AnyType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ListType(
        ClosedDynamicUnionType(Set(
          AnyType(isNullable = true)(pos),
          BooleanType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ) should be(
      ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    )
  }

  // RULES TESTED: 7
  test("If all types are present, they should normalize to ANY") {
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      PropertyValueType(isNullable = true)(pos),
      NodeType(isNullable = true)(pos),
      RelationshipType(isNullable = true)(pos),
      MapType(isNullable = true)(pos),
      PathType(isNullable = true)(pos),
      ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      AnyType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      PropertyValueType(isNullable = false)(pos),
      NodeType(isNullable = false)(pos),
      RelationshipType(isNullable = false)(pos),
      MapType(isNullable = false)(pos),
      PathType(isNullable = false)(pos),
      ListType(AnyType(isNullable = true)(pos), isNullable = false)(pos)
    ))(pos)) should be(
      AnyType(isNullable = false)(pos)
    )

    CypherType.normalizeTypes(ClosedDynamicUnionType(setOfAllPropertyTypes ++ Set(
      NodeType(isNullable = true)(pos),
      RelationshipType(isNullable = true)(pos),
      MapType(isNullable = true)(pos),
      PathType(isNullable = true)(pos),
      ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      AnyType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(setOfAllPropertyTypes.map(_.withIsNullable(false)) ++ Set(
        NodeType(isNullable = false)(pos),
        RelationshipType(isNullable = false)(pos),
        MapType(isNullable = false)(pos),
        PathType(isNullable = false)(pos),
        ListType(AnyType(isNullable = true)(pos), isNullable = false)(pos)
      ))(pos)
    ) should be(
      AnyType(isNullable = false)(pos)
    )

    CypherType.normalizeTypes(ClosedDynamicUnionType(setOfAllPropertyTypes ++ Set(
      NodeType(isNullable = true)(pos),
      RelationshipType(isNullable = true)(pos),
      MapType(isNullable = true)(pos),
      PathType(isNullable = true)(pos),
      ListType(
        ClosedDynamicUnionType(setOfAllPropertyTypes ++ Set(
          NodeType(isNullable = true)(pos),
          RelationshipType(isNullable = true)(pos),
          MapType(isNullable = true)(pos),
          PathType(isNullable = true)(pos),
          ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(
      AnyType(isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ListType(
        ClosedDynamicUnionType(setOfAllPropertyTypes ++ Set(
          NodeType(isNullable = true)(pos),
          RelationshipType(isNullable = true)(pos),
          MapType(isNullable = true)(pos),
          PathType(isNullable = true)(pos),
          ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ) should be(
      ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    )
  }

  // RULES TESTED: 9
  test("Property value type normalizes to all storable types") {
    CypherType.normalizeTypes(PropertyValueType(isNullable = true)(pos)) should be(
      ClosedDynamicUnionType(setOfAllPropertyTypes)(pos)
    )

    CypherType.normalizeTypes(PropertyValueType(isNullable = false)(pos)) should be(
      ClosedDynamicUnionType(setOfAllPropertyTypes.map(_.withIsNullable(false)))(pos)
    )

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        PropertyValueType(isNullable = true)(pos),
        BooleanType(isNullable = true)(pos),
        NodeType(isNullable = true)(pos)
      )
    )(pos)) should be(
      ClosedDynamicUnionType(setOfAllPropertyTypes ++ Set(NodeType(isNullable = true)(pos)))(pos)
    )
  }

  // RULES TESTED: 6
  test("closed dynamic unions should be flattened when inside lists") {
    val union1 = ClosedDynamicUnionType(Set(
      BooleanType(isNullable = false)(pos),
      StringType(isNullable = true)(pos)
    ))(pos)
    val union2 = ClosedDynamicUnionType(Set(
      IntegerType(isNullable = false)(pos),
      ClosedDynamicUnionType(Set(StringType(isNullable = false)(pos)))(pos)
    ))(pos)
    val union3 = ClosedDynamicUnionType(Set(
      ClosedDynamicUnionType(Set(
        ClosedDynamicUnionType(Set(
          ClosedDynamicUnionType(Set(DateType(isNullable = true)(pos)))(pos)
        ))(pos),
        DurationType(isNullable = true)(pos)
      ))(pos)
    ))(pos)

    CypherType.normalizeTypes(ListType(
      ClosedDynamicUnionType(Set(union1, union2, union3))(pos),
      isNullable = true
    )(
      pos
    )) should be(
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          StringType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos),
          DateType(isNullable = true)(pos),
          DurationType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    )
  }

  // RULES TESTED: 6
  test("closed dynamic unions should be flattened when mixed with other types") {
    val union1 = ClosedDynamicUnionType(Set(BooleanType(isNullable = false)(pos)))(pos)
    val union2 = ClosedDynamicUnionType(Set(
      BooleanType(isNullable = false)(pos),
      StringType(isNullable = false)(pos)
    ))(pos)
    val union3 = ClosedDynamicUnionType(Set(IntegerType(isNullable = false)(pos)))(pos)
    val union4 = ClosedDynamicUnionType(Set(
      FloatType(isNullable = false)(pos),
      StringType(isNullable = false)(pos)
    ))(pos)
    val union14 = ClosedDynamicUnionType(Set(union1, union4))(pos)
    val notAUnionDate = DateType(isNullable = false)(pos)
    val notAUnionList =
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = false)(pos),
          BooleanType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)

    val inputTypeSet = Set[CypherType](union3, notAUnionList, union2, notAUnionDate, union14)
    val outputTypeSet = Set[CypherType](
      BooleanType(isNullable = true)(pos),
      StringType(isNullable = true)(pos),
      IntegerType(isNullable = true)(pos),
      FloatType(isNullable = true)(pos),
      DateType(isNullable = true)(pos),
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos)
    )

    CypherType.normalizeTypes(
      ListType(ClosedDynamicUnionType(inputTypeSet)(pos), isNullable = false)(pos)
    ) should be(
      ListType(ClosedDynamicUnionType(outputTypeSet)(pos), isNullable = false)(pos)
    )

    CypherType.normalizeTypes(ClosedDynamicUnionType(inputTypeSet)(pos)) should be(
      ClosedDynamicUnionType(outputTypeSet)(pos)
    )
  }

  // RULES TESTED: 6, 10
  test("Lists that are encapsulated fully by another list should be removed") {
    // LIST<BOOLEAN> | LIST<BOOLEAN> == LIST<BOOLEAN>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | ANY<ANY<LIST<ANY<BOOLEAN>>>> == LIST<BOOLEAN>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      ClosedDynamicUnionType(Set(
        ClosedDynamicUnionType(Set(
          ListType(
            ClosedDynamicUnionType(Set(
              BooleanType(isNullable = true)(pos)
            ))(pos),
            isNullable = true
          )(pos)
        ))(pos)
      ))(pos)
    ))(pos)) should be(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> == LIST<BOOLEAN | INTEGER>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    )

    // LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> | LIST<BOOLEAN | INTEGER | FLOAT> == LIST<BOOLEAN | INTEGER | FLOAT>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos),
          FloatType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos),
          FloatType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    )

    // LIST<BOOLEAN> | LIST<BOOLEAN NOT NULL> == LIST<BOOLEAN>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(BooleanType(isNullable = false)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | LIST<ANY> == LIST<ANY>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<BOOLEAN> | LIST<NULL> == LIST<BOOLEAN>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(NullType()(pos), isNullable = true)(pos)
    ))(pos)) should be(
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos)
    )

    // LIST<STRING> NOT NULL | LIST<STRING NOT NULL> == LIST<STRING>
    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        ListType(StringType(isNullable = true)(pos), isNullable = false)(pos),
        ListType(StringType(isNullable = false)(pos), isNullable = true)(pos)
      )
    )(pos)) should be(
      ListType(StringType(isNullable = true)(pos), isNullable = true)(pos)
    )

    // Not normalized: LIST<BOOLEAN NOT NULL> | LIST<NULL>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = false)(pos), isNullable = true)(pos),
      ListType(NullType()(pos), isNullable = true)(pos)
    ))(pos)) should be(ClosedDynamicUnionType(Set(
      ListType(BooleanType(isNullable = false)(pos), isNullable = true)(pos),
      ListType(NullType()(pos), isNullable = true)(pos)
    ))(pos))

    // Not completely overlapping types, not normalized: LIST<BOOLEAN | INTEGER> | LIST<BOOLEAN | FLOAT>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          FloatType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(ClosedDynamicUnionType(Set(
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          FloatType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos))

    // Nullable unions: LIST<BOOLEAN NOT NULL | INTEGER NOT NULL> | LIST<BOOLEAN | INTEGER> = LIST<BOOLEAN | INTEGER>
    CypherType.normalizeTypes(ClosedDynamicUnionType(Set(
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = false)(pos),
          IntegerType(isNullable = false)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos)) should be(ListType(
      ClosedDynamicUnionType(Set(
        BooleanType(isNullable = true)(pos),
        IntegerType(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos))

    // Nullable unions: LIST<INTEGER NOT NULL> | LIST<INTEGER | FLOAT> = LIST<INTEGER | FLOAT>
    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        ListType(IntegerType(isNullable = false)(pos), isNullable = true)(pos),
        ListType(
          ClosedDynamicUnionType(Set(
            IntegerType(isNullable = true)(pos),
            FloatType(isNullable = true)(pos)
          ))(pos),
          isNullable = true
        )(pos)
      ))(pos)
    ) should be(ListType(
      ClosedDynamicUnionType(Set(
        IntegerType(isNullable = true)(pos),
        FloatType(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos))

    // Nullable unions: LIST<INTEGER> | LIST<INTEGER | FLOAT> = LIST<INTEGER | FLOAT>
    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos),
        ListType(
          ClosedDynamicUnionType(Set(
            IntegerType(isNullable = true)(pos),
            FloatType(isNullable = true)(pos)
          ))(pos),
          isNullable = true
        )(pos)
      ))(pos)
    ) should be(ListType(
      ClosedDynamicUnionType(Set(
        IntegerType(isNullable = true)(pos),
        FloatType(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos))

    // Nullable unions: LIST<INTEGER> | LIST<INTEGER NOT NULL | FLOAT NOT NULL> = LIST<INTEGER> | LIST<INTEGER NOT NULL | FLOAT NOT NULL>
    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos),
        ListType(
          ClosedDynamicUnionType(Set(
            IntegerType(isNullable = false)(pos),
            FloatType(isNullable = false)(pos)
          ))(pos),
          isNullable = true
        )(pos)
      ))(pos)
    ) should be(ClosedDynamicUnionType(Set(
      ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(
        ClosedDynamicUnionType(Set(
          IntegerType(isNullable = false)(pos),
          FloatType(isNullable = false)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ))(pos))
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
        ListType(
          ClosedDynamicUnionType(Set(
            typeExpr(true),
            ClosedDynamicUnionType(Set(typeExpr(true)))(pos)
          ))(pos),
          isNullable = true
        )(pos)
      )) should be(
        List(ListType(typeExpr(true), isNullable = true)(pos))
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
    val union = ClosedDynamicUnionType(Set(
      BooleanType(isNullable = false)(pos),
      StringType(isNullable = false)(pos)
    ))(pos)

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(
        union,
        ClosedDynamicUnionType(Set(union))(pos)
      ))(pos)
    ) should be(union)
  }

  // Test sorting
  // RULES TESTED: 11
  test("Single types should be ordered correctly") {
    // NOTHING, NULL, BOOLEAN NOT NULL, BOOLEAN, ... (remove PATH as it comes after list and add back in later)
    val noInnerTypes: List[CypherType] =
      List(NothingType()(pos), NullType()(pos)) ++ typesWithoutInnerTypes.dropRight(1).flatMap {
        case (_, typeExpr) => List(typeExpr(false), typeExpr(true))
      }
    // LIST<NOTHING> NOT NULL, LIST<NOTHING>, LIST<NULL> NOT NULL, LIST<NULL>, LIST<BOOLEAN NOT NULL> NOT NULL,  ...
    val singleTypeLists = noInnerTypes.flatMap(type_ => {
      List(
        ListType(type_, isNullable = false)(pos),
        ListType(type_, isNullable = true)(pos)
      )
    })

    val singleTypePathAndAnyLists = List(
      ListType(PathType(isNullable = false)(pos), isNullable = false)(pos),
      ListType(PathType(isNullable = false)(pos), isNullable = true)(pos),
      ListType(PathType(isNullable = true)(pos), isNullable = false)(pos),
      ListType(PathType(isNullable = true)(pos), isNullable = true)(pos),
      ListType(AnyType(isNullable = false)(pos), isNullable = false)(pos),
      ListType(AnyType(isNullable = false)(pos), isNullable = true)(pos),
      ListType(AnyType(isNullable = true)(pos), isNullable = false)(pos),
      ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    )
    // LIST<LIST<NOTHING> NOT NULL> NOT NULL, LIST<LIST<NOTHING> NOT NULL>, LIST<LIST<NOTHING>> NOT NULL, ...
    val listOfSingleTypeLists = (singleTypeLists ++ singleTypePathAndAnyLists).flatMap(type_ => {
      List(
        ListType(type_, isNullable = false)(pos),
        ListType(type_, isNullable = true)(pos)
      )
    }) ++ singleTypePathAndAnyLists
    // all of the above
    val allNonUnionSingleTypes =
      noInnerTypes ++ singleTypeLists ++ listOfSingleTypeLists ++ List(
        PathType(isNullable = false)(pos),
        PathType(isNullable = true)(pos)
      )
    // ANY<NOTHING>, ANY<NULL>, ANY<BOOLEAN NOT NULL>, ANY<BOOLEAN>, ...
    val unionsOfSingleType = allNonUnionSingleTypes.map(t => ClosedDynamicUnionType(Set(t))(pos))

    // All single types
    val all = allNonUnionSingleTypes ++ unionsOfSingleType ++ List(
      AnyType(isNullable = false)(pos),
      AnyType(isNullable = true)(pos)
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
      val secondType1 = ListType(StringType(isNullable = false)(pos), isNullable = true)(pos)
      val secondType2 = ListType(IntegerType(isNullable = false)(pos), isNullable = false)(pos)

      val List1 =
        ListType(ClosedDynamicUnionType(Set(typeExpr(true), secondType1))(pos), isNullable = true)(pos)
      val List2 =
        ListType(ClosedDynamicUnionType(Set(typeExpr(false), secondType2))(pos), isNullable = true)(pos)

      val listToSort: List[CypherType] = List(List1, List2)
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
      ListType(
        ClosedDynamicUnionType(
          Set(
            BooleanType(isNullable = true)(pos),
            PathType(isNullable = true)(pos)
          )
        )(pos),
        isNullable = true
      )(pos)
    val List2 =
      ListType(
        ClosedDynamicUnionType(
          Set(
            BooleanType(isNullable = true)(pos),
            PathType(isNullable = false)(pos)
          )
        )(pos),
        isNullable = true
      )(pos)

    val listToSort: List[CypherType] = List(List1, List2)
    listToSort.sorted should be(
      List(List2, List1)
    )
  }

  // RULES TESTED: 11
  test("Ordering of Lists of different sizes") {
    // LIST<NODE>
    val list1Type = ListType(NodeType(isNullable = true)(pos), isNullable = true)(pos)
    // LIST<POINT | MAP>
    val list2Type =
      ListType(
        ClosedDynamicUnionType(Set(
          PointType(isNullable = true)(pos),
          MapType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    // LIST<BOOLEAN | INTEGER | FLOAT>
    val list3Type = ListType(
      ClosedDynamicUnionType(Set(
        BooleanType(isNullable = true)(pos),
        IntegerType(isNullable = true)(pos),
        FloatType(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)
    // LIST<BOOLEAN | STRING | DATE | MAP>
    val list4Type = ListType(
      ClosedDynamicUnionType(Set(
        BooleanType(isNullable = true)(pos),
        StringType(isNullable = true)(pos),
        DateType(isNullable = true)(pos),
        MapType(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)

    val unionOfAllLists = ClosedDynamicUnionType(Set(list1Type, list3Type, list2Type, list4Type))(pos)

    CypherType.normalizeTypes(unionOfAllLists).description should be(
      "LIST<NODE> | LIST<POINT | MAP> | LIST<BOOLEAN | INTEGER | FLOAT> | LIST<BOOLEAN | STRING | DATE | MAP>"
    )
  }

  // RULES TESTED: 11
  test("Ordering of lists with LIST<ANY>, ANY is bigger than any union") {
    // LIST<POINT | MAP>
    val listType =
      ListType(
        ClosedDynamicUnionType(Set(
          PointType(isNullable = true)(pos),
          MapType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    // LIST<POINT NOT NULL | MAP NOT NULL>
    val listNotNullType =
      ListType(
        ClosedDynamicUnionType(Set(
          PointType(isNullable = false)(pos),
          MapType(isNullable = false)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    // LIST<ANY NOT NULL>
    val listAnyNotNullType = ListType(AnyType(isNullable = false)(pos), isNullable = true)(pos)
    // LIST<ANY>
    val listAnyType = ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)

    val unionWithNotNullAnyLists = ClosedDynamicUnionType(Set(listType, listAnyNotNullType))(pos)
    CypherType.normalizeTypes(unionWithNotNullAnyLists).description should be(
      "LIST<POINT | MAP> | LIST<ANY NOT NULL>"
    )

    val unionWithAnyLists = ClosedDynamicUnionType(Set(listNotNullType, listAnyType))(pos)
    CypherType.normalizeTypes(unionWithAnyLists).description should be(
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
      ListType(typeExpr(true), isNullable = true)(pos).description should be(s"LIST<$typeName>")
      ListType(typeExpr(false), isNullable = true)(pos).description should be(s"LIST<$typeName NOT NULL>")
      ListType(typeExpr(true), isNullable = false)(pos).description should be(s"LIST<$typeName> NOT NULL")
      ListType(typeExpr(false), isNullable = false)(pos).description should be(s"LIST<$typeName NOT NULL> NOT NULL")
    }

    test(s"ANY<$typeName> should have correct descriptions") {
      ClosedDynamicUnionType(Set(typeExpr(true)))(pos).description should be(s"$typeName")
      ClosedDynamicUnionType(Set(typeExpr(false)))(pos).description should be(s"$typeName NOT NULL")
    }
  }

  test(s"NOTHING should have correct description (no not null)") {
    NothingType()(pos).description should be("NOTHING")
  }

  test(s"LIST<NOTHING> should have correct descriptions") {
    ListType(NothingType()(pos), isNullable = true)(pos).description should be(s"LIST<NOTHING>")
    ListType(NothingType()(pos), isNullable = false)(pos).description should be(s"LIST<NOTHING> NOT NULL")
  }

  test(s"ANY<NOTHING> should have correct descriptions") {
    ClosedDynamicUnionType(Set(NothingType()(pos)))(pos).description should be(s"NOTHING")
  }

  test(s"NULL should have correct description (no not null)") {
    NullType()(pos).description should be("NULL")
  }

  test(s"NULL NOT NULL should have correct description of NOTHING") {
    NullType()(pos).withIsNullable(false).description should be("NOTHING")
  }

  test(s"LIST<NULL> should have correct descriptions") {
    ListType(NullType()(pos), isNullable = true)(pos).description should be(s"LIST<NULL>")
    ListType(NullType()(pos).withIsNullable(false), isNullable = true)(pos).description should be(
      s"LIST<NOTHING>"
    )
    ListType(NullType()(pos), isNullable = false)(pos).description should be(s"LIST<NULL> NOT NULL")
    ListType(NullType()(pos).withIsNullable(false), isNullable = false)(pos).description should be(
      s"LIST<NOTHING> NOT NULL"
    )
  }

  test(s"ANY<NULL> should have correct descriptions") {
    ClosedDynamicUnionType(Set(NullType()(pos)))(pos).description should be(s"NULL")
    ClosedDynamicUnionType(Set(NullType()(pos).withIsNullable(false)))(pos).description should be(s"NOTHING")
  }

  test("lists with multiple types should have correct descriptions") {
    CypherType.normalizeTypes(ListType(
      ClosedDynamicUnionType(Set(
        BooleanType(isNullable = true)(pos),
        IntegerType(isNullable = true)(pos),
        StringType(isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<BOOLEAN | STRING | INTEGER>")

    CypherType.normalizeTypes(ListType(
      ClosedDynamicUnionType(Set(
        DateType(isNullable = false)(pos),
        StringType(isNullable = false)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<STRING NOT NULL | DATE NOT NULL>")

    CypherType.normalizeTypes(ListType(
      ClosedDynamicUnionType(Set(
        DateType(isNullable = false)(pos),
        IntegerType(isNullable = true)(pos),
        StringType(isNullable = false)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<STRING | INTEGER | DATE>")

    CypherType.normalizeTypes(ListType(
      ClosedDynamicUnionType(Set(
        ListType(StringType(isNullable = true)(pos), isNullable = false)(pos),
        ListType(StringType(isNullable = true)(pos), isNullable = true)(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<LIST<STRING>>")

    CypherType.normalizeTypes(ListType(
      ClosedDynamicUnionType(Set(
        ClosedDynamicUnionType(Set(StringType(isNullable = true)(pos)))(pos),
        ClosedDynamicUnionType(Set(StringType(isNullable = true)(pos)))(pos)
      ))(pos),
      isNullable = true
    )(pos)).description should be("LIST<STRING>")
  }

  test("closed dynamic union with multiple types should have correct descriptions") {
    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        BooleanType(isNullable = true)(pos),
        IntegerType(isNullable = true)(pos),
        StringType(isNullable = true)(pos)
      )
    )(pos)).description should be("BOOLEAN | STRING | INTEGER")

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(DateType(isNullable = false)(pos), StringType(isNullable = false)(pos))
    )(pos)).description should be("STRING NOT NULL | DATE NOT NULL")

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        DateType(isNullable = false)(pos),
        IntegerType(isNullable = true)(pos),
        StringType(isNullable = false)(pos)
      )
    )(pos)).description should be("STRING | INTEGER | DATE")

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        ListType(StringType(isNullable = true)(pos), isNullable = false)(pos),
        ListType(StringType(isNullable = false)(pos), isNullable = true)(pos)
      )
    )(pos)).description should be("LIST<STRING>")

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        ClosedDynamicUnionType(Set(StringType(isNullable = true)(pos)))(pos),
        ClosedDynamicUnionType(Set(StringType(isNullable = false)(pos)))(pos)
      )
    )(pos)).description should be("STRING")

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        ClosedDynamicUnionType(Set(StringType(isNullable = true)(pos)))(pos),
        ClosedDynamicUnionType(Set(StringType(isNullable = false)(pos)))(pos)
      )
    )(pos)).simplify.description should be("STRING")
  }

  test("Ordering of different lists should be correct in descriptions") {
    val list1 = ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos)
    val list2 =
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          StringType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    val list3 = ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos)
    val list4 =
      ListType(
        ClosedDynamicUnionType(Set(
          FloatType(isNullable = true)(pos),
          StringType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    val notAList = DateType(isNullable = true)(pos)

    CypherType.normalizeTypes(ListType(
      ClosedDynamicUnionType(Set(list1, list2, list3, list4, notAList))(pos),
      isNullable = true
    )(pos)).description should be(
      "LIST<DATE | LIST<INTEGER> | LIST<BOOLEAN | STRING> | LIST<STRING | FLOAT>>"
    )

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(
        ListType(ClosedDynamicUnionType(Set(list1, list4))(pos), isNullable = true)(pos),
        notAList,
        list3,
        list2
      )
    )(pos)).description should be(
      "DATE | LIST<INTEGER> | LIST<BOOLEAN | STRING> | LIST<LIST<BOOLEAN> | LIST<STRING | FLOAT>>"
    )
  }

  test("Ordering of different closed dynamic unions should be correct in descriptions") {
    val union1 = ClosedDynamicUnionType(Set(BooleanType(isNullable = true)(pos)))(pos)
    val union2 = ClosedDynamicUnionType(Set(
      BooleanType(isNullable = true)(pos),
      StringType(isNullable = true)(pos)
    ))(pos)
    val union3 = ClosedDynamicUnionType(Set(IntegerType(isNullable = true)(pos)))(pos)
    val union4 = ClosedDynamicUnionType(Set(
      FloatType(isNullable = true)(pos),
      StringType(isNullable = true)(pos)
    ))(pos)
    val notAUnion = DateType(isNullable = true)(pos)

    CypherType.normalizeTypes(
      ListType(
        ClosedDynamicUnionType(Set(union1, union2, union3, union4, notAUnion))(pos),
        isNullable = true
      )(pos)
    ).description should be("LIST<BOOLEAN | STRING | INTEGER | FLOAT | DATE>")

    CypherType.normalizeTypes(ClosedDynamicUnionType(
      Set(ClosedDynamicUnionType(Set(union1, union4))(pos), notAUnion, union3, union2)
    )(pos)).description should be("BOOLEAN | STRING | INTEGER | FLOAT | DATE")
  }

  // Rule combination tests
  // For more in depth testing of each individual type see above tests.

  // Use a second closedDynamicUnion as a set doesn't need to be tested
  private val rule1 = ClosedDynamicUnionType(
    Set(
      BooleanType(isNullable = true)(pos),
      ClosedDynamicUnionType(Set(BooleanType(isNullable = true)(pos)))(pos)
    )
  )(pos)
  private val rule1Normalized = BooleanType(isNullable = true)(pos)

  private val rule2 = NullType()(pos).withIsNullable(false)
  private val rule2Normalized = NothingType()(pos)

  private val rule3 = ClosedDynamicUnionType(
    Set(
      StringType(isNullable = true)(pos),
      NothingType()(pos)
    )
  )(pos)
  private val rule3Normalized = StringType(isNullable = true)(pos)

  private val rule4 = ClosedDynamicUnionType(
    Set(
      IntegerType(isNullable = true)(pos),
      NullType()(pos)
    )
  )(pos)
  private val rule4Normalized = IntegerType(isNullable = true)(pos)

  private val rule5 = ClosedDynamicUnionType(
    Set(
      IntegerType(isNullable = true)(pos),
      FloatType(isNullable = false)(pos)
    )
  )(pos)

  private val rule5Normalized = ClosedDynamicUnionType(
    Set(
      IntegerType(isNullable = true)(pos),
      FloatType(isNullable = true)(pos)
    )
  )(pos)

  private val rule6 = ClosedDynamicUnionType(
    Set(
      ClosedDynamicUnionType(Set(NodeType(isNullable = false)(pos)))(pos)
    )
  )(pos)
  private val rule6Normalized = NodeType(isNullable = false)(pos)

  private val rule7 = ClosedDynamicUnionType(setOfAllPropertyTypes ++ Set(
    NodeType(isNullable = true)(pos),
    RelationshipType(isNullable = true)(pos),
    MapType(isNullable = true)(pos),
    PathType(isNullable = true)(pos),
    ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
  ))(pos)
  private val rule7Normalized = AnyType(isNullable = true)(pos)

  private val rule8 = ClosedDynamicUnionType(
    Set(
      IntegerType(isNullable = true)(pos),
      FloatType(isNullable = true)(pos),
      AnyType(isNullable = true)(pos)
    )
  )(pos)
  private val rule8Normalized = AnyType(isNullable = true)(pos)

  private val rule9 = PropertyValueType(isNullable = true)(pos)
  private val rule9Normalized = ClosedDynamicUnionType(setOfAllPropertyTypes)(pos)

  private val rule10 = ClosedDynamicUnionType(Set(
    ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos),
    ListType(
      ClosedDynamicUnionType(
        Set(
          IntegerType(isNullable = true)(pos),
          FloatType(isNullable = true)(pos)
        )
      )(pos),
      isNullable = true
    )(pos)
  ))(pos)

  private val rule10Normalized = ListType(
    ClosedDynamicUnionType(
      Set(
        IntegerType(isNullable = true)(pos),
        FloatType(isNullable = true)(pos)
      )
    )(pos),
    isNullable = true
  )(pos)

  private def checkRulesNormalizeTheSame(
    listRules: Set[CypherType],
    listNormalizedRules: Set[CypherType]
  ): Unit = {
    CypherType.normalizeTypes(
      ClosedDynamicUnionType(listRules)(pos)
    ) should be(CypherType.normalizeTypes(ClosedDynamicUnionType(listNormalizedRules)(pos)))
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

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(rule1, rule2, rule3))(pos)
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

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(rule4, rule5, rule6))(pos)
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

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(rule7, rule8, rule9, rule10))(pos)
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

    CypherType.normalizeTypes(
      ClosedDynamicUnionType(Set(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10))(pos)
    ).description should be("ANY")
  }

  // Test isSubtype of method for types
  test("simpleTypes isSubtype works as expected") {
    val `STRING` = StringType(isNullable = true)(pos)
    val `INTEGER` = IntegerType(isNullable = true)(pos)
    val `INTEGER NOT NULL` = IntegerType(isNullable = false)(pos)
    val `ANY` = AnyType(isNullable = true)(pos)
    val `STRING | ANY` = ClosedDynamicUnionType(Set(`STRING`, `ANY`))(pos)
    val `STRING | INTEGER` = ClosedDynamicUnionType(Set(`STRING`, `INTEGER`))(pos)

    // Simple type isSubtypeOf itself but only if nullabilities match
    `INTEGER`.isSubtypeOf(`ANY`) shouldBe true
    `INTEGER`.isSubtypeOf(`INTEGER`) shouldBe true
    `INTEGER NOT NULL`.isSubtypeOf(`INTEGER`) shouldBe true
    `INTEGER`.isSubtypeOf(`INTEGER NOT NULL`) shouldBe false
    `INTEGER`.isSubtypeOf(`ANY`) shouldBe true
    `INTEGER NOT NULL`.isSubtypeOf(`ANY`) shouldBe true

    // Simple type isSubtype if contained in Closed Dynamic Union
    `INTEGER`.isSubtypeOf(`STRING | INTEGER`) shouldBe true
    `INTEGER`.isSubtypeOf(`STRING | ANY`) shouldBe true
  }

  test("listTypes isSubtype works as expected") {
    val `LIST<STRING>` = ListType(StringType(isNullable = true)(pos), isNullable = true)(pos)
    val `LIST<INTEGER>` = ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos)
    val `LIST<STRING NOT NULL>` = ListType(StringType(isNullable = false)(pos), isNullable = true)(pos)
    val `LIST<INTEGER NOT NULL>` = ListType(IntegerType(isNullable = false)(pos), isNullable = true)(pos)
    val `LIST<ANY>` = ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)

    // Lists are contained in LIST<ANY>
    `LIST<STRING>`.isSubtypeOf(`LIST<ANY>`) shouldBe true
    `LIST<INTEGER>`.isSubtypeOf(`LIST<ANY>`) shouldBe true

    // Closed dynamic unions of lists are contained in LIST<ANY>
    val `LIST<STRING> | LIST<INTEGER>` = ClosedDynamicUnionType(Set(`LIST<STRING>`, `LIST<INTEGER>`))(pos)
    `LIST<STRING> | LIST<INTEGER>`.isSubtypeOf(`LIST<ANY>`) shouldBe true

    // Closed dynamic unions of lists are contained in nullable versions of themselves, but not the other way around
    val `LIST<STRING NOT NULL> | LIST<INTEGER NOT NULL>` =
      ClosedDynamicUnionType(Set(`LIST<STRING NOT NULL>`, `LIST<INTEGER NOT NULL>`))(pos)
    `LIST<STRING NOT NULL> | LIST<INTEGER NOT NULL>`.isSubtypeOf(`LIST<STRING> | LIST<INTEGER>`) shouldBe true
    `LIST<STRING> | LIST<INTEGER>`.isSubtypeOf(`LIST<STRING NOT NULL> | LIST<INTEGER NOT NULL>`) shouldBe false
  }

  test("Closed Dynamic Unions types isSubtype works as expected") {
    val `STRING` = StringType(isNullable = true)(pos)
    val `BOOLEAN NOT NULL` = BooleanType(isNullable = false)(pos)
    val `LIST<INTEGER NOT NULL>` = ListType(IntegerType(isNullable = false)(pos), isNullable = true)(pos)
    val `LIST<ANY>` = ListType(AnyType(isNullable = true)(pos), isNullable = true)(pos)
    val `LIST<INTEGER>` = ListType(IntegerType(isNullable = true)(pos), isNullable = true)(pos)
    val `STRING | LIST<INTEGER NOT NULL>` = ClosedDynamicUnionType(Set(`STRING`, `LIST<INTEGER NOT NULL>`))(pos)
    val `STRING | LIST<INTEGER>` = ClosedDynamicUnionType(Set(`STRING`, `LIST<INTEGER>`))(pos)
    val `STRING | BOOLEAN NOT NULL | LIST<ANY>` =
      ClosedDynamicUnionType(Set(`STRING`, `BOOLEAN NOT NULL`, `LIST<ANY>`))(pos)
    val `STRING | BOOLEAN NOT NULL | LIST<INTEGER>` =
      ClosedDynamicUnionType(Set(`STRING`, `BOOLEAN NOT NULL`, `LIST<INTEGER>`))(pos)
    val `ANY` = AnyType(isNullable = true)(pos)
    val `ANY NOT NULL` = AnyType(isNullable = false)(pos)

    `STRING`.isSubtypeOf(`LIST<ANY>`) shouldBe false
    `STRING | LIST<INTEGER NOT NULL>`.isSubtypeOf(`STRING | LIST<INTEGER>`) shouldBe true
    `STRING | BOOLEAN NOT NULL | LIST<INTEGER>`.isSubtypeOf(`STRING | BOOLEAN NOT NULL | LIST<ANY>`) shouldBe true
    `STRING | BOOLEAN NOT NULL | LIST<INTEGER>`.isSubtypeOf(`ANY`) shouldBe true
    `STRING | LIST<INTEGER>`.isSubtypeOf(`ANY NOT NULL`) shouldBe false
  }
}
