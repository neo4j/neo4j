/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeek.relationshipIndexSeek
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class IndexSeekTest extends CypherFunSuite {

  implicit val idGen: IdGen = SameId(Id(42))
  private val pos = InputPosition.NONE

  private def createNodeIndexSeek(
    idName: String,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    valueExpr: QueryExpression[Expression],
    argumentIds: Set[String],
    indexOrder: IndexOrder,
    indexType: IndexType
  ): Boolean => NodeIndexSeekLeafPlan = { unique =>
    if (unique) {
      NodeUniqueIndexSeek(
        varFor(idName),
        label,
        properties,
        valueExpr,
        argumentIds.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = true
      )
    } else {
      NodeIndexSeek(
        varFor(idName),
        label,
        properties,
        valueExpr,
        argumentIds.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = true
      )
    }
  }

  val nodeTestCaseCreators: List[(String => GetValueFromIndexBehavior, Set[String], IndexOrder, IndexType) => (
    String,
    Boolean => LogicalPlan
  )] = List(
    (getValue, args, indexOrder, indexType) =>
      "a:X(prop = 1)" -> createNodeIndexSeek(
        "a",
        label("X"),
        Seq(prop("prop", getValue("prop"), NODE_TYPE)),
        exactInt(1),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:X(prop = 1)" -> createNodeIndexSeek(
        "b",
        label("X"),
        Seq(prop("prop", getValue("prop"), NODE_TYPE)),
        exactInt(1),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(prop = 1)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("prop", getValue("prop"), NODE_TYPE)),
        exactInt(1),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 1)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE)),
        exactInt(1),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 2)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE)),
        exactInt(2),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 2 OR 5)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE)),
        exactInts(2, 5),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 2, cats = 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(exactInt(2), exactInt(4))),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 2 OR 5, cats = 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(exactInts(2, 5), exactInt(4))),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 2, cats = 4 OR 5)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(exactInt(2), exactInts(4, 5))),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 2 OR 3, cats = 4 OR 5)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(exactInts(2, 3), exactInts(4, 5))),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name = varName)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE, 0)),
        variable("varName"),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name = 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        exactString("hi"),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name < 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        lt(string("hi")),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name <= 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        lte(string("hi")),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name > 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        gt(string("hi")),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name >= 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        gte(string("hi")),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(3 < name < 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        gt_lt(intLiteral(3), intLiteral(4)),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(3 <= name < 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        gte_lt(intLiteral(3), intLiteral(4)),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(3 < name <= 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        gt_lte(intLiteral(3), intLiteral(4)),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(3 <= name <= 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        gte_lte(intLiteral(3), intLiteral(4)),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs < 3, cats >= 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(lt(intLiteral(3)), exists())),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 3, cats >= 4)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(exactInt(3), gte(intLiteral(4)))),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name STARTS WITH 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE)),
        startsWith("hi"),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name STARTS WITH 'hi', cats)" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(startsWith("hi"), exists())),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name ENDS WITH 'hi')" -> (_ =>
        NodeIndexEndsWithScan(
          varFor("b"),
          label("Y"),
          prop("name", getValue("name"), NODE_TYPE),
          string("hi"),
          args.map(varFor),
          indexOrder,
          indexType
        )
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 1, name ENDS WITH 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("name", getValue("name"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(exactInt(1), exists())),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name ENDS WITH 'hi', dogs = 1)" -> (_ =>
        NodeIndexScan(
          varFor("b"),
          label("Y"),
          Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)),
          args.map(varFor),
          indexOrder,
          indexType,
          supportPartitionedScan = true
        )
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name CONTAINS 'hi')" -> (_ =>
        NodeIndexContainsScan(
          varFor("b"),
          label("Y"),
          prop("name", getValue("name"), NODE_TYPE),
          string("hi"),
          args.map(varFor),
          indexOrder,
          indexType
        )
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(dogs = 1, name CONTAINS 'hi')" -> createNodeIndexSeek(
        "b",
        label("Y"),
        Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("name", getValue("name"), NODE_TYPE, 1)),
        CompositeQueryExpression(Seq(exactInt(1), exists())),
        args,
        indexOrder,
        indexType
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name CONTAINS 'hi', dogs)" -> (_ =>
        NodeIndexScan(
          varFor("b"),
          label("Y"),
          Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)),
          args.map(varFor),
          indexOrder,
          indexType,
          supportPartitionedScan = true
        )
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name)" -> (_ =>
        NodeIndexScan(
          varFor("b"),
          label("Y"),
          Seq(prop("name", getValue("name"), NODE_TYPE)),
          args.map(varFor),
          indexOrder,
          indexType,
          supportPartitionedScan = true
        )
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name, dogs)" -> (_ =>
        NodeIndexScan(
          varFor("b"),
          label("Y"),
          Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)),
          args.map(varFor),
          indexOrder,
          indexType,
          supportPartitionedScan = true
        )
      ),
    (getValue, args, indexOrder, indexType) =>
      "b:Y(name, dogs = 3)" -> (_ =>
        NodeIndexScan(
          varFor("b"),
          label("Y"),
          Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)),
          args.map(varFor),
          indexOrder,
          indexType,
          supportPartitionedScan = true
        )
      )
  )

  val relTestCaseCreators
    : List[(String => GetValueFromIndexBehavior, Set[String], IndexOrder, IndexType) => (String, LogicalPlan)] = List(
    (getValue, args, indexOrder, indexType) =>
      "(a)-[r:R(prop = 1)]->(b)" -> DirectedRelationshipIndexSeek(
        varFor("r"),
        varFor("a"),
        varFor("b"),
        typ("R"),
        Seq(prop("prop", getValue("prop"), RELATIONSHIP_TYPE)),
        exactInt(1),
        args.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = true
      ),
    (getValue, args, indexOrder, indexType) =>
      "(a)<-[r:R(prop = 1)]-(b)" -> DirectedRelationshipIndexSeek(
        varFor("r"),
        varFor("b"),
        varFor("a"),
        typ("R"),
        Seq(prop("prop", getValue("prop"), RELATIONSHIP_TYPE)),
        exactInt(1),
        args.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = true
      ),
    (getValue, args, indexOrder, indexType) =>
      "(a)-[r:R(prop = 1)]-(b)" -> UndirectedRelationshipIndexSeek(
        varFor("r"),
        varFor("a"),
        varFor("b"),
        typ("R"),
        Seq(prop("prop", getValue("prop"), RELATIONSHIP_TYPE)),
        exactInt(1),
        args.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = true
      ),
    (getValue, args, indexOrder, indexType) =>
      "(a)-[r:REL_ABC(id)]-(b)" -> UndirectedRelationshipIndexScan(
        varFor("r"),
        varFor("a"),
        varFor("b"),
        typ("REL_ABC"),
        Seq(prop("id", getValue("id"), RELATIONSHIP_TYPE)),
        args.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = true
      )
  )

  val getValueFunctions: List[String => GetValueFromIndexBehavior] = List(
    _ => CanGetValue,
    _ => GetValue,
    _ => DoNotGetValue,
    {
      case "prop" => CanGetValue
      case "dogs" => GetValue
      case "cats" => DoNotGetValue
      case "name" => CanGetValue
      case _      => DoNotGetValue
    }
  )

  for {
    getValue <- getValueFunctions
    args <- List(Set.empty[String], Set("n", "m"))
    order <- List(IndexOrderNone, IndexOrderAscending, IndexOrderDescending)
    unique <- List(true, false)
    indexType <- IndexType.values()
    (str, expectedPlan) <- nodeTestCaseCreators.map(f => f(getValue, args, order, indexType))
  } {
    test(s"[$getValue, args=$args, order=$order, unique=$unique, indexType=$indexType] should parse `$str`") {
      nodeIndexSeek(
        str,
        getValue,
        argumentIds = args,
        indexOrder = order,
        unique = unique,
        indexType = indexType
      ) should be(expectedPlan(unique))
    }
  }

  for {
    getValue <- getValueFunctions
    args <- List(Set.empty[String], Set("n", "m"))
    order <- List(IndexOrderNone, IndexOrderAscending, IndexOrderDescending)
    indexType <- IndexType.values()
    (str, expectedPlan) <- relTestCaseCreators.map(f => f(getValue, args, order, indexType))
  } {
    test(s"[$getValue, args=$args, order=$order, indexType=$indexType] should parse `$str`") {
      relationshipIndexSeek(str, getValue, argumentIds = args, indexOrder = order, indexType = indexType) should be(
        expectedPlan
      )
    }
  }

  test("custom value expression") {
    nodeIndexSeek("a:X(prop = ???)", paramExpr = Some(string("101"))) should be(
      NodeIndexSeek(
        varFor("a"),
        label("X"),
        Seq(prop("prop", DoNotGetValue, NODE_TYPE)),
        exactString("101"),
        Set.empty,
        IndexOrderNone,
        IndexType.RANGE,
        supportPartitionedScan = true
      )
    )
  }

  test("custom query expression") {
    nodeIndexSeek("a:X(prop)", customQueryExpression = Some(exactInts(1, 2, 3))) should be(
      NodeIndexSeek(
        varFor("a"),
        label("X"),
        Seq(prop("prop", DoNotGetValue, NODE_TYPE)),
        exactInts(1, 2, 3),
        Set.empty,
        IndexOrderNone,
        IndexType.RANGE,
        supportPartitionedScan = true
      )
    )
  }

  // HELPERS

  private def label(str: String) = LabelToken(str, LabelId(0))
  private def typ(str: String) = RelationshipTypeToken(str, RelTypeId(0))

  private def prop(name: String, getValue: GetValueFromIndexBehavior, entityType: EntityType, propId: Int = 0) =
    IndexedProperty(PropertyKeyToken(name, PropertyKeyId(propId)), getValue, entityType)

  private def exactInt(int: Int) = SingleQueryExpression(intLiteral(int))

  private def exactInts(ints: Int*) = ManyQueryExpression(ListLiteral(ints.map(i => intLiteral(i)))(pos))

  private def exactString(x: String) = SingleQueryExpression(string(x))

  private def variable(name: String) = SingleQueryExpression(Variable(name)(pos))

  private def string(x: String) = StringLiteral(x)(pos.withInputLength(0))
  private def intLiteral(x: Int) = SignedDecimalIntegerLiteral(x.toString)(pos)

  private def lt(x: Literal) =
    RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(x))))(pos))

  private def lte(x: Literal) =
    RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(InclusiveBound(x))))(pos))

  private def gt(x: Literal) =
    RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(x))))(pos))

  private def gte(x: Literal) =
    RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(InclusiveBound(x))))(pos))

  private def gt_lt(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(
    RangeGreaterThan(NonEmptyList(ExclusiveBound(x))),
    RangeLessThan(NonEmptyList(ExclusiveBound(y)))
  ))(pos))

  private def gte_lt(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(
    RangeGreaterThan(NonEmptyList(InclusiveBound(x))),
    RangeLessThan(NonEmptyList(ExclusiveBound(y)))
  ))(pos))

  private def gt_lte(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(
    RangeGreaterThan(NonEmptyList(ExclusiveBound(x))),
    RangeLessThan(NonEmptyList(InclusiveBound(y)))
  ))(pos))

  private def gte_lte(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(
    RangeGreaterThan(NonEmptyList(InclusiveBound(x))),
    RangeLessThan(NonEmptyList(InclusiveBound(y)))
  ))(pos))

  private def startsWith(x: String) = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(string(x)))(pos))

  private def exists() = ExistenceQueryExpression()

}
