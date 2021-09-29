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

  private def createNodeIndexSeek(idName: String,
                                  label: LabelToken,
                                  properties: Seq[IndexedProperty],
                                  valueExpr: QueryExpression[Expression],
                                  argumentIds: Set[String],
                                  indexOrder: IndexOrder): Boolean => NodeIndexSeekLeafPlan = { unique =>
    if (unique) {
      NodeUniqueIndexSeek(idName, label, properties, valueExpr, argumentIds, indexOrder, IndexType.BTREE)
    } else {
      NodeIndexSeek(idName, label, properties, valueExpr, argumentIds, indexOrder, IndexType.BTREE)
    }
  }

  private def createRelIndexSeek(idName: String,
                                 startNode: String,
                                 endNode: String,
                                 relId: RelationshipTypeToken,
                                 properties: Seq[IndexedProperty],
                                 valueExpr: QueryExpression[Expression],
                                 argumentIds: Set[String],
                                 indexOrder: IndexOrder) = {
    DirectedRelationshipIndexSeek(idName, startNode, endNode, relId, properties, valueExpr, argumentIds, indexOrder, IndexType.BTREE)
  }

  val nodeTestCaseCreators: List[(String => GetValueFromIndexBehavior, Set[String], IndexOrder) => (String, Boolean => LogicalPlan)] = List(
    (getValue, args, indexOrder) => "a:X(prop = 1)" -> createNodeIndexSeek("a", label("X"), Seq(prop("prop", getValue("prop"), NODE_TYPE)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:X(prop = 1)" -> createNodeIndexSeek("b", label("X"), Seq(prop("prop", getValue("prop"), NODE_TYPE)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(prop = 1)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("prop", getValue("prop"), NODE_TYPE)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 1)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE)), exactInt(2), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2 OR 5)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE)), exactInts(2, 5), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2, cats = 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(exactInt(2), exactInt(4))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2 OR 5, cats = 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(exactInts(2, 5), exactInt(4))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2, cats = 4 OR 5)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(exactInt(2), exactInts(4, 5))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2 OR 3, cats = 4 OR 5)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(exactInts(2, 3), exactInts(4, 5))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name = 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), exactString("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name < 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), lt(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name <= 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), lte(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name > 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), gt(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name >= 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), gte(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(3 < name < 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), gt_lt(intLiteral(3), intLiteral(4)), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(3 <= name < 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), gte_lt(intLiteral(3), intLiteral(4)), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(3 < name <= 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), gt_lte(intLiteral(3), intLiteral(4)), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(3 <= name <= 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), gte_lte(intLiteral(3), intLiteral(4)), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs < 3, cats >= 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(lt(intLiteral(3)), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 3, cats >= 4)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(exactInt(3), gte(intLiteral(4)))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name STARTS WITH 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), startsWith("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name STARTS WITH 'hi', cats)" -> createNodeIndexSeek("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("cats", getValue("cats"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(startsWith("hi"), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name ENDS WITH 'hi')" -> (_ => NodeIndexEndsWithScan("b", label("Y"), prop("name", getValue("name"), NODE_TYPE), string("hi"), args, indexOrder, IndexType.BTREE)),
    (getValue, args, indexOrder) => "b:Y(dogs = 1, name ENDS WITH 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("name", getValue("name"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(exactInt(1), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name ENDS WITH 'hi', dogs = 1)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)), args, indexOrder, IndexType.BTREE)),
    (getValue, args, indexOrder) => "b:Y(name CONTAINS 'hi')" -> (_ => NodeIndexContainsScan("b", label("Y"), prop("name", getValue("name"), NODE_TYPE), string("hi"), args, indexOrder, IndexType.BTREE)),
    (getValue, args, indexOrder) => "b:Y(dogs = 1, name CONTAINS 'hi')" -> createNodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue("dogs"), NODE_TYPE, 0), prop("name", getValue("name"), NODE_TYPE, 1)), CompositeQueryExpression(Seq(exactInt(1), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name CONTAINS 'hi', dogs)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)), args, indexOrder, IndexType.BTREE)),
    (getValue, args, indexOrder) => "b:Y(name)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE)), args, indexOrder, IndexType.BTREE)),
    (getValue, args, indexOrder) => "b:Y(name, dogs)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)), args, indexOrder, IndexType.BTREE)),
    (getValue, args, indexOrder) => "b:Y(name, dogs = 3)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue("name"), NODE_TYPE, 0), prop("dogs", getValue("dogs"), NODE_TYPE, 1)), args, indexOrder, IndexType.BTREE))
  )

  val relTestCaseCreators: List[(String => GetValueFromIndexBehavior, Set[String], IndexOrder) => (String, LogicalPlan)] = List(
    (getValue, args, indexOrder) => "(a)-[r:R(prop = 1)]->(b)" -> DirectedRelationshipIndexSeek("r", "a", "b", typ("R"), Seq(prop("prop", getValue("prop"), RELATIONSHIP_TYPE)), exactInt(1), args, indexOrder, IndexType.BTREE),
    (getValue, args, indexOrder) => "(a)<-[r:R(prop = 1)]-(b)" -> DirectedRelationshipIndexSeek("r", "b", "a", typ("R"), Seq(prop("prop", getValue("prop"), RELATIONSHIP_TYPE)), exactInt(1), args, indexOrder, IndexType.BTREE),
    (getValue, args, indexOrder) => "(a)-[r:R(prop = 1)]-(b)" -> UndirectedRelationshipIndexSeek("r", "a", "b", typ("R"), Seq(prop("prop", getValue("prop"), RELATIONSHIP_TYPE)), exactInt(1), args, indexOrder, IndexType.BTREE),
    (getValue, args, indexOrder) => "(a)-[r:REL_ABC(id)]-(b)" -> UndirectedRelationshipIndexScan("r", "a", "b", typ("REL_ABC"), Seq(prop("id", getValue("id"), RELATIONSHIP_TYPE)), args, indexOrder, IndexType.BTREE),
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
      case _ => DoNotGetValue
    }
  )

  for {
    getValue <- getValueFunctions
    args <- List(Set.empty[String], Set("n", "m"))
    order <- List(IndexOrderNone, IndexOrderAscending, IndexOrderDescending)
    unique <- List(true, false)
    (str, expectedPlan) <- nodeTestCaseCreators.map(f => f(getValue, args, order))
  } {
    test(s"[$getValue, args=$args, order=$order, unique=$unique] should parse `$str`") {
      nodeIndexSeek(str, getValue, argumentIds = args, indexOrder=order, unique=unique) should be(expectedPlan(unique))
    }
  }

  for {
    getValue <- getValueFunctions
    args <- List(Set.empty[String], Set("n", "m"))
    order <- List(IndexOrderNone, IndexOrderAscending, IndexOrderDescending)
    (str, expectedPlan) <- relTestCaseCreators.map(f => f(getValue, args, order))
  } {
    test(s"[$getValue, args=$args, order=$order] should parse `$str`") {
      relationshipIndexSeek(str, getValue, argumentIds = args, indexOrder=order) should be(expectedPlan)
    }
  }

  test("custom value expression") {
    nodeIndexSeek("a:X(prop = ???)", paramExpr = Some(string("101"))) should be(
      NodeIndexSeek("a", label("X"), Seq(prop("prop", DoNotGetValue, NODE_TYPE)), exactString("101"), Set.empty, IndexOrderNone, IndexType.BTREE)
    )
  }

  test("custom query expression") {
    nodeIndexSeek("a:X(prop)", customQueryExpression = Some(exactInts(1, 2, 3)) ) should be(
      NodeIndexSeek("a", label("X"), Seq(prop("prop", DoNotGetValue, NODE_TYPE)), exactInts(1, 2, 3), Set.empty, IndexOrderNone, IndexType.BTREE)
    )
  }

  // HELPERS

  private def label(str:String) = LabelToken(str, LabelId(0))
  private def typ(str: String) = RelationshipTypeToken(str, RelTypeId(0))

  private def prop(name: String, getValue: GetValueFromIndexBehavior, entityType: EntityType, propId: Int = 0) =
    IndexedProperty(PropertyKeyToken(name, PropertyKeyId(propId)), getValue, entityType)

  private def exactInt(int: Int) = SingleQueryExpression(intLiteral(int))

  private def exactInts(ints: Int*) = ManyQueryExpression(ListLiteral(ints.map(i => intLiteral(i)))(pos))

  private def exactString(x: String) = SingleQueryExpression(string(x))

  private def string(x: String) = StringLiteral(x)(pos)
  private def intLiteral(x: Int) = SignedDecimalIntegerLiteral(x.toString)(pos)

  private def lt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def lte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(InclusiveBound(x))))(pos))
  private def gt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def gte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(InclusiveBound(x))))(pos))
  private def gt_lt(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(RangeGreaterThan(NonEmptyList(ExclusiveBound(x))), RangeLessThan(NonEmptyList(ExclusiveBound(y)))))(pos))
  private def gte_lt(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(RangeGreaterThan(NonEmptyList(InclusiveBound(x))), RangeLessThan(NonEmptyList(ExclusiveBound(y)))))(pos))
  private def gt_lte(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(RangeGreaterThan(NonEmptyList(ExclusiveBound(x))), RangeLessThan(NonEmptyList(InclusiveBound(y)))))(pos))
  private def gte_lte(x: Literal, y: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(RangeGreaterThan(NonEmptyList(InclusiveBound(x))), RangeLessThan(NonEmptyList(InclusiveBound(y)))))(pos))

  private def startsWith(x: String) = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(string(x)))(pos))

  private def exists() = ExistenceQueryExpression()

}
