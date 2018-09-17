/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util._
import org.opencypher.v9_0.util.symbols.CTString
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class IndexWithValuesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport with PlanMatchHelp {

  // or planner between two indexes

  test("in an OR index plan should not get values for range predicates") {
    val plan = new given {
      indexOn("Awesome", "prop1").providesValues()
      indexOn("Awesome", "prop2").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 > 42 OR n.prop2 > 3 RETURN n.prop1, n.prop2"

    plan._2 should equal(
      Projection(
        Distinct(
          Union(
            Selection(Ands(Set(GreaterThan(prop("n", "prop1"), SignedDecimalIntegerLiteral("42")(pos))(pos)))(pos),
              IndexSeek("n:Awesome(prop1)")),
            Selection(Ands(Set(GreaterThan(prop("n", "prop2"), SignedDecimalIntegerLiteral("3")(pos))(pos)))(pos),
              IndexSeek("n:Awesome(prop2)", propIds = Map("prop2" -> 1)))
          ),
          Map("n" -> Variable("n")(pos))
        ),
        Map("n.prop1" -> Property(Variable("n")(pos), PropertyKeyName("prop1")(pos))(pos), "n.prop2" -> Property(Variable("n")(pos), PropertyKeyName("prop2")(pos))(pos))
      )
    )
  }

  test("in an OR index plan should not get values for equality predicates") {
    val plan = new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 = 42 OR n.prop2 = 3 RETURN n.prop1, n.prop2"

    plan._2 should equal(
      Projection(
        Distinct(
          Union(
            IndexSeek("n:Awesome(prop2 = 3)", propIds = Map("prop2" -> 1)),
            IndexSeek("n:Awesome(prop1 = 42)", propIds = Map("prop1" -> 0))
          ),
          Map("n" -> Variable("n")(pos))
        ),
        Map("n.prop1" -> Property(Variable("n")(pos), PropertyKeyName("prop1")(pos))(pos), "n.prop2" -> Property(Variable("n")(pos), PropertyKeyName("prop2")(pos))(pos))
      )
    )
  }

  // Index exact seeks

  test("should plan index seek with GetValue when the property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42)", GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("for exact seeks, should even plan index seek with GetValue when the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42)", GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with DoNotGetValue when another property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      indexOn("Awesome", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42)", DoNotGetValue),
        Map(propertyProj("n", "foo")))
    )
  }

  test("should plan projection and index seek with GetValue when two properties are projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      indexOn("Awesome", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.foo, n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42)", GetValue),
        Map(propertyProj("n", "foo"), cachedNodePropertyProj("n", "prop")))
    )
  }

  // This can change to GetValue if alignGetValueFromIndexBehavior gets cleverer
  test("should plan projection and index seek with DoNotGetValue when another predicate uses the property") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      indexOn("Awesome", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop <= 42 AND n.prop % 2 = 0 RETURN n.foo"

    plan._2 should equal(
      Projection(
        Selection(Ands(Set(Equals(Modulo(Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos), SignedDecimalIntegerLiteral("2")(pos))(pos), SignedDecimalIntegerLiteral("0")(pos))(pos)))(pos),
          IndexSeek("n:Awesome(prop <= 42)", DoNotGetValue)),
        Map(propertyProj("n", "foo")))
    )
  }

  test("should plan index seek with GetValue when the property is projected and renamed in a RETURN") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.prop AS foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42)", GetValue),
        Map(cachedNodePropertyProj("foo", "n", "prop")))
    )
  }

  test("should plan index seek with GetValue when the property is projected and renamed in a WITH") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n.prop AS foo, true AS bar RETURN foo, bar AS baz"

    plan._2 should equal(
      Projection(
        Projection(
          IndexSeek("n:Awesome(prop = 42)", GetValue),
          Map(cachedNodePropertyProj("foo", "n", "prop"), "bar" -> True()(pos))),
        Map("baz" -> Variable("bar")(pos)))
    )
  }

  test("should not be fooled to use a variable when the node variable is defined twice") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n as m MATCH (m)-[r]-(n) RETURN n.prop"

    plan._2 should equal(
      Projection(
        Expand(
          Projection(
            NodeIndexSeek(
              "  n@7",
              LabelToken("Awesome", LabelId(0)),
              Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
              SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
              Set.empty,
              IndexOrderNone),
            Map("m" -> Variable("  n@7")(pos))),
          "m", SemanticDirection.BOTH, Seq.empty, "  n@63", "r"),
        Map("n.prop" -> Property(Variable("  n@63")(pos), PropertyKeyName("prop")(pos))(pos)))
    )
  }

  test("should plan index seek with GetValue when the property is projected before the property access") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n MATCH (m)-[r]-(n) RETURN n.prop"

    plan._2 should equal(
      Projection(
        Expand(
          IndexSeek("n:Awesome(prop = 42)", GetValue),
          "n", SemanticDirection.BOTH, Seq.empty, "m", "r"),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is projected inside of a function") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN toUpper(n.prop)"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 'foo')", GetValue),
        Map("toUpper(n.prop)" -> FunctionInvocation(Namespace(List())(pos), FunctionName("toUpper")(pos), distinct = false, IndexedSeq(cachedNodeProperty("n", "prop")))(pos)))
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in ORDER BY") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN n.foo ORDER BY toUpper(n.prop)"

    plan._2 should equal(
      Projection(
        Projection(
          Sort(
            Projection(
              IndexSeek("n:Awesome(prop = 'foo')", GetValue),
              Map("  FRESHID61" -> FunctionInvocation(Namespace(List())(pos), FunctionName("toUpper")(pos), distinct = false, IndexedSeq(cachedNodeProperty("n", "prop")))(pos))),
            Seq(Ascending("  FRESHID61"))),
          Map("  FRESHID48" -> Property(Variable("n")(pos), PropertyKeyName("foo")(pos))(pos))),
        Map("n.foo" -> Variable("  FRESHID48")(pos)))
    )
  }

  test("should plan index seek with GetValue when the property is part of an aggregating column") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN sum(n.prop), n.foo AS nums"

    plan._2 should equal(
      Aggregation(
        IndexSeek("n:Awesome(prop = 42)", GetValue),
        Map("nums" -> Property(Variable("n")(pos), PropertyKeyName("foo")(pos))(pos)),
        Map("sum(n.prop)" -> FunctionInvocation(Namespace(List())(pos), FunctionName("sum")(pos), distinct = false, IndexedSeq(cachedNodeProperty("n", "prop")))(pos)))
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in key column of an aggregation and in ORDER BY") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN sum(n.foo), n.prop ORDER BY n.prop"

    plan._2 should equal(
      Projection(
        Sort(
          Aggregation(
            IndexSeek("n:Awesome(prop = 'foo')", GetValue),
            Map(cachedNodePropertyProj("  FRESHID60", "n", "prop")),
            Map("  FRESHID46" -> FunctionInvocation(Namespace(List())(pos), FunctionName("sum")(pos), distinct = false, IndexedSeq(Property(Variable("n")(pos), PropertyKeyName("foo")(pos))(pos)))(pos))),
          Seq(Ascending("  FRESHID60"))),
        Map("sum(n.foo)" -> Variable("  FRESHID46")(pos), "n.prop" -> Variable("  FRESHID60")(pos)))
    )
  }

  test("should plan index seek with GetValue when the property is part of a distinct column") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN DISTINCT n.prop"

    plan._2 should equal(
      Distinct(
        IndexSeek("n:Awesome(prop = 42)", GetValue),
        Map(cachedNodePropertyProj("n", "prop")))
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in an unwind projection") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' UNWIND [n.prop] AS foo RETURN foo"

    plan._2 should equal(
      UnwindCollection(
        IndexSeek("n:Awesome(prop = 'foo')", GetValue),
        "foo", ListLiteral(List(cachedNodeProperty("n", "prop")))(pos))
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in a procedure call") {
    val signature = ProcedureSignature(QualifiedName(Seq.empty, "fooProcedure"),
      IndexedSeq(FieldSignature("input", CTString)),
      Some(IndexedSeq(FieldSignature("value", CTString))),
      None,
      ProcedureReadOnlyAccess(Array.empty))

    val plan = new given {
      procedure(signature)
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' CALL fooProcedure(n.prop) YIELD value RETURN value"

    plan._2 should equal(
      ProcedureCall(
        IndexSeek("n:Awesome(prop = 'foo')", GetValue),
        ResolvedCall(signature,
          IndexedSeq(CoerceTo(cachedNodeProperty("n", "prop"), CTString)),
          IndexedSeq(ProcedureResultItem(None, Variable("value")(pos))(pos)))(pos))
    )
  }

  // STARTS WITH seek

  test("should plan starts with seek with GetValue when the property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop STARTS WITH 'foo')", GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and starts with seek with DoNotGetValue when the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop STARTS WITH 'foo')", DoNotGetValue),
        Map(propertyProj("n", "prop")))
    )
  }

  test("should plan projection and starts with seek with DoNotGetValue when another property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      indexOn("Awesome", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop STARTS WITH 'foo')", DoNotGetValue),
        Map(propertyProj("n", "foo")))
    )
  }

  // RANGE seek

  test("should plan range seek with GetValue when the property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop > 'foo')", GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and range seek with DoNotGetValue when the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop > 'foo')", DoNotGetValue),
        Map(propertyProj("n", "prop")))
    )
  }

  test("should plan projection and range seek with DoNotGetValue when another property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop")
      indexOn("Awesome", "foo")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop > 'foo')", DoNotGetValue),
        Map(propertyProj("n", "foo")))
    )
  }

  // RANGE seek on unique index

  test("should plan range seek with GetValue when the property is projected (unique index)") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop > 'foo')", GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and range seek with DoNotGetValue when the index does not provide values (unique index)") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop > 'foo')", DoNotGetValue),
        Map(propertyProj("n", "prop")))
    )
  }

  test("should plan projection and range seek with DoNotGetValue when another property is projected (unique index)") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
      uniqueIndexOn("Awesome", "foo")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop > 'foo')", DoNotGetValue),
        Map(propertyProj("n", "foo")))
    )
  }

  // RANGE seek on merge unique index

  ignore("should plan seek with GetValue when the property is projected (merge unique index)") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.prop"

    plan._2 should equal(
      AntiConditionalApply(
        Optional(
          ActiveRead(
            NodeUniqueIndexSeek(
              "n",
              LabelToken("Awesome", LabelId(0)),
              Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue)),
              SingleQueryExpression(StringLiteral("foo")(pos)),
              Set.empty,
              IndexOrderNone)
          )
        ),
        MergeCreateNode(
          Argument(Set()),
          "n", Seq(LabelName("Awesome")(pos)), Some(MapExpression(List((PropertyKeyName("prop")(pos), StringLiteral("foo")(pos))))(pos))
        ),
        Seq("n"))
    )
  }

  ignore("for exact seeks, should even plan index seek with GetValue when the index does not provide values (merge unique index)") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.prop"

    plan._2 should equal(
      AntiConditionalApply(
        Optional(
          ActiveRead(
            NodeUniqueIndexSeek(
              "n",
              LabelToken("Awesome", LabelId(0)),
              Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue)),
              SingleQueryExpression(StringLiteral("foo")(pos)),
              Set.empty,
              IndexOrderNone)
          )
        ),
        MergeCreateNode(
          Argument(Set()),
          "n", Seq(LabelName("Awesome")(pos)), Some(MapExpression(List((PropertyKeyName("prop")(pos), StringLiteral("foo")(pos))))(pos))
        ),
        Seq("n"))
    )
  }

  test("should plan projection and range seek with DoNotGetValue when another property is projected (merge unique index)") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
      uniqueIndexOn("Awesome", "foo")
    } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.foo"

    plan._2 should equal(
      Projection(
        AntiConditionalApply(
          Optional(
            ActiveRead(
              NodeUniqueIndexSeek(
                "n",
                LabelToken("Awesome", LabelId(0)),
                Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
                SingleQueryExpression(StringLiteral("foo")(pos)),
                Set.empty,
                IndexOrderNone)
            )
          ),
          MergeCreateNode(
            Argument(Set()),
            "n", Seq(LabelName("Awesome")(pos)), Some(MapExpression(List((PropertyKeyName("prop")(pos), StringLiteral("foo")(pos))))(pos))
          ),
          Seq("n")),
        Map(propertyProj("n", "foo")))
    )
  }

  // composite index

  test("should plan index seek with GetValue when the property is projected (composite index)") {
    val plan = new given {
      indexOn("Awesome", "prop", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.prop, n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42, foo = 21)", GetValue),
        Map(cachedNodePropertyProj("n", "prop"), cachedNodePropertyProj("n", "foo"))
      )
    )
  }

  test("for exact seeks, should even plan index seek with GetValue when the index does not provide values (composite index)") {
    val plan = new given {
      indexOn("Awesome", "prop", "foo")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.prop, n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42, foo = 21)", GetValue),
        Map(cachedNodePropertyProj("n", "prop"), cachedNodePropertyProj("n", "foo"))
      )
    )
  }

  test("should plan projection and index seek with DoNotGetValue when another property is projected (composite index)") {
    val plan = new given {
      indexOn("Awesome", "prop", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.bar"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop = 42, foo = 21)", DoNotGetValue),
        Map("n.bar" -> Property(Variable("n")(pos), PropertyKeyName("bar")(pos))(pos)))
    )
  }

  test("should plan index seek with GetValue and DoNotGetValue when only one property is projected (composite index)") {
    val plan = new given {
      indexOn("Awesome", "prop", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.prop"

    plan._2 should equal(
      Projection(
        NodeIndexSeek(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue), IndexedProperty(PropertyKeyToken(PropertyKeyName("foo") _, PropertyKeyId(1)), DoNotGetValue)),
          CompositeQueryExpression(Seq(SingleQueryExpression(SignedDecimalIntegerLiteral("42") _), SingleQueryExpression(SignedDecimalIntegerLiteral("21") _))),
          Set.empty,
          IndexOrderNone),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  // CONTAINS scan

  test("should plan index contains scan with GetValue when the property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop CONTAINS 'foo')", GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index contains scan with DoNotGetValue when the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop CONTAINS 'foo')", DoNotGetValue),
        Map(propertyProj("n", "prop")))
    )
  }

  test("should plan projection and index contains scan with DoNotGetValue when another property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      indexOn("Awesome", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop CONTAINS 'foo')", DoNotGetValue),
        Map(propertyProj("n", "foo")))
    )
  }

  // ENDS WITH scan

  test("should plan index ends with scan with GetValue when the property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop ENDS WITH 'foo')", GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index ends with scan with DoNotGetValue when the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.prop"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop ENDS WITH 'foo')", DoNotGetValue),
        Map(propertyProj("n", "prop")))
    )
  }

  test("should plan projection and index ends with scan with DoNotGetValue when another property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      indexOn("Awesome", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.foo"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop ENDS WITH 'foo')", DoNotGetValue),
        Map(propertyProj("n", "foo")))
    )
  }
}
