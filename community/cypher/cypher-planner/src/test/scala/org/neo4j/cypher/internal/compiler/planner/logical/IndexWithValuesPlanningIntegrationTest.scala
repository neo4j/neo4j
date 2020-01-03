/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v4_0.ast.ProcedureResultItem
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.expressions.{AndedPropertyInequalities, LabelToken, SemanticDirection}
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.symbols.CTString
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class IndexWithValuesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  // or planner between two indexes

  test("in an OR index plan should not use cached values outside union for range predicates") {
    val plan = new given {
      indexOn("Awesome", "prop1").providesValues()
      indexOn("Awesome", "prop2").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 > 42 OR n.prop2 > 3 RETURN n.prop1, n.prop2"

    plan._2 should equal(
      Projection(
        Distinct(
          Union(
            IndexSeek("n:Awesome(prop1 > 42)", GetValue),
            IndexSeek("n:Awesome(prop2 > 3)", GetValue, propIds = Some(Map("prop2" -> 1)))
          ),
          Map("n" -> varFor("n"))
        ),
        Map("n.prop1" -> cachedNodeProp("n", "prop1"), "n.prop2" -> cachedNodeProp("n", "prop2"))
      )
    )
  }

  test("in an OR index plan should use cached values outside union for range predicates if they are on the same property") {
    val plan = new given {
      indexOn("Awesome", "prop1").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 > 42 OR n.prop1 < 3 RETURN n.prop1, n.prop2"

    plan._2 should equal(
      Projection(
        Distinct(
          Union(
            IndexSeek("n:Awesome(prop1 > 42)", GetValue),
            IndexSeek("n:Awesome(prop1 < 3)", GetValue)
          ),
          Map("n" -> varFor("n"))
        ),
        Map(cachedNodePropertyProj("n", "prop1"), "n.prop2" -> prop("n", "prop2"))
      )
    )
  }

  test("in an OR index plan should not use cached values outside union for equality predicates") {
    val plan = new given {
      indexOn("Awesome", "prop1").providesValues()
      indexOn("Awesome", "prop2").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 = 42 OR n.prop2 = 3 RETURN n.prop1, n.prop2"

    plan._2 should equal(
      Projection(
        Distinct(
          Union(
            IndexSeek("n:Awesome(prop2 = 3)", GetValue, propIds = Some(Map("prop2" -> 1))),
            IndexSeek("n:Awesome(prop1 = 42)", GetValue, propIds = Some(Map("prop1" -> 0)))
          ),
          Map("n" -> varFor("n"))
        ),
        Map("n.prop1" -> cachedNodeProp("n", "prop1"), "n.prop2" -> cachedNodeProp("n", "prop2"))
      )
    )
  }

  test("in an OR index plan with 4 indexes should not get values for equality predicates ") {
    val plan = new given {
      indexOn("Awesome", "prop1").providesValues()
      indexOn("Awesome", "prop2").providesValues()
      indexOn("Awesome2", "prop1").providesValues()
      indexOn("Awesome2", "prop2").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome:Awesome2) WHERE n.prop1 = 42 OR n.prop2 = 3 RETURN n.prop1, n.prop2"

    plan._2 should equal(
      Projection(
        Selection(
          Seq(hasLabels("n", "Awesome"), hasLabels("n", "Awesome2")),
          Distinct(
            Union(
              Union(
                Union(
                  IndexSeek("n:Awesome(prop2 = 3)", GetValue, propIds = Some(Map("prop2" -> 1))),
                  IndexSeek("n:Awesome2(prop2 = 3)", GetValue, propIds = Some(Map("prop2" -> 1)), labelId = 1)
                ),
                IndexSeek("n:Awesome(prop1 = 42)", GetValue, propIds = Some(Map("prop1" -> 0)))
              ),
              IndexSeek("n:Awesome2(prop1 = 42)", GetValue, propIds = Some(Map("prop1" -> 0)), labelId = 1)
            ),
            Map("n" -> varFor("n"))
          )
        ),
        Map("n.prop1" -> cachedNodeProp("n", "prop1"), "n.prop2" -> cachedNodeProp("n", "prop2"))
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

  test("should plan projection and index seek with GetValue when another predicate uses the property") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop <= 42 AND n.prop % 2 = 0 RETURN n.foo"

    plan._2 should equal(
      Projection(
        Selection(ands(equals(modulo(cachedNodeProp("n", "prop"), literalInt(2)), literalInt(0))),
          IndexSeek("n:Awesome(prop <= 42)", GetValue)),
        Map(propertyProj("n", "foo")))
    )
  }

  test("should plan projection and index seek with GetValue when another predicate uses the property 2") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome)-[r]->(m) WHERE n.prop <= 42 AND n.prop % m.foo = 0 RETURN n.foo"

    plan._2 should equal(
      Projection(
        Selection(ands(equals(modulo(cachedNodeProp("n", "prop"), prop("m", "foo")), literalInt(0))),
          Expand(
            CacheProperties(
              IndexSeek("n:Awesome(prop <= 42)", GetValue),
              Set(cachedNodeProp("n", "foo"))
            ),
            "n", SemanticDirection.OUTGOING, Seq.empty, "m", "r")
        ),
        Map("n.foo" -> cachedNodeProp("n", "foo")))
    )
  }

  test("should plan index seek with GetValue when the property is projected after a renaming projection") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n AS m MATCH (m)-[r]-(o) RETURN m.prop"

    plan._2 should equal(
      Projection(
        Expand(
          Projection(
            NodeIndexSeek(
              "n",
              LabelToken("Awesome", LabelId(0)),
              Seq(indexedProperty("prop", 0, GetValue)),
              SingleQueryExpression(literalInt(42)),
              Set.empty,
              IndexOrderNone),
            Map("m" -> varFor("n"))),
          "m", SemanticDirection.BOTH, Seq.empty, "o", "r"),
        Map("m.prop" -> cachedNodeProp("n", "prop")))
    )
  }

  test("should plan index seek with GetValue when the property is used in a predicate after a renaming projection") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 42 WITH n AS m MATCH (m)-[r]-(o) WHERE m.prop < 50 RETURN o"

    plan._2 should equal(
      Expand(
        Selection(ands(lessThan(cachedNodeProp("n", "prop"), literalInt(50))),
          Projection(
            IndexSeek("n:Awesome(prop > 42)", GetValue),
            Map("m" -> varFor("n")))),
        "m", SemanticDirection.BOTH, Seq.empty, "o", "r")
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
          Map(cachedNodePropertyProj("foo", "n", "prop"), "bar" -> trueLiteral)),
        Map("baz" -> varFor("bar")))
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
              Seq(indexedProperty("prop", 0, DoNotGetValue)),
              SingleQueryExpression(literalInt(42)),
              Set.empty,
              IndexOrderNone),
            Map("m" -> varFor("  n@7"))),
          "m", SemanticDirection.BOTH, Seq.empty, "  n@63", "r"),
        Map("n.prop" -> prop("  n@63", "prop"))
      )
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
        Map("toUpper(n.prop)" -> function("toUpper", cachedNodeProp("n", "prop")))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in ORDER BY") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN n.foo ORDER BY toUpper(n.prop)"

    plan._2 should equal(
      Projection(
        Sort(
          Projection(
            IndexSeek(
              "n:Awesome(prop = 'foo')", GetValue),
            Map("toUpper(n.prop)" -> function("toUpper", cachedNodeProp("n", "prop")))),
          Seq(Ascending("toUpper(n.prop)"))),
        Map("n.foo" -> prop("n", "foo"))
      )
    )
  }

  test("should plan index seek with GetValue when the property is part of an aggregating column") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN sum(n.prop), n.foo AS nums"

    plan._2 should equal(
      Aggregation(
        IndexSeek("n:Awesome(prop = 42)", GetValue),
        Map("nums" -> prop("n", "foo")),
        Map("sum(n.prop)" -> sum(cachedNodeProp("n", "prop")))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in key column of an aggregation and in ORDER BY") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN sum(n.foo), n.prop ORDER BY n.prop"

    plan._2 should equal(
      Sort(
        Aggregation(
          IndexSeek("n:Awesome(prop = 'foo')", GetValue),
          Map(cachedNodePropertyProj("n.prop", "n", "prop")),
          Map("sum(n.foo)" -> sum(prop("n", "foo")))
        ),
        Seq(Ascending("n.prop")))
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
        "foo", listOf(cachedNodeProp("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in a procedure call") {
    val signature = ProcedureSignature(QualifiedName(Seq.empty, "fooProcedure"),
      IndexedSeq(FieldSignature("input", CTString)),
      Some(IndexedSeq(FieldSignature("value", CTString))),
      None,
      ProcedureReadOnlyAccess(Array.empty),
      id = 42)

    val plan = new given {
      procedure(signature)
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' CALL fooProcedure(n.prop) YIELD value RETURN value"

    plan._2 should equal(
      ProcedureCall(
        IndexSeek("n:Awesome(prop = 'foo')", GetValue),
        ResolvedCall(signature,
          IndexedSeq(coerceTo(cachedNodeProp("n", "prop"), CTString)),
          IndexedSeq(ProcedureResultItem(None, varFor("value"))(pos)))(pos))
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

  // seek on merge unique index

  test("should plan seek with GetValue when the property is projected (merge unique index), but need a projection because of the Optional") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.prop"

    plan._2 should equal(
      Projection(
        AntiConditionalApply(
          Optional(
            NodeUniqueIndexSeek(
              "n",
              LabelToken("Awesome", LabelId(0)),
              Seq(indexedProperty("prop", 0, GetValue)),
              SingleQueryExpression(literalString("foo")),
              Set.empty,
              IndexOrderNone)
          ),
          MergeCreateNode(
            Argument(Set()),
            "n", Seq(labelName("Awesome")), Some(mapOf(("prop", literalString("foo"))))
          ),
          Seq("n")),
        Map("n.prop" -> cachedNodeProp("n", "prop")))
    )
  }

  test("for exact seeks, should even plan index seek with GetValue when the index does not provide values (merge unique index), but need a projection because of the Optional") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.prop"

    plan._2 should equal(
      Projection(
        AntiConditionalApply(
          Optional(
            NodeUniqueIndexSeek(
              "n",
              LabelToken("Awesome", LabelId(0)),
              Seq(indexedProperty("prop", 0, GetValue)),
              SingleQueryExpression(literalString("foo")),
              Set.empty,
              IndexOrderNone)
          ),
          MergeCreateNode(
            Argument(Set()),
            "n", Seq(labelName("Awesome")), Some(mapOf(("prop", literalString("foo"))))
          ),
          Seq("n")),
        Map("n.prop" -> cachedNodeProp("n", "prop")))
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
            NodeUniqueIndexSeek(
              "n",
              LabelToken("Awesome", LabelId(0)),
              Seq(indexedProperty("prop", 0, DoNotGetValue)),
              SingleQueryExpression(literalString("foo")),
              Set.empty,
              IndexOrderNone)
          ),
          MergeCreateNode(
            Argument(Set()),
            "n", Seq(labelName("Awesome")), Some(mapOf(("prop", literalString("foo"))))
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
        Map("n.bar" -> prop("n", "bar")))
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
          Seq(indexedProperty("prop", 0, GetValue), indexedProperty("foo", 1, DoNotGetValue)),
          CompositeQueryExpression(Seq(SingleQueryExpression(literalInt(42)), SingleQueryExpression(literalInt(21)))),
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

  // EXISTS

  test("should plan exists scan with GetValue when the property is projected") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE exists(n.prop) RETURN n.prop"

    plan._2 should equal(
      Projection(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue)),
          Set.empty,
          IndexOrderNone),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan exists scan with DoNotGetValue when the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE exists(n.prop) RETURN n.prop"

    plan._2 should equal(
      Projection(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue)),
          Set.empty,
          IndexOrderNone),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  // EXISTENCE / NODE KEY CONSTRAINT

  test("should plan scan with GetValue when existence constraint on projected property") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      existenceOrNodeKeyConstraintOn("Awesome", Set("prop"))
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop"

    plan._2 should equal(
      Projection(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue)),
          Set.empty,
          IndexOrderNone),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan scan with DoNotGetValue when existence constraint but the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop")
      existenceOrNodeKeyConstraintOn("Awesome", Set("prop"))
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop"

    plan._2 should equal(
      Projection(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue)),
          Set.empty,
          IndexOrderNone),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test("should plan scan with GetValue when composite existence constraint on projected property") {
    val plan = new given {
      indexOn("Awesome", "prop2").providesValues()
      existenceOrNodeKeyConstraintOn("Awesome", Set("prop1", "prop2"))
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop2"

    plan._2 should equal(
      Projection(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 0, GetValue)),
          Set.empty,
          IndexOrderNone),
        Map(cachedNodePropertyProj("n", "prop2"))
      )
    )
  }

  test("should plan scan with DoNotGetValue when composite existence constraint but the index does not provide values") {
    val plan = new given {
      indexOn("Awesome", "prop2")
      existenceOrNodeKeyConstraintOn("Awesome", Set("prop1", "prop2"))
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop2"

    plan._2 should equal(
      Projection(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 0, DoNotGetValue)),
          Set.empty,
          IndexOrderNone),
        Map(propertyProj("n", "prop2"))
      )
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

  test("should use cached access after projection of non returned property") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
      indexOn("Awesome", "foo").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop < 2 RETURN n.prop ORDER BY n.foo"

    plan._2 should equal(
      Projection(
        Sort(
          Projection(
            IndexSeek("n:Awesome(prop < 2)", GetValue),
            Map("n.foo" -> prop("n", "foo"))),
          Seq(Ascending("n.foo"))
        ),
        Map("n.prop" -> cachedNodeProp("n", "prop"))
      )
    )
  }

  // AGGREGATIONS (=> implicit exists)

  test("should plan scan with GetValue when the property is used in avg function") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN avg(n.prop)"

    plan._2 should equal(
      Aggregation(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue)),
          Set.empty,
          IndexOrderNone),
        Map.empty,
        Map(s"avg(n.prop)" -> avg(cachedNodeProp("n", "prop")))
      )
    )
  }

  test("should plan scan with DoNotGetValue when the property is used in avg function") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN avg(n.prop)"

    plan._2 should equal(
      Aggregation(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue)),
          Set.empty,
          IndexOrderNone),
        Map.empty,
        Map("avg(n.prop)" -> avg(prop("n", "prop")))
      )
    )
  }

  test("should plan scan with GetValue when the property is used in sum function") {
    val plan = new given {
      indexOn("Awesome", "prop").providesValues()
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN sum(n.prop)"

    plan._2 should equal(
      Aggregation(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue)),
          Set.empty,
          IndexOrderNone),
        Map.empty,
        Map("sum(n.prop)" -> sum(cachedNodeProp("n", "prop")))
      )
    )
  }

  test("should plan scan with DoNotGetValue when the property is used in sum function") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN sum(n.prop)"

    plan._2 should equal(
      Aggregation(
        NodeIndexScan(
          "n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue)),
          Set.empty,
          IndexOrderNone),
        Map.empty,
        Map("sum(n.prop)" -> sum(prop("n", "prop")))
      )
    )
  }

  private def cachedNodePropertyProj(node: String, property: String) =
    s"$node.$property" -> cachedNodeProp(node, property)

  private def cachedNodePropertyProj(alias: String, node: String, property: String ) =
    alias -> cachedNodeProp(node, property)

  private def propertyProj(node: String, property: String ) =
    s"$node.$property" -> prop(node, property)

}
