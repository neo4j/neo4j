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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class IndexWithValuesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2
    with LogicalPlanningIntegrationTestSupport with PlanMatchHelp {

  // or planner between two indexes

  test("in an OR index plan should use cached values outside union for range predicates") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop1").providesValues()
        indexOn("Awesome", "prop2").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 > 42 OR n.prop2 > 3 RETURN n.prop1, n.prop2"

    Seq(
      nodeIndexSeek("n:Awesome(prop1 > 42)", _ => GetValue),
      nodeIndexSeek("n:Awesome(prop2 > 3)", _ => GetValue, propIds = Some(Map("prop2" -> 1)))
    ).permutations.map {
      case Seq(seek1, seek2) =>
        Projection(
          CacheProperties(
            Distinct(
              Union(
                seek1,
                seek2
              ),
              Map(v"n" -> v"n")
            ),
            Set(cachedNodeProp("n", "prop1"), cachedNodeProp("n", "prop2"))
          ),
          Map(v"n.prop1" -> cachedNodeProp("n", "prop1"), v"n.prop2" -> cachedNodeProp("n", "prop2"))
        )
      case _ => sys.error("the impossible happened")
    }.toSeq should contain(plan._1)
  }

  test(
    "in an OR index plan should use cached values outside union for range predicates if they are on the same property"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop1").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 > 42 OR n.prop1 < 3 RETURN n.prop1, n.prop2"
    Seq(
      nodeIndexSeek("n:Awesome(prop1 > 42)", _ => GetValue),
      nodeIndexSeek("n:Awesome(prop1 < 3)", _ => GetValue)
    ).permutations.map {
      case Seq(seek1, seek2) =>
        Projection(
          CacheProperties(
            Distinct(
              Union(
                seek1,
                seek2
              ),
              Map(v"n" -> v"n")
            ),
            Set(cachedNodeProp("n", "prop1"), cachedNodeProp("n", "prop2", "n", knownToAccessStore = true))
          ),
          Map(cachedNodePropertyProj("n", "prop1"), v"n.prop2" -> cachedNodeProp("n", "prop2"))
        )
      case _ => sys.error("the impossible happened")
    }.toSeq should contain(plan._1)
  }

  test("in an OR index plan should use cached values outside union for equality predicates") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop1").providesValues()
        indexOn("Awesome", "prop2").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 = 42 OR n.prop2 = 3 RETURN n.prop1, n.prop2"

    Seq(
      nodeIndexSeek("n:Awesome(prop2 = 3)", _ => GetValue, propIds = Some(Map("prop2" -> 1))),
      nodeIndexSeek("n:Awesome(prop1 = 42)", _ => GetValue, propIds = Some(Map("prop1" -> 0)))
    ).permutations.map {
      case Seq(seek1, seek2) =>
        Projection(
          CacheProperties(
            Distinct(
              Union(
                seek1,
                seek2
              ),
              Map(v"n" -> v"n")
            ),
            Set(cachedNodeProp("n", "prop1"), cachedNodeProp("n", "prop2", "n", knownToAccessStore = false))
          ),
          Map(v"n.prop1" -> cachedNodeProp("n", "prop1"), v"n.prop2" -> cachedNodeProp("n", "prop2"))
        )
      case _ => sys.error("the impossible happened")
    }.toSeq should contain(plan._1)
  }

  test("in an OR index plan with 4 indexes should get values for equality predicates") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop1").providesValues()
        indexOn("Awesome", "prop2").providesValues()
        indexOn("Awesome2", "prop1").providesValues()
        indexOn("Awesome2", "prop2").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome:Awesome2) WHERE n.prop1 = 42 OR n.prop2 = 3 RETURN n.prop1, n.prop2"

    // We don't want to assert on the produce results or the projection in this test
    val Projection(unionPlan, _) = plan._1.asInstanceOf[Projection]

    val hasLabel1 = hasLabels("n", "Awesome")
    val hasLabel2 = hasLabels("n", "Awesome2")
    val seekLabel1Prop1 = nodeIndexSeek("n:Awesome(prop1 = 42)", _ => GetValue, propIds = Some(Map("prop1" -> 0)))
    val seekLabel1Prop2 = nodeIndexSeek("n:Awesome(prop2 = 3)", _ => GetValue, propIds = Some(Map("prop2" -> 1)))
    val seekLabel2Prop1 =
      nodeIndexSeek("n:Awesome2(prop1 = 42)", _ => GetValue, propIds = Some(Map("prop1" -> 0)), labelId = 1)
    val seekLabel2Prop2 =
      nodeIndexSeek("n:Awesome2(prop2 = 3)", _ => GetValue, propIds = Some(Map("prop2" -> 1)), labelId = 1)

    val coveringCombinations = Seq(
      (seekLabel1Prop1, hasLabel2),
      (seekLabel1Prop2, hasLabel2),
      (seekLabel2Prop1, hasLabel1),
      (seekLabel2Prop2, hasLabel1)
    )

    val planAlternatives =
      for {
        Seq((seek1, filter1), (seek2, filter2)) <- coveringCombinations.permutations.map(_.take(2)).toSeq
      } yield CacheProperties(
        Distinct(
          Union(
            Selection(Seq(filter1), seek1),
            Selection(Seq(filter2), seek2)
          ),
          Map(v"n" -> v"n")
        ),
        Set(cachedNodeProp("n", "prop1"), cachedNodeProp("n", "prop2"))
      )

    planAlternatives should contain(unionPlan)

  }

  // Index exact seeks

  test("should plan index seek with GetValue when the property is projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("for exact seeks, should even plan index seek with GetValue when the index does not provide values") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with DoNotGetValue when another property is projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
        indexOn("Awesome", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42)", _ => DoNotGetValue),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when two properties are projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
        indexOn("Awesome", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.foo, n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
        Map(propertyProj("n", "foo"), cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when another predicate uses the property") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop <= 42 AND n.prop % 2 = 0 RETURN n.foo"

    plan._1 should equal(
      Projection(
        Selection(
          ands(equals(modulo(cachedNodeProp("n", "prop"), literalInt(2)), literalInt(0))),
          nodeIndexSeek("n:Awesome(prop <= 42)", _ => GetValue)
        ),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when another predicate uses the property 2") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome)-[r]->(m) WHERE n.prop <= 42 AND n.prop % m.foo = 0 RETURN n.foo"

    plan._1 should equal(
      Projection(
        Selection(
          ands(equals(modulo(cachedNodeProp("n", "prop"), prop("m", "foo")), literalInt(0))),
          Expand(
            CacheProperties(
              nodeIndexSeek("n:Awesome(prop <= 42)", _ => GetValue),
              Set(cachedNodePropFromStore("n", "foo"))
            ),
            v"n",
            SemanticDirection.OUTGOING,
            Seq.empty,
            v"m",
            v"r"
          )
        ),
        Map(v"n.foo" -> cachedNodeProp("n", "foo"))
      )
    )
  }

  test("should plan index seek with GetValue when the property is projected after a renaming projection") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n AS m MATCH (m)-[r]-(o) RETURN m.prop"

    plan._1 should equal(
      Projection(
        Expand(
          Projection(
            NodeIndexSeek(
              v"n",
              LabelToken("Awesome", LabelId(0)),
              Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
              SingleQueryExpression(literalInt(42)),
              Set.empty,
              IndexOrderNone,
              IndexType.RANGE,
              supportPartitionedScan = true
            ),
            Map(v"m" -> v"n")
          ),
          v"m",
          SemanticDirection.BOTH,
          Seq.empty,
          v"o",
          v"r"
        ),
        Map(v"m.prop" -> cachedNodeProp("n", "prop", "m"))
      )
    )
  }

  test("should plan index seek with GetValue when the property is used in a predicate after a renaming projection") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 42 WITH n AS m MATCH (m)-[r]-(o) WHERE m.prop < 50 RETURN o"

    plan._1 should equal(
      Expand(
        Selection(
          ands(lessThan(cachedNodeProp("n", "prop", "m"), literalInt(50))),
          Projection(
            nodeIndexSeek("n:Awesome(prop > 42)", _ => GetValue),
            Map(v"m" -> v"n")
          )
        ),
        v"m",
        SemanticDirection.BOTH,
        Seq.empty,
        v"o",
        v"r"
      )
    )
  }

  test("should plan index seek with GetValue when the property is projected and renamed in a RETURN") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n.prop AS foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
        Map(cachedNodePropertyProj("foo", "n", "prop"))
      )
    )
  }

  test("should plan index seek with GetValue when the property is projected and renamed in a WITH") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n.prop AS foo, true AS bar RETURN foo, bar AS baz"

    plan._1 should equal(
      Projection(
        Projection(
          nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
          Map(cachedNodePropertyProj("foo", "n", "prop"), v"bar" -> trueLiteral)
        ),
        Map(v"baz" -> v"bar")
      )
    )
  }

  test("should not be fooled to use a variable when the node variable is defined twice") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n as m MATCH (m)-[r]-(n) RETURN n.prop"

    plan._1 should equal(
      Projection(
        Expand(
          Projection(
            NodeIndexSeek(
              v"n",
              LabelToken("Awesome", LabelId(0)),
              Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
              SingleQueryExpression(literalInt(42)),
              Set.empty,
              IndexOrderNone,
              IndexType.RANGE,
              supportPartitionedScan = true
            ),
            Map(v"m" -> v"n")
          ),
          v"m",
          SemanticDirection.BOTH,
          Seq.empty,
          v"n",
          v"r"
        ),
        Map(v"n.prop" -> prop("n", "prop"))
      )
    )
  }

  test("should plan index seek with GetValue when the property is projected before the property access") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 WITH n MATCH (m)-[r]-(n) RETURN n.prop"

    plan._1 should equal(
      Projection(
        Expand(
          nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
          v"n",
          SemanticDirection.BOTH,
          Seq.empty,
          v"m",
          v"r"
        ),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is projected inside of a function") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN toUpper(n.prop)"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 'foo')", _ => GetValue),
        Map(v"toUpper(n.prop)" -> function("toUpper", cachedNodeProp("n", "prop")))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in ORDER BY") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN n.foo ORDER BY toUpper(n.prop)"

    plan._1 should equal(
      Projection(
        Sort(
          Projection(
            nodeIndexSeek(
              "n:Awesome(prop = 'foo')",
              _ => GetValue
            ),
            Map(v"toUpper(n.prop)" -> function("toUpper", cachedNodeProp("n", "prop")))
          ),
          Seq(Ascending(v"toUpper(n.prop)"))
        ),
        Map(v"n.foo" -> prop("n", "foo"))
      )
    )
  }

  test("should plan index seek with GetValue when the property is part of an aggregating column") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN sum(n.prop), n.foo AS nums"

    plan._1 should equal(
      Aggregation(
        nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
        Map(v"nums" -> prop("n", "foo")),
        Map(v"sum(n.prop)" -> sum(cachedNodeProp("n", "prop")))
      )
    )
  }

  test(
    "should plan projection and index seek with GetValue when the property is used in key column of an aggregation and in ORDER BY"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' RETURN sum(n.foo), n.prop ORDER BY n.prop"

    plan._1 should equal(
      Sort(
        Aggregation(
          nodeIndexSeek("n:Awesome(prop = 'foo')", _ => GetValue),
          Map(cachedNodePropertyProj("n.prop", "n", "prop")),
          Map(v"sum(n.foo)" -> sum(prop("n", "foo")))
        ),
        Seq(Ascending(v"n.prop"))
      )
    )
  }

  test("should plan index seek with GetValue when the property is part of a distinct column") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN DISTINCT n.prop"

    plan._1 should equal(
      Distinct(
        nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in an unwind projection") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' UNWIND [n.prop] AS foo RETURN foo"

    plan._1 should equal(
      UnwindCollection(
        nodeIndexSeek("n:Awesome(prop = 'foo')", _ => GetValue),
        v"foo",
        listOf(cachedNodeProp("n", "prop"))
      )
    )
  }

  test("should plan projection and index seek with GetValue when the property is used in a procedure call") {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "fooProcedure"),
      IndexedSeq(FieldSignature("input", CTString)),
      Some(IndexedSeq(FieldSignature("value", CTString))),
      None,
      ProcedureReadOnlyAccess,
      id = 42
    )

    val plan =
      new givenConfig {
        procedure(signature)
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 'foo' CALL fooProcedure(n.prop) YIELD value RETURN value"

    plan._1 should equal(
      ProcedureCall(
        nodeIndexSeek("n:Awesome(prop = 'foo')", _ => GetValue),
        ResolvedCall(
          signature,
          IndexedSeq(coerceTo(cachedNodeProp("n", "prop"), CTString)),
          IndexedSeq(ProcedureResultItem(None, v"value")(pos))
        )(pos)
      )
    )
  }

  // STARTS WITH seek

  test("should plan starts with seek with GetValue when the property is projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop STARTS WITH 'foo')", _ => GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and starts with seek with DoNotGetValue when the index does not provide values") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop STARTS WITH 'foo')", _ => DoNotGetValue),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and starts with seek with DoNotGetValue when another property is projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
        indexOn("Awesome", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop STARTS WITH 'foo')", _ => DoNotGetValue),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  // RANGE seek

  test("should plan range seek with GetValue when the property is projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop > 'foo')", _ => GetValue),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and range seek with DoNotGetValue when the index does not provide values") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop > 'foo')", _ => DoNotGetValue),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and range seek with DoNotGetValue when another property is projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
        indexOn("Awesome", "foo")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop > 'foo')", _ => DoNotGetValue),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  // RANGE seek on unique index

  test("should plan range seek with GetValue when the property is projected (unique index)") {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop > 'foo')", _ => GetValue, unique = true),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test(
    "should plan projection and range seek with DoNotGetValue when the index does not provide values (unique index)"
  ) {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop > 'foo')", _ => DoNotGetValue, unique = true),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and range seek with DoNotGetValue when another property is projected (unique index)") {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop")
        uniqueIndexOn("Awesome", "foo")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop > 'foo')", _ => DoNotGetValue, unique = true),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  // seek on merge unique index

  test(
    "should plan seek with GetValue when the property is projected (merge unique index), but need a projection because of the Optional"
  ) {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.prop"

    plan._1 should equal(
      Projection(
        Merge(
          NodeUniqueIndexSeek(
            v"n",
            LabelToken("Awesome", LabelId(0)),
            Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
            SingleQueryExpression(literalString("foo")),
            Set.empty,
            IndexOrderNone,
            IndexType.RANGE,
            supportPartitionedScan = true
          ),
          Seq(createNodeWithProperties("n", Seq("Awesome"), "{prop: 'foo'}")),
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        Map(v"n.prop" -> cachedNodeProp("n", "prop"))
      )
    )
  }

  test(
    "for exact seeks, should even plan index seek with GetValue when the index does not provide values (merge unique index), but need a projection because of the Optional"
  ) {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop")
      } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.prop"

    plan._1 should equal(
      Projection(
        Merge(
          NodeUniqueIndexSeek(
            v"n",
            LabelToken("Awesome", LabelId(0)),
            Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
            SingleQueryExpression(literalString("foo")),
            Set.empty,
            IndexOrderNone,
            IndexType.RANGE,
            supportPartitionedScan = true
          ),
          Seq(createNodeWithProperties("n", Seq("Awesome"), "{prop: 'foo'}")),
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        Map(v"n.prop" -> cachedNodeProp("n", "prop"))
      )
    )
  }

  test(
    "should plan projection and range seek with DoNotGetValue when another property is projected (merge unique index)"
  ) {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop")
        uniqueIndexOn("Awesome", "foo")
      } getLogicalPlanFor "MERGE (n:Awesome {prop: 'foo'}) RETURN n.foo"

    plan._1 should equal(
      Projection(
        Merge(
          NodeUniqueIndexSeek(
            v"n",
            LabelToken("Awesome", LabelId(0)),
            Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
            SingleQueryExpression(literalString("foo")),
            Set.empty,
            IndexOrderNone,
            IndexType.RANGE,
            supportPartitionedScan = true
          ),
          Seq(createNodeWithProperties("n", Seq("Awesome"), "{prop: 'foo'}")),
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  // composite index

  test("should plan index seek with GetValue when the property is projected (composite index)") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.prop, n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42, foo = 21)", _ => GetValue, supportPartitionedScan = false),
        Map(cachedNodePropertyProj("n", "prop"), cachedNodePropertyProj("n", "foo"))
      )
    )
  }

  test(
    "for exact seeks, should even plan index seek with GetValue when the index does not provide values (composite index)"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop", "foo")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.prop, n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42, foo = 21)", _ => GetValue, supportPartitionedScan = false),
        Map(cachedNodePropertyProj("n", "prop"), cachedNodePropertyProj("n", "foo"))
      )
    )
  }

  test(
    "should plan projection and index seek with DoNotGetValue when another property is projected (composite index)"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.bar"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop = 42, foo = 21)", _ => DoNotGetValue, supportPartitionedScan = false),
        Map(v"n.bar" -> prop("n", "bar"))
      )
    )
  }

  test("should plan index seek with GetValue and DoNotGetValue when only one property is projected (composite index)") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.foo = 21 RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek(
          "n:Awesome(prop = 42, foo = 21)",
          Map("prop" -> GetValue, "foo" -> DoNotGetValue),
          supportPartitionedScan = false
        ),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  // CONTAINS scan

  test("should plan index contains scan with GetValue when the property is projected") {
    val plan =
      new givenConfig {
        textIndexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop CONTAINS 'foo')", _ => GetValue, indexType = IndexType.TEXT),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index contains scan with DoNotGetValue when the index does not provide values") {
    val plan =
      new givenConfig {
        textIndexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop CONTAINS 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index contains scan with DoNotGetValue when another property is projected") {
    val plan =
      new givenConfig {
        textIndexOn("Awesome", "prop").providesValues()
        textIndexOn("Awesome", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop CONTAINS 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  test("should plan index contains scan with GetValue when the relationship property is projected") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop CONTAINS 'foo' RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, indexType = IndexType.TEXT)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`r.prop`")
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop CONTAINS 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => GetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  test(
    "should plan relationship projection and index contains scan with DoNotGetValue when the index does not provide values"
  ) {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop CONTAINS 'foo' RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`r.prop`")
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop CONTAINS 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  test(
    "should plan relationship projection and index contains scan with DoNotGetValue when another property is projected"
  ) {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop CONTAINS 'foo' RETURN r.foo"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, indexType = IndexType.TEXT)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`r.foo`")
        .projection("r.foo AS `r.foo`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop CONTAINS 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  // EXISTS

  test("should plan exists scan with GetValue when the property is projected") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN n.prop"

    plan._1 should equal(
      Projection(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test(s"should plan exists scan with GetValue when the relationship property is projected") {
    val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true)
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build())
  }

  test(
    s"should plan exists scan with DoNotGetValue when the a relationship property is projected, but from a different variable"
  ) {
    val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL WITH count(*) AS count MATCH (a)-[r]-(b) RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true)
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop AS `r.prop`")
      .apply()
      .|.allRelationshipsScan("(a)-[r]-(b)", "count")
      .aggregation(Seq(), Seq("count(*) AS count"))
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", getValue = _ => DoNotGetValue, indexType = IndexType.RANGE)
      .build())
  }

  test("should plan exists scan with DoNotGetValue when the index does not provide values") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN n.prop"

    plan._1 should equal(
      Projection(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test(s"should plan exists scan with DoNotGetValue when the relationship index does not provide values") {
    val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop AS `r.prop`")
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", getValue = _ => DoNotGetValue, indexType = IndexType.RANGE)
      .build())
  }

  // EXISTENCE / NODE KEY CONSTRAINT

  test("should plan scan with GetValue when existence constraint on projected property") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
        nodePropertyExistenceConstraintOn("Awesome", Set("prop"))
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop"

    plan._1 should equal(
      Projection(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan scan (relationship) with GetValue when existence constraint on projected property") {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build())
  }

  test("should plan scan with DoNotGetValue when existence constraint but the index does not provide values") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
        nodePropertyExistenceConstraintOn("Awesome", Set("prop"))
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop"

    plan._1 should equal(
      Projection(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test(
    "should plan scan (relationship) with DoNotGetValue when existence constraint  but the index does not provide values"
  ) {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop AS `r.prop`")
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", getValue = _ => DoNotGetValue, indexType = IndexType.RANGE)
      .build())
  }

  test("should plan scan with GetValue when composite existence constraint on projected property") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop2").providesValues()
        nodePropertyExistenceConstraintOn("Awesome", Set("prop1", "prop2"))
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop2"

    plan._1 should equal(
      Projection(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 0, GetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map(cachedNodePropertyProj("n", "prop2"))
      )
    )
  }

  test("should plan scan (relationship) with GetValue when composite existence constraint on projected property") {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN r.prop2"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop1", "prop2"), 1.0, 0.01, withValues = true)
      .addRelationshipExistenceConstraint("REL", "prop1")
      .addRelationshipExistenceConstraint("REL", "prop2")
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    withClue("Index scan on two properties should be planned if they are only available through constraint") {
      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop2] AS `r.prop2`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop1, prop2)]-(b)",
          getValue = Map("prop1" -> DoNotGetValue, "prop2" -> GetValue),
          indexType = IndexType.RANGE
        )
        .build())
    }

  }

  test("should plan scan (node) with GetValue when composite existence constraint on projected property") {
    val query = s"MATCH (a:A) RETURN a.prop2"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .addNodeIndex("A", Seq("prop1", "prop2"), 1.0, 0.01, withValues = true)
      .addNodeExistenceConstraint("A", "prop1")
      .addNodeExistenceConstraint("A", "prop2")
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    withClue("Index scan on two properties should be planned if they are only available through constraint") {
      plan should equal(planner.subPlanBuilder()
        .projection("cacheN[a.prop2] AS `a.prop2`")
        .nodeIndexOperator(
          "a:A(prop1, prop2)",
          getValue = Map("prop1" -> DoNotGetValue, "prop2" -> GetValue),
          indexType = IndexType.RANGE
        )
        .build())
    }
  }

  test(
    "should plan scan with DoNotGetValue when composite existence constraint but the index does not provide values"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop2")
        nodePropertyExistenceConstraintOn("Awesome", Set("prop1", "prop2"))
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN n.prop2"

    plan._1 should equal(
      Projection(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 0, DoNotGetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map(propertyProj("n", "prop2"))
      )
    )
  }

  test(
    "should plan scan (relationship) with DoNotGetValue when composite existence constraint but the index does not provide values"
  ) {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN r.prop2"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop1", "prop2"), 1.0, 0.01)
      .addRelationshipExistenceConstraint("REL", "prop1")
      .addRelationshipExistenceConstraint("REL", "prop2")
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop2 AS `r.prop2`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1, prop2)]-(b)",
        getValue = _ => DoNotGetValue,
        indexType = IndexType.RANGE
      )
      .build())
  }

  // ENDS WITH scan

  test("should plan index ends with scan with GetValue when the property is projected") {
    val plan =
      new givenConfig {
        textIndexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop ENDS WITH 'foo')", _ => GetValue, indexType = IndexType.TEXT),
        Map(cachedNodePropertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index ends with scan with DoNotGetValue when the index does not provide values") {
    val plan =
      new givenConfig {
        textIndexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.prop"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop ENDS WITH 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT),
        Map(propertyProj("n", "prop"))
      )
    )
  }

  test("should plan projection and index ends with scan with DoNotGetValue when another property is projected") {
    val plan =
      new givenConfig {
        textIndexOn("Awesome", "prop").providesValues()
        textIndexOn("Awesome", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.foo"

    plan._1 should equal(
      Projection(
        nodeIndexSeek("n:Awesome(prop ENDS WITH 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT),
        Map(propertyProj("n", "foo"))
      )
    )
  }

  test("should use cached access after projection of non returned property") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
        indexOn("Awesome", "foo").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop < 2 RETURN n.prop ORDER BY n.foo"

    plan._1 should equal(
      Projection(
        Sort(
          Projection(
            nodeIndexSeek("n:Awesome(prop < 2)", _ => GetValue),
            Map(v"n.foo" -> prop("n", "foo"))
          ),
          Seq(Ascending(v"n.foo"))
        ),
        Map(v"n.prop" -> cachedNodeProp("n", "prop"))
      )
    )
  }

  test("should plan index ends with scan with GetValue when the relationship property is projected") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop ENDS WITH 'foo' RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, indexType = IndexType.TEXT)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`r.prop`")
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop ENDS WITH 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => GetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  test(
    "should plan relationship projection and index ends with scan with DoNotGetValue when the index does not provide values"
  ) {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop ENDS WITH 'foo' RETURN r.prop"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`r.prop`")
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop ENDS WITH 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  test(
    "should plan relationship projection and index ends with scan with DoNotGetValue when another property is projected"
  ) {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop ENDS WITH 'foo' RETURN r.foo"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, indexType = IndexType.TEXT)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`r.foo`")
        .projection("r.foo AS `r.foo`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop ENDS WITH 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  // AGGREGATIONS (=> implicit exists)

  test("should plan scan with GetValue when the property is used in avg function") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN avg(n.prop)"

    plan._1 should equal(
      Aggregation(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map.empty,
        Map(v"avg(n.prop)" -> avg(cachedNodeProp("n", "prop")))
      )
    )
  }

  test("should plan scan (relationship) with GetValue when the property is used in avg function") {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN avg(r.prop)"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true)
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("avg(cacheR[r.prop]) AS `avg(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build())
  }

  test("should plan scan with DoNotGetValue when the property is used in avg function") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN avg(n.prop)"

    plan._1 should equal(
      Aggregation(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map.empty,
        Map(v"avg(n.prop)" -> avg(prop("n", "prop")))
      )
    )
  }

  test("should plan scan (relationship) with DoNotGetValue when the property is used in avg function") {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN avg(r.prop)"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("avg(r.prop) AS `avg(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", indexType = IndexType.RANGE)
      .build())
  }

  test("should plan scan with GetValue when the property is used in sum function") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN sum(n.prop)"

    plan._1 should equal(
      Aggregation(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map.empty,
        Map(v"sum(n.prop)" -> sum(cachedNodeProp("n", "prop")))
      )
    )
  }

  test("should plan scan (relationship) with GetValue when the property is used in sum function") {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN sum(r.prop)"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true)
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("sum(cacheR[r.prop]) AS `sum(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build())
  }

  test("should plan scan with DoNotGetValue when the property is used in sum function") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN sum(n.prop)"

    plan._1 should equal(
      Aggregation(
        NodeIndexScan(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        Map.empty,
        Map(v"sum(n.prop)" -> sum(prop("n", "prop")))
      )
    )
  }

  test("should plan scan (relationship) with DoNotGetValue when the property is used in sum function") {
    val query = s"MATCH (a)-[r:REL]-(b) RETURN sum(r.prop)"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("sum(r.prop) AS `sum(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", indexType = IndexType.RANGE)
      .build())
  }

  private def relIndexSeekConfig(withValues: Boolean) =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = withValues)

  test("should plan seek with GetValue when the relationship property is projected") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop = 123 RETURN r.prop"

    val planner = relIndexSeekConfig(withValues = true).build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .relationshipIndexOperator("(a)-[r:REL(prop = 123)]-(b)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build())
  }

  test(
    "should plan seek with DoNotGetValue when the a relationship property is projected, but from a different variable"
  ) {
    val query =
      """MATCH (a)-[r:REL]-(b)
        |WHERE r.prop = 123
        |WITH count(*) AS count
        |MATCH (a)-[r]-(b)
        |RETURN r.prop""".stripMargin

    val planner = relIndexSeekConfig(withValues = true).build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop AS `r.prop`")
      .apply()
      .|.allRelationshipsScan("(a)-[r]-(b)", "count")
      .aggregation(Seq(), Seq("count(*) AS count"))
      .relationshipIndexOperator(
        "(a)-[r:REL(prop = 123)]-(b)",
        getValue = _ => DoNotGetValue,
        indexType = IndexType.RANGE
      )
      .build())
  }

  test("should plan seek with DoNotGetValue when the relationship index does not provide values") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 RETURN r.prop"

    val planner = relIndexSeekConfig(withValues = false).build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop AS `r.prop`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop > 123)]-(b)",
        getValue = _ => DoNotGetValue,
        indexType = IndexType.RANGE
      )
      .build())
  }

  test("should plan seek (relationship) with GetValue when the property is used in avg function") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 RETURN avg(r.prop)"

    val planner = relIndexSeekConfig(withValues = true).build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("avg(cacheR[r.prop]) AS `avg(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop > 123)]-(b)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build())
  }

  test("should plan seek (relationship) with DoNotGetValue when the property is used in avg function") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 RETURN avg(r.prop)"

    val planner = relIndexSeekConfig(withValues = false).build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("avg(r.prop) AS `avg(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop > 123)]-(b)", indexType = IndexType.RANGE)
      .build())
  }

  test("should plan seek (relationship) with GetValue when the property is used in sum function") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 RETURN sum(r.prop)"

    val planner = relIndexSeekConfig(withValues = true).build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("sum(cacheR[r.prop]) AS `sum(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop > 123)]-(b)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build())
  }

  test("should plan seek (relationship) with DoNotGetValue when the property is used in sum function") {
    val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 RETURN sum(r.prop)"

    val planner = relIndexSeekConfig(withValues = false).build()

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("sum(r.prop) AS `sum(r.prop)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop > 123)]-(b)", indexType = IndexType.RANGE)
      .build())
  }

  private def relCompositeIndexSeekConfig(withValues: Boolean): StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop1", "prop2"), 1.0, 0.01, withValues = withValues)
      .addRelationshipExistenceConstraint("REL", "prop1")
      .addRelationshipExistenceConstraint("REL", "prop2")
      .build()

  test("should plan seek (relationship) with GetValue when composite existence constraint on projected property") {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop1 > 123 RETURN r.prop2"

    val planner = relCompositeIndexSeekConfig(withValues = true)

    val plan = planner
      .plan(query)
      .stripProduceResults

    withClue("Index seek on two properties should be planned if they are only available through constraint") {
      plan shouldBe planner.subPlanBuilder()
        .projection("cacheR[r.prop2] AS `r.prop2`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop1 > 123, prop2)]-(b)",
          getValue = Map("prop1" -> DoNotGetValue, "prop2" -> GetValue),
          indexType = IndexType.RANGE,
          supportPartitionedScan = false
        )
        .build()
    }
  }

  test(
    "should plan seek (relationship) with DoNotGetValue when composite existence constraint but the index does not provide values"
  ) {
    val query = "MATCH (a)-[r:REL]-(b) WHERE r.prop1 = 123 RETURN r.prop2"

    val planner = relCompositeIndexSeekConfig(withValues = false)

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop2 AS `r.prop2`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 123, prop2)]-(b)",
        getValue = _ => DoNotGetValue,
        indexType = IndexType.RANGE,
        supportPartitionedScan = false
      )
      .build())
  }

  test("should plan relationship index seek with GetValue when the property is projected (composite index)") {
    val planner = relCompositeIndexSeekConfig(withValues = true)
    val q = "MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 21 RETURN r.prop1, r.prop2"
    val plan = planner.plan(q).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .projection("cacheR[r.prop1] AS `r.prop1`", "cacheR[r.prop2] AS `r.prop2`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 42, prop2 = 21)]->(b)",
        getValue = _ => GetValue,
        indexType = IndexType.RANGE,
        supportPartitionedScan = false
      )
      .build()
  }

  test(
    "for exact seeks, should even plan relationship index seek with GetValue when the index does not provide values (composite index)"
  ) {
    val planner = relCompositeIndexSeekConfig(withValues = false)
    val q = "MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 21 RETURN r.prop1, r.prop2"
    val plan = planner.plan(q).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .projection("cacheR[r.prop1] AS `r.prop1`", "cacheR[r.prop2] AS `r.prop2`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 42, prop2 = 21)]->(b)",
        getValue = _ => GetValue,
        indexType = IndexType.RANGE,
        supportPartitionedScan = false
      )
      .build()
  }

  test(
    "should plan projection and index seek with DoNotGetValue when another property is projected (composite relationship index)"
  ) {
    val planner = relCompositeIndexSeekConfig(withValues = true)
    val q = "MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 21 RETURN r.otherProp"
    val plan = planner.plan(q).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .projection("r.otherProp AS `r.otherProp`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 42, prop2 = 21)]->(b)",
        getValue = _ => DoNotGetValue,
        indexType = IndexType.RANGE,
        supportPartitionedScan = false
      )
      .build()
  }

  test(
    "should plan relationship index seek with GetValue and DoNotGetValue when only one property is projected (composite index)"
  ) {
    val planner = relCompositeIndexSeekConfig(withValues = true)
    val q = "MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 21 RETURN r.prop2"
    val plan = planner.plan(q).stripProduceResults

    plan.leftmostLeaf should matchPattern {
      case d: DirectedRelationshipIndexSeek
        if d.properties.map(p => p.propertyKeyToken.name -> p.getValueFromIndex) ==
          Seq("prop1" -> DoNotGetValue, "prop2" -> GetValue) => ()
    }
  }

  test("should plan an index scan in parallel runtime when property is accessed in ORDER BY") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Question", 1000)
      .addNodeIndex("Question", Seq("views"), existsSelectivity = 1, uniqueSelectivity = 0.1, withValues = true)
      .addNodeExistenceConstraint("Question", "views")
      .setExecutionModel(ExecutionModel.BatchedParallel(128, 1024))
      .build()

    val q =
      """
        |MATCH (q:Question)
        |WITH q ORDER BY q.views DESC
        |LIMIT 1000
        |RETURN q.name AS tag, count(*) AS count
        |ORDER BY count DESC
        |LIMIT 10
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .top(10, "count DESC")
      .aggregation(Seq("q.name AS tag"), Seq("count(*) AS count"))
      .top(1000, "`q.views` DESC")
      .projection("cacheN[q.views] AS `q.views`")
      .nodeIndexOperator("q:Question(views)", getValue = Map("views" -> GetValue), indexType = IndexType.RANGE)
      .build()
  }

  private def cachedNodePropertyProj(node: String, property: String) =
    v"$node.$property" -> cachedNodeProp(node, property)

  private def cachedNodePropertyProj(alias: String, node: String, property: String) =
    varFor(alias) -> cachedNodeProp(node, property)

  private def propertyProj(node: String, property: String) =
    v"$node.$property" -> prop(node, property)

}
