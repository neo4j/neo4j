package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.RegularPlannerQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.AscIndexOrder
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class IndexWithProvidedOrderPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  test("Order by index backed property should plan with provided order") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop"

    plan._2 should equal(
      Projection(
        Projection(
          NodeIndexSeek(
            "n",
            LabelToken("Awesome", LabelId(0)),
            Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
            RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(StringLiteral("foo")(pos)))))(pos)),
            Set.empty,
            ProvidedOrder(Seq(Ascending("n.prop")))),
          Map("  FRESHID48" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))),
        Map("n.prop" -> Variable("  FRESHID48")(pos)))
    )
  }

  test("Order by index backed property DESCENDING (unsupported) should plan sort") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop DESC"

    plan._2 should equal(
      Projection(
        Sort(
          Projection(
            NodeIndexSeek(
              "n",
              LabelToken("Awesome", LabelId(0)),
              Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
              RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(StringLiteral("foo")(pos)))))(pos)),
              Set.empty,
              ProvidedOrder(Seq(Ascending("n.prop")))),
            Map("  FRESHID48" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))),
          Seq(Descending("  FRESHID48"))),
        Map("n.prop" -> Variable("  FRESHID48")(pos)))
    )
  }

  test("Order by index backed property renamed in an earlier WITH") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor
      """MATCH (n:Awesome) WHERE n.prop > 'foo'
        |WITH n AS nnn
        |MATCH (m)-[r]->(nnn)
        |RETURN nnn.prop ORDER BY nnn.prop""".stripMargin

    plan._2 should equal(
      Projection(
        Projection(
          Expand(
            Projection(
              NodeIndexSeek(
                "n",
                LabelToken("Awesome", LabelId(0)),
                Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
                RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(StringLiteral("foo")(pos)))))(pos)),
                Set.empty,
                ProvidedOrder(Seq(Ascending("n.prop")))),
              Map("nnn" -> Variable("n")(pos))),
            "nnn", SemanticDirection.INCOMING, Seq.empty, "m", "r"),
          Map("  FRESHID85" -> Property(Variable("nnn")(pos), PropertyKeyName("prop")(pos))(pos))),
        Map("nnn.prop" -> Variable("  FRESHID85")(pos)))
    )
  }

  test("Order by index backed property renamed in same return") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor
      """MATCH (n:Awesome) WHERE n.prop > 'foo'
        |RETURN n AS m ORDER BY m.prop""".stripMargin

    plan._2 should equal(
      Projection(
        Projection(
          NodeIndexSeek(
            "n",
            LabelToken("Awesome", LabelId(0)),
            Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
            RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(StringLiteral("foo")(pos)))))(pos)),
            Set.empty,
            ProvidedOrder(Seq(Ascending("n.prop")))),
          Map("  FRESHID46" -> Variable("n")(pos))),
        Map("m" -> Variable("  FRESHID46")(pos)))
    )
  }

  test("Cannot order by index when ordering is on same property name, but different node") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor "MATCH (m:Awesome), (n:Awesome) WHERE n.prop > 'foo' RETURN m.prop ORDER BY m.prop"

    plan._2 should equal(
      Projection(
        Sort(
          Projection(
            CartesianProduct(
              NodeIndexSeek(
                "n",
                LabelToken("Awesome", LabelId(0)),
                Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
                RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(StringLiteral("foo")(pos)))))(pos)),
                Set.empty,
                ProvidedOrder(Seq(Ascending("n.prop")))),
              NodeByLabelScan("m", LabelName("Awesome")(pos), Set.empty)),
            Map("  FRESHID61" -> Property(Variable("m")(pos), PropertyKeyName("prop")(pos))(pos))),
          Seq(Ascending("  FRESHID61"))),
        Map("m.prop" -> Variable("  FRESHID61")(pos)))
    )
  }

  test("Order by index backed property should plan with provided order (starts with scan)") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop ORDER BY n.prop"

    plan._2 should equal(

      Projection(
        Projection(
          NodeIndexSeek(
            "n",
            LabelToken("Awesome", LabelId(0)),
            Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
            RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(StringLiteral("foo")(pos)))(pos)),
            Set.empty,
            ProvidedOrder(Seq(Ascending("n.prop")))),
          Map("  FRESHID58" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))),
        Map("n.prop" -> Variable("  FRESHID58")(pos)))
    )
  }

  test("Order by index backed property should plan with provided order (contains scan)") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.prop ORDER BY n.prop"

    plan._2 should equal(

      Projection(
        Projection(
          NodeIndexContainsScan(
            "n",
            LabelToken("Awesome", LabelId(0)),
            IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue),
            StringLiteral("foo")(pos),
            Set.empty,
            ProvidedOrder(Seq(Ascending("n.prop")))),
          Map("  FRESHID55" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))),
        Map("n.prop" -> Variable("  FRESHID55")(pos)))
    )
  }

  test("Order by index backed property should plan with provided order (ends with scan)") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.prop ORDER BY n.prop"

    plan._2 should equal(

      Projection(
        Projection(
          NodeIndexEndsWithScan(
            "n",
            LabelToken("Awesome", LabelId(0)),
            IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue),
            StringLiteral("foo")(pos),
            Set.empty,
            ProvidedOrder(Seq(Ascending("n.prop")))),
          Map("  FRESHID56" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))),
        Map("n.prop" -> Variable("  FRESHID56")(pos)))
    )
  }

  test("Order by index backed property should plan with provided order (scan)") {
    val plan = new given {
      indexOn("Awesome", "prop").providesOrder(AscIndexOrder)
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE EXISTS(n.prop) RETURN n.prop ORDER BY n.prop"

    plan._2 should equal(

      Projection(
        Projection(
          NodeIndexScan(
            "n",
            LabelToken("Awesome", LabelId(0)),
            IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue),
            Set.empty,
            ProvidedOrder(Seq(Ascending("n.prop")))),
          Map("  FRESHID48" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))),
        Map("n.prop" -> Variable("  FRESHID48")(pos)))
    )
  }

  test("Planner works without sorted index") {
    val plan = new given {
      cardinality = mapCardinality {
        // expand - expensive
        case RegularPlannerQuery(queryGraph, _, _, _) => Math.pow(100, queryGraph.patternRelationships.size)
        // everything else - cheap
        case _ => 1.0
      }
    } getLogicalPlanFor "MATCH (a:Awesome)-[x]->(b)-[y]->(c) WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop"

    // Without having interesting/required order in IDP, this will always plan the sort after the expands
    val aProp = Property(Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    plan._2 should equal(
      Projection(
        Sort(
          Projection(
            Selection(
              Seq(Not(Equals(Variable("x")(pos), Variable("y")(pos))(pos))(pos)),
              Expand(
                Expand(
                  Selection(
                    Seq(expressions.AndedPropertyInequalities(Variable("a")(pos), aProp, NonEmptyList(GreaterThan(
                      aProp, StringLiteral("foo")(pos))(pos)))),
                    NodeByLabelScan("a", LabelName("Awesome")(pos), Set.empty)),
                  "a", SemanticDirection.OUTGOING, Seq.empty, "b", "x"),
                "b", SemanticDirection.OUTGOING, Seq.empty, "c", "y")),
            Map("  FRESHID66" -> Property(Variable("a")(pos), PropertyKeyName("prop")(pos))(pos))),
          Seq(Ascending("  FRESHID66"))),
        Map("a.prop" -> Variable("  FRESHID66")(pos)))
    )
  }

  // TODO MATCH (a:A), (b:B) WHERE a.prop > 'foo' AND a.prop = b.prop ORDER BY a.prop

  // TODO WITH 2 as x MATCH (m:Awesome) WHERE m.prop > 'foo' RETURN m.prop ORDER BY m.prop
  // does not work because NIS is planned as RHS of apply. We could recognize that LHS has only 1 row.
}
