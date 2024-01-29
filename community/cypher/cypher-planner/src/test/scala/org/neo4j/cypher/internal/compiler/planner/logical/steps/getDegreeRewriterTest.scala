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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.CreateIrExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.getDegreeRewriterTest.relPattern
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.label_expressions.LabelExpression.disjoinRelTypesToLabelExpression
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

trait GetDegreeRewriterTestBase extends CypherFunSuite with AstConstructionTestSupport {

  def createIrExpressions: CreateIrExpressions =
    CreateIrExpressions(new AnonymousVariableNameGenerator(), new SemanticTable())

  protected def makeInputExpression(
    from: Option[String] = None,
    to: Option[String] = None,
    relationships: Seq[String] = Seq.empty,
    predicate: Option[Expression] = None,
    introducedVariables: Set[LogicalVariable] = Set.empty,
    scopeDependencies: Set[LogicalVariable] = Set.empty
  ): Expression

  protected def testNameExpr(pattern: String): String
}

trait GetDegreeRewriterExistsLikeTestBase extends GetDegreeRewriterTestBase {

  // All pattern elements have been named at this point, so the outer scope of PatternExpression is used to signal which expressions come from the outside.
  // In the test names, empty names denote anonymous notes, i.e. ones that do not come from the outside.

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} to GetDegree( (a)-[:FOO]->() ) > 0") {

    val incoming =
      createIrExpressions(makeInputExpression(
        from = Some("a"),
        relationships = Seq("FOO"),
        scopeDependencies = Set(v"a")
      ))
    val expected = HasDegreeGreaterThan(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(0))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} to GetDegree( (a)<-[:FOO]-() ) > 0") {
    val incoming =
      createIrExpressions(makeInputExpression(
        to = Some("a"),
        relationships = Seq("FOO"),
        scopeDependencies = Set(v"a")
      ))
    val expected = HasDegreeGreaterThan(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(0))(pos)
    getDegreeRewriter(incoming) should equal(expected)
  }

  test(
    s"Rewrite ${testNameExpr("(a)-[:FOO|:BAR]->()")} to HasDegreeGreaterThan( (a)-[:FOO]->(), 0) OR HasDegreeGreaterThan( (a)-[:BAR]->(), 0)"
  ) {
    val incoming = createIrExpressions(makeInputExpression(
      from = Some("a"),
      relationships = Seq("FOO", "BAR"),
      scopeDependencies = Set(v"a")
    ))
    val expected =
      ors(
        HasDegreeGreaterThan(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(0))(pos),
        HasDegreeGreaterThan(v"a", Some(RelTypeName("BAR")(pos)), OUTGOING, literalInt(0))(pos)
      )

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Does not rewrite ${testNameExpr("(a {prop: 5})-[:FOO]->()")} ") {
    val incoming = createIrExpressions(makeInputExpression(
      from = Some("a"),
      to = Some("b"),
      relationships = Seq("FOO"),
      predicate = Some(mapOf("prop" -> literalInt(5))),
      scopeDependencies = Set(v"a")
    ))

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test(s"Does not rewrite ${testNameExpr("(a)-[:FOO]->(b)")}") {
    val incoming =
      createIrExpressions(makeInputExpression(
        from = Some("a"),
        to = Some("b"),
        relationships = Seq("FOO"),
        introducedVariables = Set(v"p", v"a", v"r", v"b")
      ))

    getDegreeRewriter(incoming) should equal(incoming)
  }
}

class GetDegreeRewriterExistsExpressionTest extends GetDegreeRewriterExistsLikeTestBase {

  override def makeInputExpression(
    from: Option[String],
    to: Option[String],
    relationships: Seq[String],
    predicate: Option[Expression],
    introducedVariables: Set[LogicalVariable] = Set.empty,
    scopeDependencies: Set[LogicalVariable] = Set.empty
  ): Expression = {
    val maybeWhere: Option[Where] = predicate.map(p => Where(p)(p.position))
    simpleExistsExpression(
      patternForMatch(relPattern(from, to, relationships).element),
      maybeWhere = maybeWhere,
      introducedVariables = introducedVariables,
      scopeDependencies = scopeDependencies
    )
  }

  override protected def testNameExpr(pattern: String): String = s"EXISTS { $pattern }"

  test("does not rewrite EXIST with SKIP in RETURN") {
    val incoming = createIrExpressions(
      ExistsExpression(
        singleQuery(
          match_(nodePat(Some("a"))),
          return_(skip(2), v"a".as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("does not rewrite EXISTS with LIMIT in RETURN") {
    val incoming = createIrExpressions(
      ExistsExpression(
        singleQuery(
          match_(nodePat(Some("a"))),
          return_(limit(42), v"a".as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("does not rewrite EXISTS with aggregation in RETURN") {
    val a = nodePat(Some("a"))
    val b = nodePat(Some("b"))
    val `(a)-[r]-(b)` = RelationshipChain(a, relPat(Some("r")), b)(pos)
    val incoming = createIrExpressions(
      ExistsExpression(
        singleQuery(
          match_(`(a)-[r]-(b)`),
          return_(collect(v"a").as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("does rewrite EXISTS with grouped aggregation in RETURN") {
    val a = nodePat(Some("a"))
    val b = nodePat(Some("b"))
    val `(a)-[r]-(b)` = RelationshipChain(a, relPat(Some("r")), b)(pos)
    val incoming = createIrExpressions(
      ExistsExpression(
        singleQuery(
          match_(`(a)-[r]-(b)`),
          return_(v"a".as("a"), collect(literal(1)).as("c"))
        )
      )(pos, None, Some(Set(v"a", v"c")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("does not rewrite EXISTS with ORDER BY, SKIP and LIMIT in RETURN") {
    val incoming = createIrExpressions(
      ExistsExpression(
        singleQuery(
          match_(nodePat(Some("a"))),
          return_(orderBy(v"a".desc), skip(2), limit(42), v"a".as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }
}

class GetDegreeRewriterExistsFunctionTest extends GetDegreeRewriterExistsLikeTestBase {

  override def makeInputExpression(
    from: Option[String],
    to: Option[String],
    relationships: Seq[String],
    predicate: Option[Expression],
    introducedVariables: Set[LogicalVariable] = Set.empty,
    scopeDependencies: Set[LogicalVariable] = Set.empty
  ): Expression = {
    Exists(PatternExpression(
      pattern = relPattern(from, to, relationships, predicate)
    )(
      computedIntroducedVariables = Some(introducedVariables),
      computedScopeDependencies = Some(scopeDependencies)
    ))(pos)
  }

  override protected def testNameExpr(pattern: String): String = s"exists ( $pattern )"
}

object getDegreeRewriterTest extends AstConstructionTestSupport {

  def relPattern(
    from: Option[String] = None,
    to: Option[String] = None,
    relationships: Seq[String] = Seq.empty,
    fromPropertyPredicate: Option[Expression] = None
  ): RelationshipsPattern = {
    RelationshipsPattern(RelationshipChain(
      NodePattern(Some(from.map(varFor(_)).getOrElse(v"DEFAULT")), None, fromPropertyPredicate, None)(pos),
      RelationshipPattern(
        Some(v"r"),
        disjoinRelTypesToLabelExpression(relationships.map(r => RelTypeName(r)(pos))),
        None,
        None,
        None,
        SemanticDirection.OUTGOING
      )(pos),
      NodePattern(Some(to.map(varFor(_)).getOrElse(v"DEFAULT")), None, None, None)(pos)
    )(pos))(pos)
  }
}

class GetDegreeRewriterCountExpressionTest extends GetDegreeRewriterCountLikeTestBase {

  override def makeInputExpression(
    from: Option[String],
    to: Option[String],
    relationships: Seq[String],
    predicate: Option[Expression],
    introducedVariables: Set[LogicalVariable] = Set.empty,
    scopeDependencies: Set[LogicalVariable] = Set.empty
  ): Expression = {
    val maybeWhere: Option[Where] = predicate.map(p => Where(p)(p.position))
    simpleCountExpression(
      patternForMatch(relPattern(from, to, relationships).element),
      maybeWhere = maybeWhere,
      introducedVariables = introducedVariables,
      scopeDependencies = scopeDependencies
    )
  }

  override protected def testNameExpr(pattern: String): String = s"COUNT { $pattern }"

  test("does not rewrite COUNT with SKIP in RETURN") {
    val incoming = createIrExpressions(
      CountExpression(
        singleQuery(
          match_(nodePat(Some("a"))),
          return_(skip(2), v"a".as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("does not rewrite COUNT with LIMIT in RETURN") {
    val incoming = createIrExpressions(
      CountExpression(
        singleQuery(
          match_(nodePat(Some("a"))),
          return_(limit(42), v"a".as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("does not rewrite COUNT with ORDER BY, SKIP and LIMIT in RETURN") {
    val incoming = createIrExpressions(
      CountExpression(
        singleQuery(
          match_(nodePat(Some("a"))),
          return_(orderBy(v"a".desc), skip(2), limit(42), v"a".as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("does not rewrite COUNT with DISTINCT in RETURN") {
    val incoming = createIrExpressions(
      CountExpression(
        singleQuery(
          match_(nodePat(Some("a"))),
          returnDistinct(v"a".as("a"))
        )
      )(pos, None, Some(Set(v"a")))
    )

    getDegreeRewriter(incoming) should equal(incoming)
  }
}

trait GetDegreeRewriterCountLikeTestBase extends GetDegreeRewriterTestBase {

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} to GetDegree( (a)-[:FOO]->() )") {
    val incoming =
      createIrExpressions(makeInputExpression(
        from = Some("a"),
        relationships = Seq("FOO"),
        scopeDependencies = Set(v"a")
      ))
    val expected = GetDegree(v"a", Some(RelTypeName("FOO")(pos)), SemanticDirection.OUTGOING)(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} to GetDegree( (a)<-[:FOO]-() )") {
    val incoming =
      createIrExpressions(makeInputExpression(
        to = Some("a"),
        relationships = Seq("FOO"),
        scopeDependencies = Set(v"a")
      ))
    val expected = GetDegree(v"a", Some(RelTypeName("FOO")(pos)), SemanticDirection.INCOMING)(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO|:BAR]->()")} to GetDegree( (a)-[:FOO]->() ) + GetDegree( (a)-[:BAR]->() )") {
    val incoming =
      createIrExpressions(makeInputExpression(
        from = Some("a"),
        relationships = Seq("FOO", "BAR"),
        scopeDependencies = Set(v"a")
      ))
    val expected =
      add(
        GetDegree(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING)(pos),
        GetDegree(v"a", Some(RelTypeName("BAR")(pos)), OUTGOING)(pos)
      )

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Does not rewrite ${testNameExpr("[ (a)-[:FOO]->(b)")}") {
    val incoming =
      createIrExpressions(makeInputExpression(
        from = Some("a"),
        to = Some("b"),
        relationships = Seq("FOO"),
        scopeDependencies = Set(v"a", v"b")
      ))

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test(s"Does not rewrite ${testNameExpr("(a)-[:FOO]->() WHERE a.prop")}") {
    val incoming = createIrExpressions(Size(makeInputExpression(
      from = Some("a"),
      to = Some("b"),
      relationships = Seq("FOO"),
      predicate = Some(prop("a", "prop")),
      scopeDependencies = Set(v"a", v"b")
    ))(pos))

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} > 5 to HasDegreeGreaterThan( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeGreaterThan(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} > 5 to HasDegreeGreaterThan( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeGreaterThan(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 > ${testNameExpr("} (a)-[:FOO]->()")} to HasDegreeLessThan( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        literalInt(5),
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
      )(pos))
    val expected = HasDegreeLessThan(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 > ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeLessThan( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
      )(pos))
    val expected = HasDegreeLessThan(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} >= 5 to HasDegreeGreaterThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
      literalInt(5)
    )(pos))
    val expected = HasDegreeGreaterThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} >= 5 to HasDegreeGreaterThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
      literalInt(5)
    )(pos))
    val expected = HasDegreeGreaterThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 >= ${testNameExpr("(a)-[:FOO]->()")} to HasDegreeLessThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      literalInt(5),
      makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
    )(pos))
    val expected = HasDegreeLessThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 >= ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeLessThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      literalInt(5),
      makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
    )(pos))
    val expected = HasDegreeLessThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} = 5 to HasDegree( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
        literalInt(5)
      )(pos))
    val expected = HasDegree(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} = 5 to HasDegree( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
        literalInt(5)
      )(pos))
    val expected = HasDegree(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 = ${testNameExpr("(a)-[:FOO]->()")} to HasDegree( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        literalInt(5),
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
      )(pos))
    val expected = HasDegree(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 = ${testNameExpr("()-[:FOO]->(a)")} to HasDegree( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
      )(pos))
    val expected = HasDegree(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} <= 5 to HasDegreeLessThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(LessThanOrEqual(
      makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
      literalInt(5)
    )(pos))
    val expected = HasDegreeLessThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} <= 5 to HasDegreeLessThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(LessThanOrEqual(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeLessThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 <= ${testNameExpr("(a)-[:FOO]->()")} to HasDegreeGreaterThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(LessThanOrEqual(
      literalInt(5),
      makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
    )(pos))
    val expected = HasDegreeGreaterThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 <= ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeGreaterThanOrEqual( (a)<-[:FOO]-() , 0)") {
    val incoming =
      createIrExpressions(LessThanOrEqual(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
      )(pos))
    val expected = HasDegreeGreaterThanOrEqual(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} < 5 to HasDegreeLessThan( (a)-[:FOO]->() , 0)") {
    val incoming =
      createIrExpressions(LessThan(
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
        literalInt(5)
      )(
        pos
      ))
    val expected = HasDegreeLessThan(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} < 5 to HasDegreeLessThan( (a)<-[:FOO]-() , 0)") {
    val incoming =
      createIrExpressions(LessThan(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeLessThan(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 < ${testNameExpr("(a)-[:FOO]->()")} to HasDegreeGreaterThan( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(LessThan(
        literalInt(5),
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
      )(
        pos
      ))
    val expected = HasDegreeGreaterThan(v"a", Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 < ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeGreaterThan( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(LessThan(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"), scopeDependencies = Set(v"a"))
      )(pos))
    val expected = HasDegreeGreaterThan(v"a", Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }
}
