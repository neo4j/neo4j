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
package org.neo4j.cypher.internal.frontend.scoping

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.scoping.AggregatingItem
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingItem
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionItem
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier.AggregationInNonAggregatingProjection
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier.AggregationReferencingDeclared
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier.Ambiguous
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier.AmbiguousAndComplex
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier.Complex
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier.NonProblematic
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Unit tests for [[SubclauseExpressionClassifier.classifyImplicit]] — Table A
 * (implicit grouping, no GROUP BY). The classifier takes pre-computed refs and
 * projection metadata, so these tests construct the inputs directly rather than
 * building full scope trees.
 */
class SubclauseExpressionClassifierTest extends CypherFunSuite with AstConstructionTestSupport {

  private def classify(
    expression: Expression,
    isProjectionAggregating: Boolean = true,
    hasUnrecognizedAggregate: Option[Boolean] = None,
    aggArgs: Set[LogicalVariable] = Set.empty,
    outer: Set[LogicalVariable] = Set.empty,
    shadowing: Set[LogicalVariable] = Set.empty,
    introduced: Set[LogicalVariable] = Set.empty,
    groupingKeyAliases: Set[LogicalVariable] = Set.empty,
    item: Option[ProjectionItem] = None
  ): SubclauseExpressionClassifier.Classification =
    SubclauseExpressionClassifier.classifyImplicit(
      SubclauseExpressionClassifier.ClassifierInputs(
        expression = expression,
        item = item,
        hasGroupBy = false,
        isProjectionAggregating = isProjectionAggregating,
        hasUnrecognizedAggregate = hasUnrecognizedAggregate.getOrElse(expression.containsAggregate),
        aggArgs = aggArgs,
        outer = outer,
        shadowing = shadowing,
        introduced = introduced,
        groupingKeyAliases = groupingKeyAliases
      )
    )

  test("literal → NonProblematic") {
    classify(literalInt(1)) shouldBe a[NonProblematic]
  }

  test("parameter → NonProblematic") {
    classify(parameter("p", org.neo4j.cypher.internal.util.symbols.CTAny, None)) shouldBe a[NonProblematic]
  }

  test("plain variable → NonProblematic") {
    classify(varFor("x")) shouldBe a[NonProblematic]
  }

  test("matchingItem + outer ambiguous → Ambiguous") {
    val a = varFor("a")
    val expr = add(a.copyId, count(a.copyId)) // a + COUNT(a) — matches projection item
    val item = NonAggregatingItem(expr, Some(varFor("c")))
    val result = classify(
      expression = expr,
      aggArgs = Set(a), // also inside the aggregate
      outer = Set(a),
      shadowing = Set(a),
      item = Some(item)
    )
    result shouldBe an[Ambiguous]
  }

  test("aggregate in non-aggregating projection → AggregationInNonAggregatingProjection") {
    val expr = sum(varFor("x"))
    classify(
      expression = expr,
      isProjectionAggregating = false,
      aggArgs = Set.empty
    ) shouldBe an[AggregationInNonAggregatingProjection]
  }

  test("aggregate arg references ambiguous (alias shadowing incoming) → AggregationReferencingDeclared") {
    val a = varFor("a")
    val expr = sum(a.copyId)
    val result = classify(
      expression = expr,
      aggArgs = Set(a),
      outer = Set(a),
      shadowing = Set(a)
    )
    result shouldBe an[AggregationReferencingDeclared]
  }

  test("aggregate args reference multiple ambiguous aliases → AggregationReferencingDeclared lists all") {
    // RETURN 1 AS a, 10 AS b, count(a) AS cnt ORDER BY sum(a) + count(b)
    // both `a` and `b` shadow incoming variables and sit in aggregate arguments, so both are
    // reported (42I79 renders the list ANDED).
    val a = varFor("a")
    val b = varFor("b")
    val expr = add(sum(a.copyId), count(b.copyId)) // sum(a) + count(b)
    val result = classify(
      expression = expr,
      aggArgs = Set(a, b),
      outer = Set(a, b),
      shadowing = Set(a, b)
    )
    result shouldBe an[AggregationReferencingDeclared]
    result.asInstanceOf[AggregationReferencingDeclared].problematicReferences shouldBe Set(a, b)
  }

  test("aggregate arg references same-clause-only alias → AggregationReferencingDeclared") {
    val y = varFor("y")
    val expr = sum(y.copyId)
    val result = classify(
      expression = expr,
      aggArgs = Set(y),
      outer = Set(y),
      introduced = Set(y)
    )
    result shouldBe an[AggregationReferencingDeclared]
  }

  test("aggregate outer (non-arg) references ambiguous, agg args clean → AggregationReferencingDeclared (strict)") {
    // Updated for CIP-248: a fresh aggregating subclause expression whose outer
    // reference is ambiguous is an error in Cypher 25 (42I79). The D1 deprecation
    // path is reserved for sort = projection item exactly.
    val a = varFor("a")
    val b = varFor("b")
    val expr = add(a.copyId, sum(b.copyId))
    val result = classify(
      expression = expr,
      aggArgs = Set(b), // only b inside agg
      outer = Set(a, b),
      shadowing = Set(a) // only outer ref a is ambiguous
    )
    result shouldBe an[AggregationReferencingDeclared]
  }

  test("fresh aggregating sort with outer ambiguous (no matching item) → AggregationReferencingDeclared") {
    // RETURN 1 AS a, ..., count(a) AS cnt ORDER BY sum(c) + a
    // sort `sum(c) + a` doesn't equal any projection item exactly; outer `a` is
    // shadowed; per CIP-248 Rule 3 strictness in Cypher 25: 42I79 error.
    val a = varFor("a")
    val c = varFor("c")
    val expr = add(sum(c.copyId), a.copyId)
    val result = classify(
      expression = expr,
      aggArgs = Set(c),
      outer = Set(c, a),
      shadowing = Set(a),
      introduced = Set(a),
      item = None
    )
    result shouldBe an[AggregationReferencingDeclared]
  }

  test("fresh aggregating sort with outer same-clause-only (no matching item) → AggregationReferencingDeclared") {
    // RETURN 1 AS y, ..., count(a) AS cnt ORDER BY sum(c) + y
    // outer `y` is a new alias (not shadowing); strict 42I79 error.
    val y = varFor("y")
    val c = varFor("c")
    val expr = add(sum(c.copyId), y.copyId)
    val result = classify(
      expression = expr,
      aggArgs = Set(c),
      outer = Set(c, y),
      introduced = Set(y),
      item = None
    )
    result shouldBe an[AggregationReferencingDeclared]
  }

  test("aggregate with no problematic refs → NonProblematic") {
    val v = varFor("a")
    val expr = sum(v.copyId)
    classify(
      expression = expr,
      aggArgs = Set(v),
      outer = Set(v)
    ) shouldBe a[NonProblematic]
  }

  test("non-aggregating subclause expression matches projection item exactly → NonProblematic") {
    val v = varFor("a")
    val expr = prop(v.copyId, "p") // a.p
    val item = NonAggregatingItem(expr, Some(varFor("b")))
    classify(
      expression = expr,
      item = Some(item)
    ) shouldBe a[NonProblematic]
  }

  test("complex matching projection item as sub-expression, no ambiguity → Complex") {
    val v = varFor("a")
    val itemExpr = add(prop(v.copyId, "p"), literalInt(0)) // a.p + 0
    val item = NonAggregatingItem(itemExpr, Some(varFor("b")))
    // sort/where strictly contains the complex item — itemExpr appears as sub-expression
    val expr = add(itemExpr, literalInt(1))
    classify(
      expression = expr,
      item = Some(item)
    ) shouldBe a[Complex]
  }

  test("complex matching projection item as sub-expression + outer ambiguous → AmbiguousAndComplex") {
    val v = varFor("a")
    val itemExpr = add(prop(v.copyId, "p"), literalInt(0))
    val item = NonAggregatingItem(itemExpr, Some(varFor("b")))
    val expr = add(itemExpr, literalInt(1))
    val result = classify(
      expression = expr,
      outer = Set(v),
      shadowing = Set(varFor("a")), // alias `a` shadows incoming `a`
      item = Some(item)
    )
    result shouldBe an[AmbiguousAndComplex]
  }

  // ---------------------------------------------------------------------------
  // Table B — explicit GROUP BY (strict)
  // ---------------------------------------------------------------------------

  private def classifyGroupBy(
    expression: Expression,
    hasUnrecognizedAggregate: Option[Boolean] = None,
    aggArgs: Set[LogicalVariable] = Set.empty,
    outer: Set[LogicalVariable] = Set.empty,
    shadowing: Set[LogicalVariable] = Set.empty,
    introduced: Set[LogicalVariable] = Set.empty,
    groupingKeyAliases: Set[LogicalVariable] = Set.empty
  ): SubclauseExpressionClassifier.Classification =
    SubclauseExpressionClassifier.classifyWithGroupBy(
      SubclauseExpressionClassifier.ClassifierInputs(
        expression = expression,
        item = None,
        hasGroupBy = true,
        isProjectionAggregating = true,
        hasUnrecognizedAggregate = hasUnrecognizedAggregate.getOrElse(expression.containsAggregate),
        aggArgs = aggArgs,
        outer = outer,
        shadowing = shadowing,
        introduced = introduced,
        groupingKeyAliases = groupingKeyAliases
      )
    )

  test("[GB] non-aggregating expression → NonProblematic") {
    val v = varFor("a")
    classifyGroupBy(prop(v, "p")) shouldBe a[NonProblematic]
  }

  test("[GB] aggregate arg references ambiguous (shadowing) alias → AggregationReferencingDeclared") {
    val v = varFor("a")
    val expr = sum(v.copyId)
    classifyGroupBy(
      expression = expr,
      aggArgs = Set(v),
      shadowing = Set(v)
    ) shouldBe an[AggregationReferencingDeclared]
  }

  test("[GB] aggregate arg references same-clause-only alias → AggregationReferencingDeclared") {
    val y = varFor("y")
    val expr = sum(y.copyId)
    classifyGroupBy(
      expression = expr,
      aggArgs = Set(y),
      introduced = Set(y)
    ) shouldBe an[AggregationReferencingDeclared]
  }

  test("[GB] aggregate with clean args → NonProblematic") {
    val v = varFor("a")
    val expr = sum(v.copyId)
    classifyGroupBy(
      expression = expr,
      aggArgs = Set(v)
    ) shouldBe a[NonProblematic]
  }

  test("[GB] aggregate with outer shadowing → AggregationReferencingDeclared") {
    val a = varFor("a")
    val c = varFor("c")
    val expr = add(sum(c.copyId), a.copyId)
    classifyGroupBy(
      expression = expr,
      aggArgs = Set(c),
      outer = Set(c, a),
      shadowing = Set(a)
    ) shouldBe an[AggregationReferencingDeclared]
  }

  test("[GB] aggregate with outer same-clause-only → AggregationReferencingDeclared") {
    val y = varFor("y")
    val c = varFor("c")
    val expr = add(sum(c.copyId), y.copyId)
    classifyGroupBy(
      expression = expr,
      aggArgs = Set(c),
      outer = Set(c, y),
      introduced = Set(y)
    ) shouldBe an[AggregationReferencingDeclared]
  }

  test("[GB] outer ref to grouping-key alias inside aggregating expression → NonProblematic") {
    // GROUP BY age: `age + sum(x)` is a standard mixed sort key. The grouping-key alias `age`
    // is stable per group and must not be flagged by the CIP-248, Rule-3 strictness.
    val age = varFor("age")
    val x = varFor("x")
    val expr = add(age.copyId, sum(x.copyId))
    classifyGroupBy(
      expression = expr,
      aggArgs = Set(x),
      outer = Set(age, x),
      introduced = Set(age),
      groupingKeyAliases = Set(age)
    ) shouldBe a[NonProblematic]
  }

  test(
    "[GB] outer ref to grouping-key alias still flagged when alias shadows incoming → AggregationReferencingDeclared"
  ) {
    // GROUP BY n: `RETURN n.x AS n GROUP BY n ORDER BY n + sum(x)` — `n` shadows incoming `n`.
    // Shadowing is detected via `shadowing`, independent of `groupingKeyAliases` filtering.
    val n = varFor("n")
    val x = varFor("x")
    val expr = add(n.copyId, sum(x.copyId))
    classifyGroupBy(
      expression = expr,
      aggArgs = Set(x),
      outer = Set(n, x),
      shadowing = Set(n),
      introduced = Set(n),
      groupingKeyAliases = Set(n)
    ) shouldBe an[AggregationReferencingDeclared]
  }

  // ---------------------------------------------------------------------------
  // Grouping-key alias references in implicit-grouping aggregating expressions
  // ---------------------------------------------------------------------------

  test("outer ref to implicit grouping-key alias inside aggregating expression → NonProblematic") {
    // RETURN me.age AS age, count(you.age) AS cnt
    //  ORDER BY age + count(you.age)
    // `age` is an implicit grouping-key alias; referencing it alongside the aggregate is
    // valid grouping semantics and must not trigger CIP-248, Rule-3 strictness.
    val age = varFor("age")
    val you = varFor("you")
    val expr = add(age.copyId, count(prop(you.copyId, "age")))
    val result = classify(
      expression = expr,
      aggArgs = Set(you),
      outer = Set(age, you),
      introduced = Set(age, varFor("cnt")),
      groupingKeyAliases = Set(age),
      item = None
    )
    result shouldBe a[NonProblematic]
  }

  test("outer ref to unrecognised grouping-key alias (literal alias) → AggregationReferencingDeclared") {
    // Incoming: y, c
    // RETURN 1 AS y, count(a) AS cnt ORDER BY sum(c) + y
    // `1 AS y` is lifted into groupingKeys by implicitKeys, but `1` is not a recognised
    // grouping-key shape (Variable / Property-on-Variable). The producer must not pass
    // `y` as a grouping-key alias here, and the classifier must still flag 42I79.
    val y = varFor("y")
    val c = varFor("c")
    val expr = add(sum(c.copyId), y.copyId)
    val result = classify(
      expression = expr,
      aggArgs = Set(c),
      outer = Set(c, y),
      introduced = Set(y),
      groupingKeyAliases = Set.empty,
      item = None
    )
    result shouldBe an[AggregationReferencingDeclared]
  }

  test("recognized aggregate referenced via own alias → NonProblematic (CIP-248 Rule 3)") {
    // RETURN SUM(x) AS s ORDER BY s + SUM(x)
    // The sort's `SUM(x)` matches the projection item `SUM(x) AS s`, so it is substituted to `s`
    // (the sort becomes `s + s`): no fresh aggregate, hasUnrecognizedAggregate = false. A recognized
    // aggregate is not a complex grouping-key re-statement, so it does NOT raise the Complex
    // deprecation; with no shadowing reference it is simply permitted.
    val s = varFor("s")
    val x = varFor("x")
    val item = AggregatingItem(sum(x.copyId), Some(s)) // SUM(x) AS s
    val expr = add(s.copyId, sum(x.copyId)) // s + SUM(x)
    val result = classify(
      expression = expr,
      hasUnrecognizedAggregate = Some(false),
      aggArgs = Set(x),
      outer = Set(s, x),
      introduced = Set(s),
      item = Some(item)
    )
    result shouldBe a[NonProblematic]
  }

  test("recognized aggregate, sort equals the aggregate item exactly → NonProblematic") {
    // RETURN SUM(x) AS s ORDER BY SUM(x)
    // The sort `SUM(x)` matches the projection item exactly and substitutes to `s` — no fresh
    // aggregate. An exact item match short-circuits to NonProblematic before the complex path.
    val s = varFor("s")
    val x = varFor("x")
    val sumX = sum(x.copyId)
    val item = AggregatingItem(sumX, Some(s)) // SUM(x) AS s
    val result = classify(
      expression = sumX, // reuse instance so item.expression == expression
      hasUnrecognizedAggregate = Some(false),
      aggArgs = Set(x),
      outer = Set(x),
      introduced = Set(s),
      item = Some(item)
    )
    result shouldBe a[NonProblematic]
  }

  test(
    "fresh aggregate alongside same-clause alias → AggregationReferencingDeclared (CIP-248 Rule 3, GQL_42I79 negative)"
  ) {
    // RETURN SUM(x) AS s ORDER BY s + COUNT(y)
    // COUNT(y) matches no projection item, so it cannot be substituted: a fresh aggregate
    // (hasUnrecognizedAggregate = true). A fresh aggregate may not reference the same-clause alias
    // `s` → 42I79.
    val s = varFor("s")
    val x = varFor("x")
    val y = varFor("y")
    val item = AggregatingItem(sum(x.copyId), Some(s)) // SUM(x) AS s
    val expr = add(s.copyId, count(y.copyId)) // s + COUNT(y)
    val result = classify(
      expression = expr,
      hasUnrecognizedAggregate = Some(true),
      aggArgs = Set(y),
      outer = Set(s, y),
      introduced = Set(s),
      item = Some(item)
    )
    result shouldBe an[AggregationReferencingDeclared]
  }

  test("fresh aggregate without an introduced-name reference → NonProblematic (CIP-248 Rule 3)") {
    // RETURN SUM(x) AS s ORDER BY SUM(x) + COUNT(y)
    // SUM(x) is recognized (substituted to `s`) and COUNT(y) is a fresh aggregate
    // (hasUnrecognizedAggregate = true). Unlike `s + COUNT(y)`, the user wrote no projection-
    // introduced name: the use-sites are the incoming `x`/`y`, so nothing in `introduced` is
    // referenced and the fresh aggregate is allowed.
    val s = varFor("s")
    val x = varFor("x")
    val y = varFor("y")
    val item = AggregatingItem(sum(x.copyId), Some(s)) // SUM(x) AS s
    val expr = add(sum(x.copyId), count(y.copyId)) // SUM(x) + COUNT(y)
    val result = classify(
      expression = expr,
      hasUnrecognizedAggregate = Some(true),
      aggArgs = Set(x, y),
      outer = Set(x, y),
      introduced = Set(s),
      item = Some(item)
    )
    result shouldBe a[NonProblematic]
  }

  test("recognized aggregate with outer shadowing alias → Ambiguous") {
    // RETURN n.p AS n, SUM(x) AS s ORDER BY n + SUM(x)   (alias `n` shadows incoming `n`)
    // SUM(x) is recognized (no fresh aggregate), so it does NOT raise the Complex deprecation;
    // but the outer reference `n` shadows the incoming `n`, so the ambiguous deprecation still
    // applies.
    val n = varFor("n")
    val x = varFor("x")
    val s = varFor("s")
    val sumX = sum(x.copyId)
    val item = AggregatingItem(sumX, Some(s)) // SUM(x) AS s
    val expr = add(n.copyId, sumX) // n + SUM(x)
    val result = classify(
      expression = expr,
      hasUnrecognizedAggregate = Some(false),
      outer = Set(n, x),
      shadowing = Set(n),
      introduced = Set(n, s),
      item = Some(item)
    )
    result shouldBe an[Ambiguous]
  }
}
