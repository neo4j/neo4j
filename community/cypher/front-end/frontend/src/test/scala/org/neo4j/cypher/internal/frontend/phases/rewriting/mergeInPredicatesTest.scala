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
package org.neo4j.cypher.internal.frontend.phases.rewriting

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.rewriting.rewriters.mergeInPredicates
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class mergeInPredicatesTest extends CypherFunSuite with AstRewritingTestSupport {

  /*
   * The grids below check every way to merge two comparisons of the same predicand v (see `mergeCases`) in the two
   * situations that decide whether a merge is correct under three-valued logic: a list containing null, and an empty
   * merged list. The scaladoc of MergeInPredicates explains why. Each cell states one of:
   *   - Merges(to)    the comparisons are rewritten to `to`
   *   - MustNotMerge  the comparisons are left as they are, and must be: merging would change the result
   *   - MayMerge(to)  the comparisons are left as they are, although `to` would be equivalent. The rewriter never
   *                   merges lists containing null, to keep it simple. A finer guard may turn these into Merges(to).
   */

  sealed private trait Expected {

    def describe: String = this match {
      case Merges(to)   => s"merges to `$to`"
      case MustNotMerge => "must not merge"
      case MayMerge(to) => s"does not merge, although `$to` would be equivalent"
    }

    def mapMerged(f: String => String): Expected = this match {
      case Merges(to)   => Merges(f(to))
      case MustNotMerge => MustNotMerge
      case MayMerge(to) => MayMerge(f(to))
    }
  }
  private case class Merges(to: String) extends Expected
  private case object MustNotMerge extends Expected
  private case class MayMerge(to: String) extends Expected

  private case class MergeCase(original: (String, String) => String, merged: String => String) {
    def template: String = original("A", "B")
  }

  // All ways to merge two comparisons of the same predicand, with the merged comparison
  private val mergeCases: Map[Int, MergeCase] = Map(
    // AND
    1 -> MergeCase((a, b) => s"v IN $a AND v IN $b", l => s"v IN $l"), // v IN A∩B
    2 -> MergeCase((a, b) => s"v IN $a AND NOT v IN $b", l => s"v IN $l"), // v IN A∖B
    3 -> MergeCase((a, b) => s"NOT v IN $a AND v IN $b", l => s"v IN $l"), // v IN B∖A
    4 -> MergeCase((a, b) => s"NOT v IN $a AND NOT v IN $b", l => s"NOT v IN $l"), // NOT v IN A∪B
    // OR
    5 -> MergeCase((a, b) => s"v IN $a OR v IN $b", l => s"v IN $l"), // v IN A∪B
    6 -> MergeCase((a, b) => s"v IN $a OR NOT v IN $b", l => s"NOT v IN $l"), // NOT v IN B∖A
    7 -> MergeCase((a, b) => s"NOT v IN $a OR v IN $b", l => s"NOT v IN $l"), // NOT v IN A∖B
    8 -> MergeCase((a, b) => s"NOT v IN $a OR NOT v IN $b", l => s"NOT v IN $l") // NOT v IN A∩B
  )

  /*
   * Null in a list. Every column uses A = [1, 2] and B = [2, 3], with null appended as the header says, so that
   * A∩B, A∖B and B∖A are all non-empty. A cell gives the merged list; the case turns it into the merged comparison.
   */
  private val nullInListColumns = Seq(
    ("no null", "[1, 2]", "[2, 3]"),
    ("null in A", "[1, 2, null]", "[2, 3]"),
    ("null in B", "[1, 2]", "[2, 3, null]"),
    ("null in both", "[1, 2, null]", "[2, 3, null]")
  )

  private val nullInList: Seq[(Int, Expected, Expected, Expected, Expected)] = Seq(
    // format: off
    //  no null              null in A                    null in B                    null in both
    (1, Merges("[2]"),       MustNotMerge,                MustNotMerge,                MayMerge("[2, null]")),
    (2, Merges("[1]"),       MustNotMerge,                MustNotMerge,                MustNotMerge),
    (3, Merges("[3]"),       MustNotMerge,                MustNotMerge,                MustNotMerge),
    (4, Merges("[1, 2, 3]"), MayMerge("[1, 2, null, 3]"), MayMerge("[1, 2, 3, null]"), MayMerge("[1, 2, null, 3]")),
    (5, Merges("[1, 2, 3]"), MayMerge("[1, 2, null, 3]"), MayMerge("[1, 2, 3, null]"), MayMerge("[1, 2, null, 3]")),
    (6, Merges("[3]"),       MustNotMerge,                MustNotMerge,                MustNotMerge),
    (7, Merges("[1]"),       MustNotMerge,                MustNotMerge,                MustNotMerge),
    (8, Merges("[2]"),       MustNotMerge,                MustNotMerge,                MayMerge("[2, null]"))
    // format: on
  )

  for {
    (caseNumber, noNull, nullInA, nullInB, nullInBoth) <- nullInList
    ((column, a, b), expected) <- nullInListColumns.zip(Seq(noNull, nullInA, nullInB, nullInBoth))
  } {
    val mergeCase = mergeCases(caseNumber)
    checkCell(caseNumber, column, mergeCase.original(a, b), expected.mapMerged(mergeCase.merged))
  }

  /**
   * Cases that would result in a comparison to an empty list.
   * Cases 4 and 5 result in unions, which can only be empty if both lists are empty.
   */
  private val emptyMergedList: Seq[(Int, Option[(String, String, Expected)], (String, String, Expected))] = Seq(
    // format: off
    //  A and B non-empty                      A or B empty
    (1, Some(("[1]", "[2]", MustNotMerge)),    ("[]", "[1]", Merges("false"))),
    (2, Some(("[1]", "[1]", MustNotMerge)),    ("[]", "[1]", Merges("false"))),
    (3, Some(("[1, 2]", "[2]", MustNotMerge)), ("[1]", "[]", Merges("false"))),
    (4, None,                                  ("[]", "[]", Merges("true"))),
    (5, None,                                  ("[]", "[]", Merges("false"))),
    (6, Some(("[1]", "[1]", MustNotMerge)),    ("[1]", "[]", Merges("true"))),
    (7, Some(("[1]", "[1, 2]", MustNotMerge)), ("[]", "[1]", Merges("true"))),
    (8, Some(("[1]", "[2]", MustNotMerge)),    ("[]", "[1]", Merges("true")))
    // format: on
  )

  for ((caseNumber, absent, present) <- emptyMergedList) {
    val mergeCase = mergeCases(caseNumber)
    for ((a, b, expected) <- absent) {
      checkCell(caseNumber, "empty merged list, constant absent", mergeCase.original(a, b), expected)
    }
    val (a, b, expected) = present
    checkCell(caseNumber, "empty merged list, constant present", mergeCase.original(a, b), expected)
  }

  /*
   * Equal values spelled differently. Every column uses A = [1, x] and B = [y, 3], where x and y are different
   * literals of the same value, so they must count as the same element. The merged list keeps the spelling of A.
   */
  private val equalValueColumns = Seq(
    ("integer and float", "2", "2.0"),
    ("float and integer", "2.0", "2"),
    ("decimal and hexadecimal integer", "2", "0x2"),
    ("decimal and octal integer", "2", "0o2"),
    ("zero and negative integer zero", "0", "-0"),
    ("float zero and negative float zero", "0.0", "-0.0"),
    ("float in decimal and exponent notation", "200.0", "2e2"),
    ("float with and without trailing zero", "2.0", "2.00"),
    ("integer and float beyond 2^53", "9007199254740992", "9007199254740992.0")
  )

  private val equalValue: Seq[(Int, String => String)] = Seq(
    1 -> (x => s"[$x]"),
    2 -> (_ => "[1]"),
    3 -> (_ => "[3]"),
    4 -> (x => s"[1, $x, 3]"),
    5 -> (x => s"[1, $x, 3]"),
    6 -> (_ => "[3]"),
    7 -> (_ => "[1]"),
    8 -> (x => s"[$x]")
  )

  for {
    (column, x, y) <- equalValueColumns
    (caseNumber, merged) <- equalValue
  } {
    val mergeCase = mergeCases(caseNumber)
    checkCell(
      caseNumber,
      s"equal values: $column",
      mergeCase.original(s"[1, $x]", s"[$y, 3]"),
      Merges(mergeCase.merged(merged(x)))
    )
  }

  /*
   * Different values that are close: 2^53 + 1 is not a double, so it is not equal to the double 2^53, although
   * converting it to a double gives 2^53. Uses A = [1, x] and B = [y, 3] like the columns above.
   */
  private val closeValues: Seq[(Int, Expected)] = Seq(
    1 -> MustNotMerge,
    2 -> Merges("[1, 9007199254740993]"),
    3 -> Merges("[9007199254740992.0, 3]"),
    4 -> Merges("[1, 9007199254740993, 9007199254740992.0, 3]"),
    5 -> Merges("[1, 9007199254740993, 9007199254740992.0, 3]"),
    6 -> Merges("[9007199254740992.0, 3]"),
    7 -> Merges("[1, 9007199254740993]"),
    8 -> MustNotMerge
  )

  for ((caseNumber, expected) <- closeValues) {
    val mergeCase = mergeCases(caseNumber)
    checkCell(
      caseNumber,
      "different values: integer 2^53 + 1 and float 2^53",
      mergeCase.original("[1, 9007199254740993]", "[9007199254740992.0, 3]"),
      expected.mapMerged(mergeCase.merged)
    )
  }

  private def checkCell(caseNumber: Int, column: String, original: String, expected: Expected): Unit = {
    test(s"case $caseNumber (${mergeCases(caseNumber).template}), $column: `$original` ${expected.describe}") {
      expected match {
        case Merges(to)                 => shouldRewrite(s"RETURN $original AS result", s"RETURN $to AS result")
        case MustNotMerge | MayMerge(_) => shouldNotRewrite(s"RETURN $original AS result")
      }
    }
  }

  // Three-valued logic in larger expressions

  test("should merge the IN lists without null and leave the one containing null joined by AND") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND NOT v IN [2, null] AND v IN [1, 3] AS result",
      "RETURN v IN [1] AND NOT v IN [2, null] AS result"
    )
  }

  test("should merge IN lists of one predicand when a list of another predicand contains null") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND w IN [null] AND v IN [2, 3] AS result",
      "RETURN v IN [2] AND w IN [null] AS result"
    )
  }

  // `v IN []` is FALSE for every v, so the whole conjunction is FALSE
  test("should replace with false IN lists joined by AND if one of them is empty") {
    shouldRewrite(
      "RETURN v IN [1] AND v IN [2] AND v IN [] AS result",
      "RETURN false AS result"
    )
  }

  // The empty list has the wrong polarity to dominate the conjunction.
  // v = null: original `null AND null AND TRUE` = null; merged FALSE
  test("should not replace with false IN lists without overlap joined by AND with empty NOT IN list") {
    shouldNotRewrite("RETURN v IN [1] AND v IN [2] AND NOT v IN [] AS result")
  }

  // The guard against an empty merged list applies per predicand: v is left as it is, w is still merged
  test("should merge IN lists of one predicand joined by AND when another predicand would merge to an empty list") {
    shouldRewrite(
      "RETURN v IN [1] AND v IN [2] AND w IN [1, 2] AND w IN [2, 3] AS result",
      "RETURN v IN [1] AND v IN [2] AND w IN [2] AS result"
    )
  }

  // Without the list containing null, the other two would merge to an empty list.
  // v = null: original `NOT null AND null AND null` = null; merged FALSE
  // AND is left-associative, so the list containing null is in the subtree the outermost AND merges.
  test("should not replace with false IN lists without overlap and NOT IN list containing null joined by AND") {
    shouldNotRewrite("RETURN NOT v IN [3, null] AND v IN [1] AND v IN [2] AS result")
  }

  // `v IN []` is FALSE for every v, so the whole conjunction is FALSE, whatever the list containing null evaluates to.
  // AND is left-associative, so the list containing null is in the subtree the outermost AND merges.
  test("should replace with false IN lists joined by AND with an empty one and NOT IN list containing null") {
    shouldRewrite(
      "RETURN NOT v IN [3, null] AND v IN [1] AND v IN [] AS result",
      "RETURN false AS result"
    )
  }

  // v = null, n.q = 2: original `null AND TRUE AND NOT null` = null; merged FALSE
  test("should not replace with false IN list and same NOT IN list joined by AND around an unrelated predicate") {
    shouldNotRewrite("RETURN v IN [1] AND n.q > 1 AND NOT v IN [1] AS result")
  }

  // Replacing the inner conjunction by FALSE would leave `w IN [3]`.
  // v = null, w = 4: original `(null AND null) OR FALSE` = null; rewritten FALSE
  test("should not replace with false nested IN lists without overlap joined by AND") {
    shouldNotRewrite("RETURN (v IN [1] AND v IN [2]) OR w IN [3] AS result")
  }

  // NaN and Infinity

  // NaN never matches, so v = NaN makes both sides FALSE, and dropping NaN from the IN list changes nothing
  test("should merge IN list containing NaN with NOT IN list containing NaN joined by AND") {
    shouldRewrite(
      "RETURN v IN [1, NaN] AND NOT v IN [NaN] AS result",
      "RETURN v IN [1] AS result"
    )
  }

  // `v IN [NaN]` is FALSE for a non-null v and null for a null v, just like the original
  test("should merge IN lists containing NaN joined by AND") {
    shouldRewrite(
      "RETURN v IN [NaN, 1] AND v IN [NaN, 2] AS result",
      "RETURN v IN [NaN] AS result"
    )
  }

  // `NOT v IN [NaN]` is TRUE for a non-null v and null for a null v, just like the original
  test("should merge NOT IN lists containing NaN joined by OR") {
    shouldRewrite(
      "RETURN NOT v IN [NaN, 1] OR NOT v IN [NaN, 2] AS result",
      "RETURN NOT v IN [NaN] AS result"
    )
  }

  // The original is FALSE for every non-null v, including NaN, but null for a null v
  test("should not replace with false IN list of NaN and NOT IN list of NaN joined by AND") {
    shouldNotRewrite("RETURN v IN [NaN] AND NOT v IN [NaN] AS result")
  }

  // Equal values spelled differently

  // 0 = 0.0, so v = 0 is in both lists
  test("should merge IN lists of integer zero and float zero joined by AND") {
    shouldRewrite(
      "WITH 0 AS v RETURN v IN [0] AND v IN [0.0] AS result",
      "WITH 0 AS v RETURN v IN [0] AS result"
    )
  }

  test("should make list elements with equal values distinct") {
    shouldRewrite(
      "MATCH (n:Label) WHERE n.prop1 IN [43, 44] AND n.prop2 IN [2, 2.0, 0x2, -0.0, 0]",
      "MATCH (n:Label) WHERE n.prop1 IN [43, 44] AND n.prop2 IN [2, -0.0]"
    )
  }

  // Boolean structure

  test("should merge a series of IN and NOT IN joined by AND") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND NOT a.prop IN [3,4,5] AND a.prop IN [2,3,4] RETURN *",
      "MATCH (a) WHERE a.prop IN [2] RETURN *"
    )
  }

  test("should merge (NOT mergeable) AND (NOT mergeable)") {
    shouldRewrite(
      "MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3]) AND NOT (a.prop IN [3,4] AND a.prop IN [4,5])",
      "MATCH (a) WHERE NOT a.prop IN [2,4]"
    )
  }

  test("should merge a right-skewed tree of ORs") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1] OR (v IN [2] OR (v IN [3] OR v IN [4])) AS result",
      "WITH 1 AS v RETURN v IN [1, 2, 3, 4] AS result"
    )
  }

  test("should merge a tree of ORs") {
    shouldRewrite(
      "WITH 1 AS v RETURN (v IN [1] OR (v IN [2])) OR ((v IN [3] OR v IN [4]) OR v IN [5]) AS result",
      "WITH 1 AS v RETURN v IN [1, 2, 3, 4, 5] AS result"
    )
  }

  test("should merge a right-skewed tree of ANDs") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1, 5] AND (v IN [2, 5] AND (v IN [3, 5] AND v IN [4, 5])) AS result",
      "WITH 1 AS v RETURN v IN [5] AS result"
    )
  }

  test("should merge a tree of ANDs") {
    shouldRewrite(
      """
        |WITH 1 AS v
        |RETURN (v IN [1, 'tree'] AND (v IN [2, 'tree']))
        |  AND ((v IN [3, 'tree'] AND v IN [4, 'tree']) AND v IN [5, 'tree']) AS result""".stripMargin,
      """
        |WITH 1 AS v
        |RETURN v IN ['tree'] AS result""".stripMargin
    )
  }

  test("should merge two mergeable ANDs joined by OR") {
    shouldRewrite(
      "MATCH (a) WHERE (a.prop IN [1,2,3] AND a.prop IN [2,3,4]) OR (a.prop IN [2,3,4] AND a.prop IN [3,4,5]) RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3,4] RETURN *"
    )
  }

  test(
    "should merge two mergeable ORs joined by AND"
  ) {
    shouldRewrite(
      "MATCH (a) WHERE (a.prop IN [1,2,3] OR a.prop IN [2,3,4]) AND (a.prop IN [2,3,4] OR a.prop IN [3,4,5]) RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3,4] RETURN *"
    )
  }

  test("should merge AND within NOT") {
    shouldRewrite(
      "MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3])",
      "MATCH (a) WHERE NOT a.prop IN [2]"
    )
  }

  test("should merge three lists joined by OR for a property") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] OR a.prop IN [3,4,5] RETURN *",
      "MATCH (a) WHERE a.prop IN [1,2,3,4,5] RETURN *"
    )
  }

  test("should replace AND whose LHS is always true with the RHS") {
    shouldRewrite(
      "MATCH (n) RETURN (n.prop IN [1,2,3] OR TRUE) AND n.prop IN [3,4,5] AS FOO",
      "MATCH (n) RETURN n.prop IN [3,4,5] AS FOO"
    )
  }

  test("should replace OR whose LHS is always false with the RHS") {
    shouldRewrite(
      "MATCH (n) RETURN (n.prop IN [1,2,3] AND FALSE) OR n.prop IN [3,4,5] AS FOO",
      "MATCH (n) RETURN n.prop IN [3,4,5] AS FOO"
    )
  }

  test("should replace (NOT _ IN [] AND predicate) with (predicate) as LHS is always TRUE") {
    shouldRewrite(
      "RETURN NOT n.prop IN [] AND n.prop IS NOT NULL AS result",
      "RETURN n.prop IS NOT NULL AS result"
    )
  }

  test("should make list elements distinct") {
    shouldRewrite(
      "MATCH (n:Label) WHERE n.prop1 IN [43, 44] AND n.prop2 IN [3, 3]",
      "MATCH (n:Label) WHERE n.prop1 IN [43, 44] AND n.prop2 IN [3]"
    )
  }

  test("should merge equal comparisons of different order") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1, 2, 3] OR a.prop IN [3, 2, 1] RETURN a",
      "MATCH (a) WHERE a.prop IN [1, 2, 3] RETURN a"
    )
  }

  test("should merge comparisons of literals for id function") {
    shouldRewrite(
      "MATCH (a) WHERE id(a) IN [42] OR id(a) IN [13] RETURN a",
      "MATCH (a) WHERE id(a) IN [42, 13] RETURN a"
    )
  }

  test("should not rewrite OR with LHS and RHS that are empty list literals") {
    shouldNotRewrite("RETURN [] OR [] AS n")
  }

  test("should not rewrite OR with LHS and RHS that are non-empty list literals") {
    shouldNotRewrite("RETURN [1] OR [1] AS n")
  }

  test("should not rewrite AND with LHS and RHS that are empty list literals") {
    shouldNotRewrite("RETURN [] AND [] AS n")
  }

  test("should not rewrite AND with LHS and RHS that are non-empty list literals") {
    shouldNotRewrite("RETURN [1] AND [1] AS n")
  }

  // Predicands

  test("should merge only IN comparisons for the same predicand") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1] OR (w IN [2] OR (v IN [3] OR v IN [4])) AS result",
      "WITH 1 AS v RETURN v IN [1, 3, 4] OR w IN [2] AS result"
    )
  }

  test("should merge when lhs expression is parameter") {
    shouldRewrite(
      "RETURN NOT $p IN [1, 2] AND NOT $q IN [2, 3] AND NOT $p IN [1, 3] AS result",
      "RETURN NOT $p IN [1, 2, 3] AND NOT $q IN [2, 3] AS result"
    )
  }

  test("should merge for multiple predicands") {
    shouldRewrite(
      "WITH 1 AS p, 2 AS q RETURN NOT p IN [1, 2] AND NOT q IN [2, 3] AND NOT p IN [1, 3] AND q IN [4] AS result",
      "WITH 1 AS p, 2 AS q RETURN NOT p IN [1, 2, 3] AND q IN [4] AS result"
    )
  }

  test("should merge lists for multiple properties") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND a.foo IN ['foo','bar'] AND a.prop IN [2,3,4] AND a.foo IN ['bar'] RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3] AND a.foo IN ['bar'] RETURN *"
    )
  }

  test("should not replace with false a sequence of AND IN where non-overlapping lists for one of two predicands") {
    shouldNotRewrite("RETURN v IN [1, 2] AND u IN [3, 4] AND v IN [5, 6] AS result")
  }

  test("should not replace with true a sequence of OR NOT IN where non-overlapping lists for one of two predicands") {
    shouldNotRewrite("RETURN NOT v IN [1, 2] OR NOT u IN [3, 4] OR NOT v IN [5, 6] AS result")
  }

  test("should not merge IN for different variables") {
    shouldNotRewrite("MATCH (a)-->(b) WHERE a.prop IN [1, 2, 3] AND b.prop IN [2, 3, 4]")
  }

  test("should merge IN comparisons across AND for same predicand and ignore unrelated comparison") {
    shouldRewrite(
      "RETURN (m.id IN [21,22,31] AND (NOT m.id IN [22,32]) AND (t.id <> 1 AND t.id <> 3)) AS result",
      "RETURN (m.id IN [21,31] AND (t.id <> 1 AND t.id <> 3)) AS result"
    )
  }

  test("should merge IN comparisons across OR for same predicand and ignore unrelated comparison") {
    shouldRewrite(
      "RETURN (m.id IN [21,22,31] OR (m.id = 'other') OR (t.id <> 1 AND t.id <> 3) OR m.id IN [1]) AS result",
      "RETURN (m.id IN [21,22,31,1] OR (m.id = 'other') OR (t.id <> 1 AND t.id <> 3)) AS result"
    )
  }

  test("should merge IN comparisons with unrelated comparisons higher in a tree of boolean binaries") {
    shouldRewrite(
      "RETURN (m.id = 'other' OR (t.id <> 1 AND (t.id <> 3 OR m.id IN [21,22,31] OR m.id IN [1]))) AS result",
      "RETURN (m.id = 'other' OR (t.id <> 1 AND (t.id <> 3 OR m.id IN [21,22,31,1]))) AS result"
    )
  }

  test("should skip merging list of same predicand if in AND with different predicand nested in series of ORs") {
    shouldRewrite(
      "WITH v IN [1, 2] OR (b IN [4] OR v IN [2, 3]) OR (c IN [5] AND NOT v IN [0, 1, 2]) OR NOT v IN [3, 4] AS result",
      "WITH NOT v IN [4] OR b IN [4] OR (c IN [5] AND NOT v IN [0, 1, 2]) AS result"
    )
  }

  test("should not merge across AND and OR when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] AND ($q IN [1.2, 1.3] OR $p IN ['b', 'd']) AS result")
  }

  test("should not merge across OR and AND when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] OR ($q IN [1.2, 1.3] AND $p IN ['b', 'd']) AS result")
  }

  test("should not merge across AND NOT when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] AND NOT ($q IN [1.2, 1.3] AND $p IN ['b', 'd']) AS result")
  }

  test("should not merge across OR NOT when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] OR NOT ($q IN [1.2, 1.3] OR $p IN ['b', 'd']) AS result")
  }

  test("should not merge lists with an intermediate NOT that does not immediately contain IN") {
    shouldNotRewrite(
      "RETURN NOT $p IN [1, 2] OR NOT ($q IN [3, 4] OR $p IN [2, 3])"
    )
  }

  // Scopes

  test("should not merge IN that are in different any()") {
    shouldNotRewrite("MATCH (n) WHERE ANY(i IN n.prop WHERE i IN [1,2]) AND ANY(i IN n.prop WHERE i IN [3]) RETURN n")
  }

  test("should merge IN that are in the same any()") {
    shouldRewrite(
      "MATCH (n) WHERE ANY(i IN n.prop WHERE i IN [1,2] AND i IN [2,3]) RETURN n",
      "MATCH (n) WHERE ANY(i IN n.prop WHERE i IN [2]) RETURN n"
    )
  }

  test("should not merge IN comparisons across QPP boundary") {
    shouldRewrite(
      "MATCH (a WHERE v IN ['a', 'b'] AND NOT v IN ['b']) ((l)-->(r) WHERE v IN [4.5, 10.1] AND NOT v IN [10.01])+",
      "MATCH (a WHERE v IN ['a']) ((l)-->(r) WHERE v IN [4.5, 10.1])+"
    )
  }

  test("should not merge IN comparisons across EXISTS subquery boundary") {
    shouldRewrite(
      "MATCH (a) WHERE v IN ['a', 'b'] AND NOT v IN ['b'] AND EXISTS { MATCH (a)-->(b) WHERE v IN ['c'] }",
      "MATCH (a) WHERE v IN ['a'] AND EXISTS { MATCH (a)-->(b) WHERE v IN ['c'] }"
    )
  }

  test("should not merge IN comparisons across pattern comprehension boundary") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND NOT v IN [2, 3] AND size([v IN $p WHERE v IN [1]]) > 0 AS result",
      "RETURN v IN [1] AND size([v IN $p WHERE v IN [1]]) > 0 AS result"
    )
  }

  // List elements that are not literals

  test("should not consolidate lists of integers with lists of expressions") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1] OR (w IN [2] OR (v IN [v IN [3]] OR v IN [4])) AS result",
      "WITH 1 AS v RETURN v IN [1, 4] OR w IN [2] OR v IN [v IN [3]] AS result"
    )
  }

  test("should not collapse collections containing function invocations") {
    shouldNotRewrite("MATCH (a) WHERE id(a) IN [42] OR id(a) IN [rand()] RETURN a")
  }

  test("should not collapse non-equal collections containing function invocations") {
    shouldNotRewrite("MATCH (a) WHERE a.prop IN [42, rand()] OR a.prop IN [rand(), 54] RETURN a")
  }

  // Each rand() is evaluated on its own, so the two comparisons are not the same
  test("should not collapse equal collections containing function invocations") {
    shouldNotRewrite("MATCH (a) WHERE a.prop IN [1, rand(), 'string'] OR a.prop IN [1, rand(), 'string'] RETURN a")
  }

  test("should not merge IN lists containing properties") {
    shouldNotRewrite(
      "MATCH (a WHERE v IN [a.p] AND NOT v IN [a.q]) ((l)-->(r) WHERE v IN [l.p, r.p] AND NOT v IN [l.q, r.q])+"
    )
  }

  test("should not merge lists containing variables") {
    shouldNotRewrite("RETURN v IN [1, 2] AND v IN [2, a] AS result")
  }

  test("should not merge lists containing parameters") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND NOT v IN [3, $x] AND NOT v IN [2, 3] AS result",
      "RETURN v IN [1] AND NOT v IN [3, $x] AS result"
    )
  }

  test("should not merge lists containing date functions") {
    shouldNotRewrite(
      "RETURN NOT v IN [date('2025-02-02'), 2] AND v IN [2, 3] AS result"
    )
  }

  private def shouldRewrite(from: String, to: String): Unit = {
    val exceptionFactory = OpenCypherExceptionFactory(None)
    val original = parse(from, exceptionFactory).asInstanceOf[Query]
    val expected = parse(to, exceptionFactory).asInstanceOf[Query]
    val common: Rewriter = flattenBooleanOperators.instance(CancellationChecker.NeverCancelled)
    val result = mergeInPredicates.instance(original)

    common(result) should equal(common(expected))
  }

  private def shouldNotRewrite(query: String): Unit = shouldRewrite(query, query)

}
