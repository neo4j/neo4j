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

import org.neo4j.cypher.internal.CypherVersion

sealed trait Outcome

object Outcome {

  /**
   * Weakens an outcome for use against fuzzed / surrounded queries, where the surrounding context may
   * introduce additional, unpredictable errors that an exact assertion could never anticipate. The
   * intended error(s) must still be present, but extras are tolerated:
   *
   *   - [[Exactly]]`(errs)` ⇒ [[AllOf]]`(errs)` — each listed error must still be present; the
   *     leaf-code-set equality is dropped, since surrounding adds codes.
   *   - [[Absent]]          ⇒ [[Ignore]]        — absence of a code cannot be guaranteed once the
   *     query is wrapped in arbitrary context.
   *   - [[AllOf]] / [[Versioned]]               — relaxed structurally, per element / per branch.
   *
   * Single [[GqlError]], [[Passes]] and [[Ignore]] are already surrounding-robust (they only assert a
   * code is present somewhere, or that nothing is asserted) and pass through unchanged. As a result
   * this is the identity transform for every outcome that does not use the exact/absent combinators.
   */
  def relaxedForFuzzing(outcome: Outcome): Outcome = outcome match {
    case Exactly(errors @ _*) => AllOf(errors: _*)
    case _: Absent            => Ignore
    case AllOf(outcomes @ _*) => AllOf(outcomes.map(relaxedForFuzzing): _*)
    case Versioned(default, cases @ _*) =>
      Versioned(relaxedForFuzzing(default), cases.map { case (v, o) => v -> relaxedForFuzzing(o) }: _*)
    case other => other
  }
}

case class Versioned(default: Outcome, map: (CypherVersion, Outcome)*) extends Outcome

object Versioned {
  def ignoreBeforeCypher25(outcome: Outcome): Outcome = Versioned(outcome, CypherVersion.Cypher5 -> Ignore)
  def passesBeforeCypher25(outcome: Outcome): Outcome = Versioned(outcome, CypherVersion.Cypher5 -> Passes)

  def passesCypher25Onwards(beforeCypher25: Outcome): Outcome =
    Versioned(Passes, CypherVersion.Cypher5 -> beforeCypher25)

  def differentOutcomeCypher25Onwards(outcome: Outcome, beforeCypher25: Outcome): Outcome =
    Versioned(outcome, CypherVersion.Cypher5 -> beforeCypher25)
}

sealed trait Unversioned extends Outcome

case object Ignore extends Unversioned

case object Passes extends Unversioned

sealed trait MsgMatch

object MsgMatch {
  case object Equals extends MsgMatch
  case object Contains extends MsgMatch
}

trait GqlError extends Unversioned {
  val num: String
  val msg: String

  /** How this error's [[msg]] is matched against the produced status description. */
  def msgMatch: MsgMatch = MsgMatch.Equals

  final def assertMsg(actualDescription: String): Boolean = msgMatch match {
    case MsgMatch.Equals   => actualDescription.endsWith(msg)
    case MsgMatch.Contains => actualDescription.contains(msg)
  }
}

/**
 * Composite outcomes for queries that produce several semantic errors.
 *
 *   - [[AllOf]]    — every listed outcome must hold against the same run (e.g. two errors present,
 *                    or an error present alongside an [[Absent]] assertion).
 *   - [[Absent]]   — none of the given GQL status codes appears anywhere in any produced error's
 *                    cause chain. Codes are matched by string; messages are irrelevant for absence.
 *   - [[Exactly]]  — the set of produced errors' leaf (most-specific) status codes equals exactly
 *                    the listed errors' codes (ignoring the generic `42001` envelope), and each
 *                    listed error's message must also be present. Compares the set of codes, not
 *                    their multiplicity — several errors sharing a leaf code count as one.
 */
case class AllOf(outcomes: Outcome*) extends Unversioned

case class Absent(codes: String*) extends Unversioned

case class Exactly(errors: GqlError*) extends Unversioned

object GqlError {

  def ander(variables: Seq[String]): String =
    variables.toList match {
      case Nil        => ""
      case List(a)    => s"`$a`"
      case List(a, b) => s"`$a` and `$b`"
      case _          => s"${variables.init.map(v => s"`$v`").mkString(", ")} and `${variables.last}`"
    }

}

case class E42N07(variable: String) extends GqlError {
  override val num: String = "42N07"

  override val msg: String =
    s"The variable `$variable` is shadowing a variable with the same name from the outer scope and needs to be renamed."
}

case class E42N29(variable: String) extends GqlError {
  override val num: String = "42N29"

  override val msg: String = s"Pattern expressions are not allowed to introduce new variables: `$variable`."
}

case object E42N38 extends GqlError {
  override val num: String = "42N38"

  override val msg: String = "Return items must have unique names."
}

case object E42N39 extends GqlError {
  override val num: String = "42N39"

  override val msg: String = "incompatible return column names."

  override def msgMatch: MsgMatch = MsgMatch.Contains
}

case object E42N66 extends GqlError {
  override val num: String = "42N66"

  override val msg: String = "relationship variable already bound"

  override def msgMatch: MsgMatch = MsgMatch.Contains
}

case class E42N67(parameter: String) extends GqlError {
  override val num: String = "42N67"

  override val msg: String = s"Duplicate parameter `$parameter` in local callable signature."
}

case object E42N3A extends GqlError {
  override val num: String = "42N3A"

  override val msg: String = "incompatible conditional query."

  override def msgMatch: MsgMatch = MsgMatch.Contains
}

case object E42N3B extends GqlError {
  override val num: String = "42N3B"

  override val msg: String = "incompatible number of return columns."

  override def msgMatch: MsgMatch = MsgMatch.Contains
}

object E42N44 {

  def apply(variable: String, clause: String): Outcome =
    Versioned(
      E42N44WithGroupBy(variable, clause),
      CypherVersion.Cypher5 -> E42N44WithoutGroupBy(variable, clause)
    )
}

case class E42N44WithGroupBy(variable: String, clause: String) extends GqlError {
  override val num: String = "42N44"

  override val msg: String =
    s"It is not possible to access the variable `$variable` declared before the $clause clause when using `DISTINCT`, an aggregation, or a `GROUP BY` clause."
}

case class E42N44WithoutGroupBy(variable: String, clause: String) extends GqlError {
  override val num: String = "42N44"

  override val msg: String =
    s"It is not possible to access the variable `$variable` declared before the $clause clause when using `DISTINCT` or an aggregation."
}

case class E42N59(variable: String) extends GqlError {
  override val num: String = "42N59"

  override val msg: String = s"Variable `$variable` already declared."
}

case class E42N62(variable: String) extends GqlError {
  override val num: String = "42N62"

  override val msg: String = s"Variable `$variable` not defined."
}

case class E42I18(variables: String*) extends GqlError {
  override val num: String = "42I18"

  override val msg: String =
    s"The expression contains a non-grouping sub-expression ${GqlError.ander(variables)}. In an aggregating context only grouping sub-expressions and constants are allowed."
}

case class E42I24(function: String) extends GqlError {
  override val num: String = "42I24"

  override val msg: String = s"Aggregate expression '$function' is not allowed in this context."
}

case object E42I37 extends GqlError {
  override val num: String = "42I37"

  override val msg: String = "'RETURN *' is not allowed when there are no variables in scope."
}

case class E42I58(variable: String) extends GqlError {
  override val num: String = "42I58"

  override val msg: String =
    s"Entity, '$variable', cannot be created and referenced in the same clause."
}

case class E42I77(name: String) extends GqlError {
  override val num: String = "42I77"

  override val msg: String =
    s"Local callable $name() is already defined."
}

case class E42I79(variables: String*) extends GqlError {
  override val num: String = "42I79"

  override val msg: String =
    s"Aggregation in subclause expression is not allowed to reference variables declared in the same clause: ${GqlError.ander(variables)}."
}

case class E42I80(element: String, alias: String) extends GqlError {
  override val num: String = "42I80"

  override val msg: String =
    s"The grouping element '$element' is not a valid grouping key. A grouping element that references the projection item alias `$alias` must be a simple variable reference."
}

case class E42I80Aggregation(element: String, alias: String) extends GqlError {
  override val num: String = "42I80"

  override val msg: String =
    s"The grouping element '$element' is not a valid grouping key. A grouping element cannot reference the aggregation `$alias`."
}
