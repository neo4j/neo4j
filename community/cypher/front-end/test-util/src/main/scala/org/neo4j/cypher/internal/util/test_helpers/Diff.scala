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
package org.neo4j.cypher.internal.util.test_helpers

import org.neo4j.cypher.internal.util.test_helpers.ChangeInstruction.Add
import org.neo4j.cypher.internal.util.test_helpers.ChangeInstruction.Change
import org.neo4j.cypher.internal.util.test_helpers.ChangeInstruction.Keep
import org.neo4j.cypher.internal.util.test_helpers.ChangeInstruction.Remove
import org.neo4j.cypher.internal.util.test_helpers.ChangeInstruction.Replace

sealed trait ChangeInstruction

object ChangeInstruction {
  trait Keep extends ChangeInstruction
  trait Add extends ChangeInstruction
  trait Remove extends ChangeInstruction
  trait Change extends ChangeInstruction
  trait Replace extends ChangeInstruction
}

sealed trait StringLineChangeInstruction extends ChangeInstruction {
  def line: String
}

case class KeepLine(line: String) extends StringLineChangeInstruction with Keep
case class AddLine(line: String) extends StringLineChangeInstruction with Add
case class RemoveLine(line: String) extends StringLineChangeInstruction with Remove
case class ReplaceLine(line: String, replacement: String) extends StringLineChangeInstruction with Replace

object StringLinesDiff {

  def apply(before: String, after: String, harmonize: String => String = identity): Seq[StringLineChangeInstruction] = {
    val beforeLines = before.linesIterator.toIndexedSeq
    val afterLines = after.linesIterator.toIndexedSeq

    LcsBasedSeqDiff(
      beforeLines,
      afterLines,
      elementDiff = (before: String, after: String) => {
        if (harmonize(before) == harmonize(after)) {
          KeepLine(before)
        } else {
          ReplaceLine(before, after)
        }
      },
      remove = line => RemoveLine(line),
      add = line => AddLine(line),
      secondLevel = false
    )
  }
}

object LcsBasedSeqDiff {

  def apply[ELEMENT, CI](
    before: Seq[ELEMENT],
    after: Seq[ELEMENT],
    elementDiff: (ELEMENT, ELEMENT) => CI & ChangeInstruction,
    remove: ELEMENT => CI & Remove,
    add: ELEMENT => CI & Add,
    secondLevel: Boolean = false
  ): Seq[CI] = {

    val elementDiffCached: MemorizedFunction2[ELEMENT, ELEMENT, (ELEMENT, ELEMENT), CI & ChangeInstruction] = {
      given MemorizationKeyEncoder[(ELEMENT, ELEMENT), (ELEMENT, ELEMENT)] =
        new MemorizationByIdentity[(ELEMENT, ELEMENT)]

      MemorizedFunction2[ELEMENT, ELEMENT, (ELEMENT, ELEMENT), CI & ChangeInstruction](elementDiff)
    }
    // eq defines which elements get aligned
    val eq: (ELEMENT, ELEMENT) => Boolean = (b, a) =>
      elementDiffCached(b, a) match {
        case _: Keep   => true // at the first level we align elements considered equal
        case _: Change => secondLevel // at the second level we align elements considered changed
        case _         => false // we never aligned elements considered different
      }

    /**
     * cf. https://florian.github.io/diffing/
     */
    def lcsSteps(before: Seq[ELEMENT], after: Seq[ELEMENT], eq: (ELEMENT, ELEMENT) => Boolean): Seq[(Int, Int)] = {

      def lcs[A](a: Seq[(A, Int)], b: Seq[(A, Int)], eq: (A, A) => Boolean): Seq[(Int, Int)] = {
        type DP = MemorizedFunction2[Seq[(A, Int)], Seq[(A, Int)], (Int, Int), Seq[(Int, Int)]]

        given MemorizationKeyEncoder[(Seq[(A, Int)], Seq[(A, Int)]), (Int, Int)] =
          (v1: (Seq[(A, Int)], Seq[(A, Int)])) => (v1._1.length, v1._2.length)

        implicit val o: Ordering[Seq[(Int, Int)]] = Ordering.by(_.length)

        lazy val f: DP = MemorizedFunction2 {
          case (Nil, _) | (_, Nil)                  => Nil
          case (x +: xs, y +: ys) if eq(x._1, y._1) => (x._2, y._2) +: f(xs, ys)
          case (x, y)                               => o.max(f(x.tail, y), f(x, y.tail))
        }

        f(a, b)
      }

      lcs(before.zipWithIndex, after.zipWithIndex, eq)
    }

    val changeInstructions: Seq[CI] = {
      val lcsPairs = lcsSteps(before, after, eq) :+ (before.length, after.length)
      val lcsPairWindows = ((-1, -1), lcsPairs.head) +: lcsPairs.zip(lcsPairs.drop(1))
      val changeInstructions = lcsPairWindows.flatMap {
        case ((bIxPrec, aIxPrec), (bIx, aIx)) =>
          val changeInstructions =
            if (secondLevel)
              LcsBasedSeqDiff(
                before.slice(bIxPrec + 1, bIx),
                after.slice(aIxPrec + 1, aIx),
                elementDiffCached,
                remove,
                add,
                secondLevel = true
              )
            else
              SimpleTopDownSeqDiff(
                before.slice(bIxPrec + 1, bIx),
                after.slice(aIxPrec + 1, aIx),
                elementDiffCached,
                remove,
                add
              )
          if (bIx < before.length && aIx < after.length) {
            val (b, a) = (before(bIx), after(aIx))
            changeInstructions :+ elementDiffCached(b, a)
          } else {
            changeInstructions
          }
      }
      changeInstructions
    }
    changeInstructions
  }
}

object SimpleTopDownSeqDiff {

  def apply[ELEMENT, CI](
    before: Seq[ELEMENT],
    after: Seq[ELEMENT],
    elementDiff: (ELEMENT, ELEMENT) => CI & ChangeInstruction,
    remove: ELEMENT => CI & Remove,
    add: ELEMENT => CI & Add
  ): Seq[CI] = {
    def beforeSome: Seq[Some[ELEMENT]] = before.map(Some(_))

    def afterSome: Seq[Some[ELEMENT]] = after.map(Some(_))

    val changeInstructions: Seq[CI] = beforeSome.zipAll(afterSome, None, None).collect {
      case (Some(b), None)    => remove(b)
      case (None, Some(a))    => add(a)
      case (Some(b), Some(a)) => elementDiff(b, a)
    }
    changeInstructions
  }
}
trait MemorizationKeyEncoder[INPUT, KEY] extends (INPUT => KEY)

class MemorizationByIdentity[INPUT] extends MemorizationKeyEncoder[INPUT, INPUT] {
  override def apply(v1: INPUT): INPUT = v1
}

/**
 * Generic way to create memoized functions
 *
 * @see https://stackoverflow.com/questions/25129721
 */
case class MemorizedFunction1[INPUT, KEY, OUPUT](f: INPUT => OUPUT)(implicit
  keyEncoder: MemorizationKeyEncoder[INPUT, KEY]) extends (INPUT => OUPUT) {
  import scala.collection.mutable

  val cache: mutable.Map[KEY, OUPUT] = mutable.Map.empty[KEY, OUPUT]

  override def apply(x: INPUT): OUPUT = cache.getOrElseUpdate(keyEncoder(x), f(x))
}

case class MemorizedFunction2[INPUT1, INPUT2, KEY, OUPUT](f: (INPUT1, INPUT2) => OUPUT)(implicit
  keyEncoder: MemorizationKeyEncoder[(INPUT1, INPUT2), KEY]) extends ((INPUT1, INPUT2) => OUPUT) {

  import scala.collection.mutable

  val cache: mutable.Map[KEY, OUPUT] = mutable.Map.empty[KEY, OUPUT]

  override def apply(x1: INPUT1, x2: INPUT2): OUPUT = cache.getOrElseUpdate(keyEncoder((x1, x2)), f(x1, x2))
}
