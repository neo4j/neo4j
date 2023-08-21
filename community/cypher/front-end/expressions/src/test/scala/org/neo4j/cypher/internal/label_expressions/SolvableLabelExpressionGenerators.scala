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
package org.neo4j.cypher.internal.label_expressions

import org.scalacheck.Gen

trait SolvableLabelExpressionGenerators {

  // Maximum size of the pool of labels used when generating expressions, must be >= 1
  def maximumNumberOfLabels: Int = 12

  final val zero: SolvableLabelExpression =
    SolvableLabelExpression.wildcard.and(SolvableLabelExpression.wildcard.not)

  final val one: SolvableLabelExpression =
    SolvableLabelExpression.wildcard.or(SolvableLabelExpression.wildcard.not)

  def genLabelExpression: Gen[SolvableLabelExpression] =
    genLabels.flatMap(genLabelExpressionWithLabels)

  def gen2LabelExpressions: Gen[(SolvableLabelExpression, SolvableLabelExpression)] =
    for {
      allLabels <- genLabels
      first <- genLabelExpressionWithLabels(allLabels)
      second <- genLabelExpressionWithLabels(allLabels)
    } yield (first, second)

  def gen3LabelExpressions: Gen[(SolvableLabelExpression, SolvableLabelExpression, SolvableLabelExpression)] =
    for {
      allLabels <- genLabels
      first <- genLabelExpressionWithLabels(allLabels)
      second <- genLabelExpressionWithLabels(allLabels)
      third <- genLabelExpressionWithLabels(allLabels)
    } yield (first, second, third)

  def genListOfLabelExpressions: Gen[List[SolvableLabelExpression]] =
    genLabels.flatMap { labels =>
      Gen.listOf(genLabelExpressionWithLabels(labels))
    }

  def genNonEmptyListOfLabelExpressions: Gen[List[SolvableLabelExpression]] =
    genLabels.flatMap { labels =>
      Gen.nonEmptyListOf(genLabelExpressionWithLabels(labels))
    }

  def genLabelExpressions: Gen[LazySolvableLabelExpression] =
    genListOfLabelExpressions.map(LazySolvableLabelExpression.fold)

  def genLabels: Gen[Set[String]] =
    Gen.chooseNum(1, maximumNumberOfLabels).flatMap { n =>
      Gen.containerOfN[Set, String](n, nonEmptyAlphaNumStr)
    }

  def nonEmptyAlphaNumStr: Gen[String] =
    for {
      head <- Gen.alphaChar
      tail <- Gen.stringOfN(5, Gen.alphaNumChar)
    } yield head + tail

  def genLabelExpressionWithLabels(allLabels: Set[String]): Gen[SolvableLabelExpression] =
    Gen.recursive[SolvableLabelExpression] { recursively =>
      Gen.sized { size =>
        if (size > 1) {
          Gen.oneOf(
            Gen.const(SolvableLabelExpression.wildcard),
            Gen.oneOf(allLabels).map(SolvableLabelExpression.label),
            Gen.resize(size - 1, recursively.map(_.not)),
            binaryExpression(recursively, _.and(_)),
            binaryExpression(recursively, _.or(_)),
            binaryExpression(recursively, _.xor(_))
          )
        } else {
          Gen.oneOf(
            Gen.const(SolvableLabelExpression.wildcard),
            Gen.oneOf(allLabels).map(SolvableLabelExpression.label)
          )
        }
      }
    }

  def binaryExpression(
    gen: Gen[SolvableLabelExpression],
    f: (SolvableLabelExpression, SolvableLabelExpression) => SolvableLabelExpression
  ): Gen[SolvableLabelExpression] =
    for {
      size <- Gen.size
      left <- Gen.resize(size / 2, gen)
      right <- Gen.resize(size / 2, gen)
    } yield f(left, right)
}
