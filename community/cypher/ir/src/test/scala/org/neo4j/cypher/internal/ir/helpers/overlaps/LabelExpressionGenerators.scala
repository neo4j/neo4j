/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ir.helpers.overlaps

import org.scalacheck.Gen

trait LabelExpressionGenerators {

  // Maximum size of the pool of labels used when generating expressions, must be >= 1
  def maximumNumberOfLabels: Int = 12

  final val zero: LabelExpression = LabelExpression.wildcard.and(LabelExpression.wildcard.not)
  final val one: LabelExpression = LabelExpression.wildcard.or(LabelExpression.wildcard.not)

  def genLabelExpression: Gen[LabelExpression] =
    genLabels.flatMap(genLabelExpressionWithLabels)

  def gen2LabelExpressions: Gen[(LabelExpression, LabelExpression)] =
    for {
      allLabels <- genLabels
      first <- genLabelExpressionWithLabels(allLabels)
      second <- genLabelExpressionWithLabels(allLabels)
    } yield (first, second)

  def gen3LabelExpressions: Gen[(LabelExpression, LabelExpression, LabelExpression)] =
    for {
      allLabels <- genLabels
      first <- genLabelExpressionWithLabels(allLabels)
      second <- genLabelExpressionWithLabels(allLabels)
      third <- genLabelExpressionWithLabels(allLabels)
    } yield (first, second, third)

  def genListOfLabelExpressions: Gen[List[LabelExpression]] =
    genLabels.flatMap { labels =>
      Gen.listOf(genLabelExpressionWithLabels(labels))
    }

  def genNonEmptyListOfLabelExpressions: Gen[List[LabelExpression]] =
    genLabels.flatMap { labels =>
      Gen.nonEmptyListOf(genLabelExpressionWithLabels(labels))
    }

  def genLabelExpressions: Gen[LabelExpressions] =
    genListOfLabelExpressions.map(LabelExpressions.fold)

  def genLabels: Gen[Set[String]] =
    Gen.chooseNum(1, maximumNumberOfLabels).flatMap { n =>
      Gen.containerOfN[Set, String](n, nonEmptyAlphaNumStr)
    }

  def nonEmptyAlphaNumStr: Gen[String] =
    for {
      head <- Gen.alphaChar
      tail <- Gen.listOfN(5, Gen.alphaNumChar)
    } yield (head :: tail).mkString

  def genLabelExpressionWithLabels(allLabels: Set[String]): Gen[LabelExpression] =
    Gen.sized { size =>
      if (size > 1) {
        Gen.oneOf(
          Gen.const(LabelExpression.wildcard),
          Gen.oneOf(allLabels.toSeq).map(LabelExpression.label),
          Gen.resize(size - 1, genLabelExpressionWithLabels(allLabels).map(_.not)),
          binaryExpression(genLabelExpressionWithLabels(allLabels), _.and(_)),
          binaryExpression(genLabelExpressionWithLabels(allLabels), _.or(_)),
          binaryExpression(genLabelExpressionWithLabels(allLabels), _.xor(_))
        )
      } else {
        Gen.oneOf(
          Gen.const(LabelExpression.wildcard),
          Gen.oneOf(allLabels.toSeq).map(LabelExpression.label)
        )
      }
    }

  def binaryExpression(
    gen: Gen[LabelExpression],
    f: (LabelExpression, LabelExpression) => LabelExpression
  ): Gen[LabelExpression] =
    for {
      size <- Gen.size
      left <- Gen.resize(size / 2, gen)
      right <- Gen.resize(size / 2, gen)
    } yield f(left, right)
}
