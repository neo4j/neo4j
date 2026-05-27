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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram

import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.planner.spi.histogram.Bucket
import org.neo4j.cypher.internal.planner.spi.histogram.Histogram
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Selectivity

object EstimateSelectivityUsingHistogram {

  def sumBucketSelectivityEstimates(
    histogram: Histogram,
    inequalities: NonEmptyList[InequalityExpression]
  ): Selectivity = {
    // The '.toList' is needed because two buckets can lead to the same selectivity.
    // Keeping it as a set would then incorrectly drop one of those selectivity values.
    val selectivitySum = histogram.buckets.toList.map(bucket => {
      estimateBucketSelectivity(bucket, inequalities)
    }).sum
    Selectivity(selectivitySum)
  }

  final def estimateBucketSelectivity(bucket: Bucket, inequalities: NonEmptyList[InequalityExpression]): Double = {
    // x.prop >= 5 AND x.prop <= 8
    // Bucket                  [l]--------------(u)
    // Predicates                  [5]-><--[8]
    val outOfRange =
      inequalities
        .map({ inequalityPredicate =>
          bucket.selectivity - estimateBucketSelectivity(bucket, inequalityPredicate)
        })
        .iterator.sum
        .min(bucket.selectivity)

    bucket.selectivity - outOfRange
  }

  /**
   * Parameters extracted from inequality predicates like n.prop <= 5 that are needed to estimate the selectivities
   * within the buckets that satisfy the inequality predicates.
   *
   * @param inequalityLiteral        The literal value in the inequality predicate, converted to a double (e.g., 5.0 in n.prop <= 5)
   * @param inclToExclConverterValue A value (named e) to convert from an inclusive boundary to an exclusive boundary
   *                                 and the other way around.
   *                                 For an integer domain it consists of adding or subtracting the value 1,
   *                                 and for a floating point domain it consists of adding or subtracting the
   *                                 smallest possible value, i.e. Double.MinPositiveValue.
   *
   *                                 The interval [x, y], where both ends are inclusive,
   *                                 can be transformed to [x, y+e), where the LHS is inclusive and the RHS is exclusive.
   *                                 This allows us to get the range of the interval: y+e - x.
   */
  case class BucketEstimationParameters(inequalityLiteral: Double, inclToExclConverterValue: Double)

  final def estimateBucketSelectivity(bucket: Bucket, operator: InequalityExpression): Double = {
    val inequalityLiteralAndInclusiveToExclusiveConverterValue: BucketEstimationParameters =
      (operator.rhs: @unchecked) match {
        case intLit: SignedDecimalIntegerLiteral => BucketEstimationParameters(intLit.value.toDouble, 1.0)
        case doubleLit: DecimalDoubleLiteral     => BucketEstimationParameters(doubleLit.value, Double.MinPositiveValue)
        // The last two cases cannot be reached due to the checks in ExpressionSelectivityCalculator
        case _: AutoExtractedParameter => throw new UnsupportedOperationException(
            "Cardinality estimation using histograms cannot be done when the value in the inequality expression is auto-parameterized"
          )
        case _ => throw new UnsupportedOperationException(
            s"Cardinality estimation using histograms does not support the value type of ${operator.rhs} in the inequality expression"
          )
      }
    val v = inequalityLiteralAndInclusiveToExclusiveConverterValue.inequalityLiteral
    val e = inequalityLiteralAndInclusiveToExclusiveConverterValue.inclToExclConverterValue
    operator match {
      case _: LessThan =>
        // x.prop < v
        if (v <= bucket.minInclusive) {
          // ( ) means open, i.e. exclusive
          // [ ] means closed, i.e. inclusive
          //                 ---------------------------
          // Range predicate <-----------(v)
          // Bucket                      [l]-----(u)
          //                 ---------------------------
          // Full bucket is out of range
          0.0
        } else if (v >= bucket.maxExclusive) {
          //                 ---------------------------
          // Range predicate <-----------(v)
          // Bucket              [l]-----(u)
          //                 ---------------------------
          // Full bucket is in range
          bucket.selectivity
        } else {
          //                 ---------------------------
          // Range predicate <-----------(v)
          // Bucket                  [l]-----(u)
          //                 ---------------------------
          // Part of the bucket is in range
          bucket.estimatePropLessThanValue(v)
        }
      case _: LessThanOrEqual =>
        // x.prop <= v
        if (v < bucket.minInclusive) {
          //                 ---------------------------
          // Range predicate <-----------[v]
          // Bucket                         [l]-----(u)
          //                 ---------------------------
          // Full bucket is out of range
          0.0
        } else if (v >= (bucket.maxExclusive - e)) {
          //                 ---------------------------
          // Range predicate <-----------[v]
          // Bucket              [l]-----[u](u)
          //                 ---------------------------
          // Full bucket is in range
          bucket.selectivity
        } else {
          //                 ---------------------------
          // Range predicate <-----------[v]
          // Bucket                 [l]--------(u)
          //                 ---------------------------
          // Part of the bucket is in range
          bucket.estimatePropLessOrEqualThanValue(v, e)
        }
      case _: GreaterThan =>
        bucket.selectivity - estimateBucketSelectivity(bucket, operator.negated)
      case _: GreaterThanOrEqual =>
        bucket.selectivity - estimateBucketSelectivity(bucket, operator.negated)
    }
  }
}
