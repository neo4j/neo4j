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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.planner.spi.histogram.Histogram
import org.neo4j.cypher.internal.planner.spi.histogram.StandardBucket
import org.neo4j.cypher.internal.util.NonEmptyList

import java.nio.file.Files
import java.nio.file.Paths

import scala.jdk.CollectionConverters.IteratorHasAsScala

object HistogramTestHelper extends AstConstructionTestSupport {

  /**
   *
   * @param nodeOrRelationship Whether the histogram is for a property on a set of nodes or relationships
   * @param labelOrTypeName    The label or relationship type that defines the subset of elements for which the property values are summarized in the histogram
   * @param property           The property whose values are summarized in the histogram
   * @param filePath           Store the output of 'the histogram creation query' from cypher shell or browser in a file and provide the path to this file
   * @return Histogram object that was constructed from the content of the file
   */
  def createHistogramFromFile(
    nodeOrRelationship: EntityType,
    labelOrTypeName: String,
    property: String,
    filePath: String
  ): Histogram = {
    val fileContent = Files.readString(Paths.get(filePath))
    createHistogramFromString(nodeOrRelationship, labelOrTypeName, property, fileContent)
  }

  /**
   *
   * @param nodeOrRelationship Whether the histogram is for a property on a set of nodes or relationships
   * @param labelOrTypeName    The label or relationship type that defines the subset of elements for which the property values are summarized in the histogram
   * @param property           The property whose values are summarized in the histogram
   * @param histogramString    Use the output of 'the histogram creation query' from cypher shell or browser
   * @return Histogram object that was constructed from the content of the string
   */
  def createHistogramFromString(
    nodeOrRelationship: EntityType,
    labelOrTypeName: String,
    property: String,
    histogramString: String
  ): Histogram = {
    // We are only interested in the lines starting with a vertical bar.
    // Other lines do not contain data, i.e. +--------------------------+ for separating purposes
    // The vertical bar used by Neo4j Browser and by cypher-shell are different, therefore we check for both options
    // to support copy-pasting the output of histogram generating queries from Browser and Cypher shell.

    val LineRegex = """\| ([^|]+) +\| ([^|]+) +\| (\d+) +\| ([\d.\-E]+) +\|""".r

    val bucketIterator = histogramString
      // normalize column separators between output from Cypher shell and Browser
      .replaceAll("│", "|")
      .lines().iterator().asScala
      // ignore row separators
      .filter(_.startsWith("|"))
      // ignore the header
      .drop(1)
      .map({
        case LineRegex(min, max, _, selectivity) =>
          StandardBucket(min.trim.toInt, max.trim.toInt, selectivity.toDouble)
        case line => throw new IllegalArgumentException(s"Invalid histogram format: $line in \n$histogramString")
      })

    Histogram(nodeOrRelationship, labelOrTypeName, property, Set.from(bucketIterator))
  }

  // n.prop < v
  def nPropLtV_int(v: Long): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(lessThan(property, literalInt(v))).inequalities
  }

  def nPropLtV_float(v: Double): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(lessThan(property, literalFloat(v))).inequalities
  }

  // n.prop <= v
  def nPropLteV_int(v: Long): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(lessThanOrEqual(property, literalInt(v))).inequalities
  }

  def nPropLteV_float(v: Double): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(lessThanOrEqual(property, literalFloat(v))).inequalities
  }

  // n.prop > v
  def nPropGtV_int(v: Long): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(greaterThan(property, literalInt(v))).inequalities
  }

  def nPropGtV_float(v: Double): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(greaterThan(property, literalFloat(v))).inequalities
  }

  // n.prop >= v
  def nPropGteV_int(v: Long): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(greaterThanOrEqual(property, literalInt(v))).inequalities
  }

  def nPropGteV_float(v: Double): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(greaterThanOrEqual(property, literalFloat(v))).inequalities
  }

  def nPropGtLt_int(lower: Long, upper: Long): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(
      greaterThan(property, literalInt(lower)),
      lessThan(property, literalInt(upper))
    ).inequalities
  }

  def nPropGtLt_float(lower: Double, upper: Double): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(
      greaterThan(property, literalFloat(lower)),
      lessThan(property, literalFloat(upper))
    ).inequalities
  }

  def nPropGtLt_int_float(lower: Long, upper: Double): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(
      greaterThan(property, literalInt(lower)),
      lessThan(property, literalFloat(upper))
    ).inequalities
  }

  def nPropGtLt_float_int(lower: Double, upper: Long): NonEmptyList[InequalityExpression] = {
    val property = prop("n", "prop")
    andedPropertyInequalities(
      greaterThan(property, literalFloat(lower)),
      lessThan(property, literalFloat(upper))
    ).inequalities
  }

  val defaultAllowedError = 1E-6
}
