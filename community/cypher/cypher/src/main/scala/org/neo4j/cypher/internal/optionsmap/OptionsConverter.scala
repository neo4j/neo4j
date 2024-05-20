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
package org.neo4j.cypher.internal.optionsmap

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.util.Locale

trait OptionsConverter[T] {

  val evaluator: ExpressionEvaluator = Evaluator.expressionEvaluator()

  def evaluate(expression: Expression, params: MapValue): AnyValue = {
    evaluator.evaluate(expression, params)
  }

  def convert(options: Options, params: MapValue, config: Option[Config] = None): Option[T] = options match {
    case NoOptions if hasMandatoryOptions =>
      // If there are mandatory options we should call convert with empty options to throw expected errors
      Some(convert(VirtualValues.EMPTY_MAP, config))
    case NoOptions => None
    case OptionsMap(map) => Some(convert(
        VirtualValues.map(
          map.keys.map(_.toLowerCase(Locale.ROOT)).toArray,
          map.view.mapValues(evaluate(_, params)).values.toArray
        ),
        config
      ))
    case OptionsParam(parameter) =>
      val opsMap = params.get(parameter.name)
      opsMap match {
        case mv: MapValue =>
          val builder = new MapValueBuilder()
          mv.foreach((k, v) => builder.add(k.toLowerCase(Locale.ROOT), v))
          Some(convert(builder.build(), config))
        case _ =>
          throw new InvalidArgumentsException(s"Could not $operation with options '$opsMap'. Expected a map value.")
      }
  }

  implicit def operation: String

  def convert(options: MapValue, config: Option[Config]): T

  protected val hasMandatoryOptions: Boolean = false
}
