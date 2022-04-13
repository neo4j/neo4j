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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.evaluator.EvaluationException
import org.neo4j.cypher.internal.evaluator.StaticEvaluation
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.logging.Level
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.util.function.Supplier
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class DriverSettingsEvaluator(proceduresSupplier: Supplier[GlobalProcedures]) {
  private val evaluator = new StaticEvaluation.StaticEvaluator(proceduresSupplier)

  def evaluate(expression: Expression, params: MapValue): AnyValue = {
    try {
      evaluator.evaluate(expression, params)
    } catch {
      case e:EvaluationException =>
        throw new InvalidArgumentsException(s"Failed evaluating the given driver settings.", e)
    }
  }

  def convert(driverSettings: Option[Either[Map[String, Expression], Parameter]], params: MapValue, operation: String): Option[Map[String, AnyValue]] = driverSettings.map {
    case Left(map) => DriverSettingsEvaluator.convert(VirtualValues.map(
      map.keys.map(_.toLowerCase).toArray,
      map.mapValues(v =>
        evaluate(v, params)
      ).values.toArray), operation)
    case Right(parameter) =>
      val settings = params.get(parameter.name)
      settings match {
        case mv: MapValue =>
          val builder = new MapValueBuilder()
          mv.foreach((k, v) => builder.add(k.toLowerCase(), v))
          DriverSettingsEvaluator.convert(builder.build(), operation)
        case _ =>
          throw new InvalidArgumentsException(s"Failed to $operation: Invalid driver settings '$settings'. Expected a map value.")
      }
  }
}

object DriverSettingsEvaluator {


  private def convert(settings: MapValue, operation: String): Map[String, AnyValue] = {
    val ssl_enforced = "ssl_enforced"
    val connection_timeout = "connection_timeout"
    val connection_max_lifetime = "connection_max_lifetime"
    val connection_pool_acquisition_timeout = "connection_pool_acquisition_timeout"
    val connection_pool_idle_test = "connection_pool_idle_test"
    val connection_pool_max_size = "connection_pool_max_size"
    val logging_level = "logging_level"
    val validKeys = Set(ssl_enforced, connection_timeout, connection_max_lifetime, connection_pool_acquisition_timeout,
      connection_pool_idle_test, connection_pool_max_size, logging_level)

    val invalidKeys = settings.keySet().asScala.toSet.diff(validKeys)

    if (invalidKeys.nonEmpty)
      throw new InvalidArgumentsException(
        s"Failed to $operation: Invalid driver setting(s) provided: ${invalidKeys.mkString(", ")}. Valid driver settings are: ${validKeys.mkString(", ")}"
      )
      
    def throwExceptionWhenInvalidValue(key: String, expectedType: String) =
      throw new InvalidArgumentsException(s"Failed to $operation: Invalid driver settings value for '$key'. Expected $expectedType value.")

    def getDurationSetting(key: String, allowNegative: Boolean = true): (String, Value) =
      settings.getOption(key).map {
        case duration: DurationValue =>
          if (!allowNegative && duration.compareTo(DurationValue.ZERO) < 0) {
            throw new InvalidArgumentsException(
              s"Failed to $operation: Invalid driver settings value for '$key'. Negative duration is not allowed.")
          } else {
            key -> duration
          }
        case _ => throwExceptionWhenInvalidValue(key, "a duration")
      }.getOrElse(key -> Values.NO_VALUE)

    def getLoggingLevel: (String, Value) =
      settings.getOption(logging_level).map {
        case loggingLevel: StringValue =>
          (logging_level,
            try {
              Level.valueOf(loggingLevel.stringValue().toUpperCase)
              loggingLevel.toUpper
            } catch {
              case _: IllegalArgumentException =>
                throw new InvalidArgumentsException(
                  s"Failed to $operation: Invalid driver settings value for '$logging_level'. Expected one of ${Level.values().mkString(", ")}.")
            })
        case _ => throwExceptionWhenInvalidValue(logging_level, "a string")
      }.getOrElse(logging_level -> Values.NO_VALUE)

    def getConnectionPoolMaxSize: (String, Value) =
      settings.getOption(connection_pool_max_size).map {
        case poolMaxSize: IntegralValue if poolMaxSize.equals(0) =>
            throw new InvalidArgumentsException(
              s"Failed to $operation: Invalid driver settings value for '$connection_pool_max_size'. Zero is not allowed.")
        case poolMaxSize: IntegralValue => connection_pool_max_size -> poolMaxSize
        case _ => throwExceptionWhenInvalidValue(connection_pool_max_size, "an integer")
      }.getOrElse(connection_pool_max_size -> Values.NO_VALUE)

    Seq(
      settings.getOption(ssl_enforced).map {
        case sslEnabled: BooleanValue => ssl_enforced -> sslEnabled
        case _ => throwExceptionWhenInvalidValue(ssl_enforced, "a boolean")
      }.getOrElse(ssl_enforced -> Values.NO_VALUE),
      getDurationSetting(connection_timeout, allowNegative = false),
      getDurationSetting(connection_max_lifetime),
      getDurationSetting(connection_pool_acquisition_timeout),
      getDurationSetting(connection_pool_idle_test),
      getConnectionPoolMaxSize,
      getLoggingLevel
    ).toMap
  }
}
