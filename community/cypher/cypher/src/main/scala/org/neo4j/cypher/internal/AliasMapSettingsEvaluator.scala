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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.evaluator.EvaluationException
import org.neo4j.cypher.internal.evaluator.StaticEvaluation
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
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

import java.util.Locale

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

class AliasMapSettingsEvaluator(procedures: Procedures, cypherVersion: CypherVersion) {
  private val evaluator = StaticEvaluation.from(procedures, cypherVersion)

  private type ExpressionMapOrParamValue = Either[Map[String, Expression], AnyValue]

  def evaluate(expression: Expression, params: MapValue): AnyValue = {
    try {
      evaluator.evaluate(expression, params, CypherRow.empty)
    } catch {
      case e: EvaluationException => throw InvalidArgumentsException.failedEvaluatingDriverSettings(e)
      case e: Exception           => throw e
    }
  }

  def convertDriverSettings(
    driverSettings: Option[Either[Map[String, Expression], Parameter]],
    params: MapValue,
    operation: String
  ): Option[Map[String, AnyValue]] = {
    driverSettings.map(settings =>
      evaluateMap(params).applyOrElse(
        settings.map(param => params.get(param.name)),
        (paramValue: ExpressionMapOrParamValue) => {
          settings match {
            case Left(_) => throw InvalidArgumentsException.invalidDriverSettingsExpectedMap(
                operation,
                paramValue.toOption.get
              )
            case Right(parameter) =>
              throw InvalidArgumentsException.invalidDriverSettingsExpectedMap42N51(
                operation,
                paramValue.toOption.get,
                parameter.name
              )
          }
        }
      )
    ).map(AliasMapSettingsEvaluator.convert(_, operation))
  }

  def convertPropertiesMap(
    settingsMap: Option[Either[Map[String, Expression], Parameter]],
    params: MapValue,
    operation: String
  ): Option[MapValue] = {
    settingsMap.map(settings =>
      evaluateMap(params).applyOrElse(
        settings.map(param => params.get(param.name)),
        (param: ExpressionMapOrParamValue) =>
          settings match {
            case Left(_) => throw InvalidArgumentsException.invalidPropertiesExpectedMap(operation, param.toOption.get)
            case Right(parameter) => throw InvalidArgumentsException.invalidPropertiesExpectedMap42N51(
                operation,
                param.toOption.get,
                parameter.name
              )
          }
      )
    )
  }

  private val evaluateMap: MapValue => PartialFunction[ExpressionMapOrParamValue, MapValue] = params => {
    case Left(map) =>
      VirtualValues.map(
        map.keys.map(_.toLowerCase(Locale.ROOT)).toArray,
        map.view.mapValues(v =>
          evaluate(v, params)
        ).values.toArray
      )
    case Right(mv: MapValue) =>
      val builder = new MapValueBuilder()
      mv.foreach((k, v) => builder.add(k.toLowerCase(Locale.ROOT), v))
      builder.build()
  }
}

object AliasMapSettingsEvaluator {

  private def convert(settings: MapValue, operation: String): Map[String, AnyValue] = {
    val ssl_enforced = "ssl_enforced"
    val connection_timeout = "connection_timeout"
    val connection_max_lifetime = "connection_max_lifetime"
    val connection_pool_acquisition_timeout = "connection_pool_acquisition_timeout"
    val connection_pool_idle_test = "connection_pool_idle_test"
    val connection_pool_max_size = "connection_pool_max_size"
    val logging_level = "logging_level"
    val validKeys = Set(
      ssl_enforced,
      connection_timeout,
      connection_max_lifetime,
      connection_pool_acquisition_timeout,
      connection_pool_idle_test,
      connection_pool_max_size,
      logging_level
    )

    val invalidKeys = settings.keySet().asScala.toSet.diff(validKeys)

    if (invalidKeys.nonEmpty) {
      throw InvalidArgumentsException.invalidDriverSettings(
        operation,
        invalidKeys.mkString(", "),
        validKeys.toList.asJava
      )
    }
    def throwExceptionWhenInvalidValue(
      key: String,
      expectedType: String,
      expectedCypherType: String,
      invalidValue: AnyValue
    ) = {
      throw InvalidArgumentsException.invalidDriverSettingsValue(
        operation,
        key,
        expectedType,
        expectedCypherType,
        invalidValue
      )
    }

    def getDurationSetting(key: String, allowNegative: Boolean = true): (String, Value) =
      settings.getOption(key).map {
        case duration: DurationValue =>
          if (!allowNegative && duration.compareTo(DurationValue.ZERO) < 0) {
            throw InvalidArgumentsException.driverSettingDurationNotPositive(operation, key, duration)
          } else {
            key -> duration
          }
        case other => throwExceptionWhenInvalidValue(key, "a duration", "DURATION", other)
      }.getOrElse(key -> Values.NO_VALUE)

    def getLoggingLevel: (String, Value) =
      settings.getOption(logging_level).map {
        case loggingLevel: StringValue =>
          (
            logging_level,
            try {
              Level.valueOf(loggingLevel.stringValue().toUpperCase(Locale.ROOT))
              loggingLevel.toUpper
            } catch {
              case _: IllegalArgumentException =>
                throw InvalidArgumentsException.unexpectedDriverSettingValue(
                  operation,
                  String.valueOf(loggingLevel),
                  logging_level,
                  Level.values().map(value => String.valueOf(value)).toList.asJava
                )
            }
          )
        case other => throwExceptionWhenInvalidValue(logging_level, "a string", "STRING", other)
      }.getOrElse(logging_level -> Values.NO_VALUE)

    def getConnectionPoolMaxSize: (String, Value) =
      settings.getOption(connection_pool_max_size).map {
        case poolMaxSize: IntegralValue if poolMaxSize.equals(0) =>
          throw InvalidArgumentsException.connectionPoolSizeZeroNotAllowed(operation, connection_pool_max_size)
        case poolMaxSize: IntegralValue => connection_pool_max_size -> poolMaxSize
        case other => throwExceptionWhenInvalidValue(connection_pool_max_size, "an integer", "INTEGER", other)
      }.getOrElse(connection_pool_max_size -> Values.NO_VALUE)

    Seq(
      settings.getOption(ssl_enforced).map {
        case sslEnabled: BooleanValue => ssl_enforced -> sslEnabled
        case other                    => throwExceptionWhenInvalidValue(ssl_enforced, "a boolean", "BOOLEAN", other)
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
