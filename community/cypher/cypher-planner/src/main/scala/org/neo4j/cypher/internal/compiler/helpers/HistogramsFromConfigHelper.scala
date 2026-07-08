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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.planner.spi.DelegatingGraphStatistics
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.planner.spi.histogram.Bucket
import org.neo4j.cypher.internal.planner.spi.histogram.Histogram
import org.neo4j.cypher.internal.planner.spi.histogram.StandardBucket
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.exceptions.InvalidArgumentException

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

object HistogramsFromConfigHelper {

  /**
   * A histogram can be used to approximate the distribution of a set of values.
   * The set of values is defined by either:
   * - the values for the property `propertyKeyId` for all nodes with the label `labelOrTypeId`
   * - the values for the property `propertyKeyId` for all relationships with the type `labelOrTypeId`
   *
   * Therefore, the key of a histogram consists of the entity-type (node or relationship) the label or type and the property-key.
   */
  case class HistogramKey(entityType: EntityType, labelOrTypeId: NameId, propertyKeyId: PropertyKeyId)

  /**
   * Extends the graph statistics with the histograms from the config.
   *
   * @param histogramsFromConfigWithIdsGrouped A map from the histogram-key to a set of histograms
   * @return A function that takes GraphStatistics and return the same GraphStatistics where the getHistogram methods
   *         take the histograms from the configuration into account
   */
  def graphStatisticsDecoratorWithHistogramsFromConfig(histogramsFromConfigWithIdsGrouped: Map[
    HistogramKey,
    Set[Histogram]
  ]): GraphStatistics => GraphStatistics = {
    def graphStatisticsDecoratorWithHistogramsFromConfig(graphStatistics: GraphStatistics): GraphStatistics = {
      new DelegatingGraphStatistics(graphStatistics) {

        // Get all node histograms from the statistics and from the configuration
        override def getHistograms(labels: Set[LabelId], propertyKey: PropertyKeyId): Set[Histogram] = {
          val applicableHistogramsFromConfig = {
            labels.flatMap(labelId =>
              histogramsFromConfigWithIdsGrouped.getOrElse(
                HistogramKey(NODE_TYPE, labelId, propertyKey),
                Set.empty
              )
            )
          }
          super.getHistograms(labels, propertyKey) ++ applicableHistogramsFromConfig
        }

        // Get all relationship histograms from the statistics and from the configuration
        override def getHistograms(typeId: RelTypeId, propertyKey: PropertyKeyId): Set[Histogram] = {
          val applicableHistogramsFromConfig =
            histogramsFromConfigWithIdsGrouped.getOrElse(
              HistogramKey(RELATIONSHIP_TYPE, typeId, propertyKey),
              Set.empty
            )
          super.getHistograms(typeId, propertyKey) ++ applicableHistogramsFromConfig
        }
      }
    }
    graphStatisticsDecoratorWithHistogramsFromConfig
  }

  /**
   * A histogram is constructed for a specific set of values. The values are defined by the label or type and the property key.
   * The label or type string and the property key string are resolved using the `tokenContext` to obtain the ids
   *
   * @param histogram The histogram for which to obtain the key
   * @param tokenContext The context to resolve token names to ids
   * @return The histogram-key
   */
  def getHistogramKey(
    histogram: Histogram,
    tokenContext: ReadTokenContext
  ): Option[HistogramKey] = {
    val maybeLabelOrTypeId =
      histogram.nodeOrRelationship match {
        case NODE_TYPE         => tokenContext.getOptLabelId(histogram.labelOrTypeName).map(LabelId.apply)
        case RELATIONSHIP_TYPE => tokenContext.getOptRelTypeId(histogram.labelOrTypeName).map(RelTypeId.apply)
      }

    val maybePropertyKeyId = tokenContext.getOptPropertyKeyId(histogram.property).map(PropertyKeyId.apply)

    for {
      labelOrTypeId <- maybeLabelOrTypeId
      propertyKeyId <- maybePropertyKeyId
    } yield HistogramKey(histogram.nodeOrRelationship, labelOrTypeId, propertyKeyId)
  }

  def getHistogramsFromConfig(bucketsFromConfig: java.util.Set[java.util.Map[String, String]]): Set[Histogram] = {

    case class BucketFromConfig(
      entityType: EntityType,
      labelOrRel: String,
      property: String,
      min: Double,
      max: Double,
      selectivity: Double
    )

    case class BucketFromConfigBuilder(
      labelOrType: String = "",
      property: String = "",
      minIncl: Double = 0.0,
      maxExcl: Double = 0.0,
      selectivity: Double = -1.0,
      entityType: EntityType = NODE_TYPE
    ) {

      def withLabelOrType(value: String): BucketFromConfigBuilder = {
        copy(labelOrType = value)
      }
      def withProperty(value: String): BucketFromConfigBuilder = {
        copy(property = value)
      }
      def withMinIncl(value: Double): BucketFromConfigBuilder = {
        copy(minIncl = value)
      }
      def withMaxExcl(value: Double): BucketFromConfigBuilder = {
        copy(maxExcl = value)
      }
      def withSelectivity(value: Double): BucketFromConfigBuilder = {
        copy(selectivity = value)
      }
      def withEntityType(value: EntityType): BucketFromConfigBuilder = {
        copy(entityType = value)
      }

      private val stringRegex = "^([a-zA-Z0-9_]+)$".r

      private def setLabelOrType(value: String): BucketFromConfigBuilder = {
        value match {
          case stringRegex(l) => withLabelOrType(l)
          case _ => throw InvalidArgumentException.invalidValueInHistogramFromConfig(
              "labelOrType",
              value,
              java.util.List.of("ALPHANUMERIC STRING")
            )
        }
      }
      private def setProperty(value: String): BucketFromConfigBuilder = {
        value match {
          case stringRegex(p) => withProperty(p)
          case _ =>
            throw InvalidArgumentException.invalidValueInHistogramFromConfig(
              "property",
              value,
              java.util.List.of("ALPHANUMERIC STRING")
            )
        }
      }
      private def setMin(value: String): BucketFromConfigBuilder = {
        value.toDoubleOption match {
          case Some(d) => withMinIncl(d)
          case None =>
            throw InvalidArgumentException.invalidValueInHistogramFromConfig("min", value, java.util.List.of("DOUBLE"))
        }
      }
      private def setMax(value: String): BucketFromConfigBuilder = {
        value.toDoubleOption match {
          case Some(d) => withMaxExcl(d)
          case None =>
            throw InvalidArgumentException.invalidValueInHistogramFromConfig("max", value, java.util.List.of("DOUBLE"))
        }
      }
      private def setSelectivity(value: String): BucketFromConfigBuilder = {
        value.toDoubleOption match {
          case Some(d) if d >= 0.0 && d <= 1.0 => withSelectivity(d)
          case _ => throw InvalidArgumentException.invalidValueInHistogramFromConfig(
              "selectivity",
              value,
              java.util.List.of("DOUBLE between 0.0 and 1.0")
            )
        }
      }

      def add(key: String, value: String): BucketFromConfigBuilder = {
        (key, value) match {
          case ("labelOrType", value)         => setLabelOrType(value)
          case ("property", value)            => setProperty(value)
          case ("min", value)                 => setMin(value)
          case ("max", value)                 => setMax(value)
          case ("selectivity", value)         => setSelectivity(value)
          case ("entityType", "node")         => withEntityType(NODE_TYPE)
          case ("entityType", "relationship") => withEntityType(RELATIONSHIP_TYPE)
          case ("entityType", value) =>
            throw InvalidArgumentException.invalidValueInHistogramFromConfig(
              "entityType",
              value,
              java.util.List.of("node", "relationship")
            )
          case _ => throw new InternalError("Unexpected key-value pair in histogram builder from config")
        }
      }
      def build(): BucketFromConfig = {
        if (labelOrType == "") {
          throw InvalidArgumentException.invalidValueInHistogramFromConfig(
            "labelOrType",
            "",
            java.util.List.of("non-empty STRING")
          )
        }
        if (property == "") {
          throw InvalidArgumentException.invalidValueInHistogramFromConfig(
            "property",
            "",
            java.util.List.of("non-empty STRING")
          )
        }
        if (selectivity == -1.0) {
          throw InvalidArgumentException.invalidValueInHistogramFromConfig(
            "selectivity",
            "",
            java.util.List.of("FLOAT between 0.0 and 1.0")
          )
        }
        if (minIncl >= maxExcl) {
          throw InvalidArgumentException.invalidValueInHistogramFromConfig(
            "max",
            maxExcl.toString,
            java.util.List.of(s"value larger than $minIncl")
          )
        }

        BucketFromConfig(entityType, labelOrType, property, minIncl, maxExcl, selectivity)
      }
    }

    val buckets = bucketsFromConfig.asScala.map { bucket =>
      bucket.asScala
        .foldLeft(BucketFromConfigBuilder()) {
          (builder, bucket) => builder.add(bucket._1, bucket._2)
        }
        .build()
    }

    // Group by isNodeEntityType, labelOrType and property
    // For each group: transform the buckets into a set of StandardBuckets and create the histogram object
    buckets.groupBy { configBucket =>
      (configBucket.entityType, configBucket.labelOrRel, configBucket.property)
    }.map {
      case ((entityType, labelOrRel, property), buckets) =>
        val standardBuckets = buckets.map[Bucket](b => StandardBucket(b.min, b.max, b.selectivity)).toSet
        Histogram(entityType, labelOrRel, property, standardBuckets)
    }.toSet
  }
}
