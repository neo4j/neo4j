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
package org.neo4j.cypher.cucumber.glue.regular

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.cucumber.CypherCucumber.Tag
import org.neo4j.cypher.cucumber.glue.regular.TestConf.Settings

import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * Cypher Cucumber test configuration.
 *
 * @param neo4jConf base neo4j settings, individual scenarios can add to this
 * @param useBolt true if queries should go though driver
 * @param useEnterprise true if enterprise edition should be used
 * @param useSpd true if spd should be used
 * @param preparserOptions pre-parser options
 * @param expectFailureTags scenarios with these tags are expected to fail
 * @param ignoreTags scenarios with these tags are expected to pass
 */
case class TestConf(
  neo4jConf: Settings,
  useBolt: Boolean,
  useEnterprise: Boolean,
  useSpd: Boolean,
  preparserOptions: Map[String, String],
  expectFailureTags: Set[String],
  ignoreTags: Set[String]
) {
  val preparserPrefix: String = TestConf.preParserPrefix(preparserOptions)
}

object TestConf {
  type Settings = Map[String, String]

  def apply(
    neo4jConf: Settings = Map.empty,
    useBolt: Boolean = false,
    useEnterprise: Boolean = true,
    useSpd: Boolean = false,
    preparserOptions: Map[String, String] = Map.empty,
    tagContext: Set[String]
  ): TestConf = {
    val fullNeo4jConf = Seq(
      Some("server.memory.query_cache.per_db_cache_num_entries" -> "128"),
      Option.when(useEnterprise)("server.metrics.enabled" -> "false"),
      Option.when(useBolt)("server.bolt.enabled" -> "true")
    ).flatten.toMap ++ neo4jConf

    // Allow for example @fails:db-format-multiversion and @ignore:db-format-multiversion
    // Note, NEO4J_OVERRIDE_STORE_FORMAT overrides this value in some testing (through FormatOverrideMigrator)
    val dbFormat = Config.newBuilder()
      .setRaw(fullNeo4jConf.view.filterKeys(_ == GraphDatabaseSettings.db_format.name()).toMap.asJava)
      .build()
      .get(GraphDatabaseSettings.db_format)
    val dbFormatFailsTag = s"${Tag.FailsPrefix}db-format-$dbFormat"
    val dbFormatIgnoreTag = s"${Tag.IgnorePrefix}db-format-$dbFormat"

    val failureTags = tagContext.map(name => Tag.FailsPrefix + name) + Tag.FailsAll + dbFormatFailsTag
    val ignoreTags = tagContext.map(name => Tag.IgnorePrefix + name) + Tag.IgnoreAll + dbFormatIgnoreTag
    new TestConf(fullNeo4jConf, useBolt, useEnterprise, useSpd, preparserOptions, failureTags, ignoreTags)
  }

  object Default extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Default$ObjectFactory"

    final override val conf: TestConf = TestConf(
      tagContext = Set("cypher-5")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object DefaultBolt extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$DefaultBolt$ObjectFactory"

    final override val conf: TestConf = TestConf(
      neo4jConf = Map("server.bolt.enabled" -> "true"),
      useBolt = true,
      tagContext = Set("cypher-5")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object Cypher25 extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Cypher25$ObjectFactory"

    final override val conf: TestConf = TestConf(
      neo4jConf = Map(
        "internal.db.query.default_language" -> "cypher_25",
        "internal.dbms.cypher.enable_experimental_versions" -> "true"
      ),
      tagContext = Set("cypher-25")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object Cypher25Bolt extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Cypher25Bolt$ObjectFactory"

    final override val conf: TestConf = TestConf(
      neo4jConf = Map(
        "server.bolt.enabled" -> "true",
        "internal.db.query.default_language" -> "cypher_25",
        "internal.dbms.cypher.enable_experimental_versions" -> "true"
      ),
      useBolt = true,
      tagContext = Set("cypher-25")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object Pipelined extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Pipelined$ObjectFactory"

    final override val conf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "pipelined"),
      tagContext = Set("cypher-5", "pipelined-runtime")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object PipelinedFallback extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$PipelinedFallback$ObjectFactory"

    final override val conf: TestConf = TestConf(
      preparserOptions = Map(
        "runtime" -> "pipelined",
        "interpretedPipesFallback" -> "all"
      ),
      tagContext = Set("cypher-5", "pipelined-runtime", "pipelined-fallback")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object Slotted extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Slotted$ObjectFactory"

    final override val conf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "slotted"),
      tagContext = Set("cypher-5")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object SlottedCompiled extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$SlottedCompiled$ObjectFactory"

    final override val conf: TestConf = TestConf(
      preparserOptions = Map(
        "runtime" -> "slotted",
        "expressionEngine" -> "compiled"
      ),
      tagContext = Set("cypher-5")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object Parallel extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Parallel$ObjectFactory"

    final override val conf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "parallel"),
      tagContext = Set("cypher-5", "parallel-runtime")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object ParallelBolt extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelBolt$ObjectFactory"

    final override val conf: TestConf = TestConf(
      neo4jConf = Map("server.bolt.enabled" -> "true"),
      useBolt = true,
      preparserOptions = Map("runtime" -> "parallel"),
      tagContext = Set("cypher-5", "parallel-runtime")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object SpdBolt extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$SpdBolt$ObjectFactory"

    final override val conf: TestConf = TestConf(
      neo4jConf = Map(
        // For unknown reasons multiversion store format override (NEO4J_OVERRIDE_STORE_FORMAT) fails here
        "db.format" -> "aligned",
        "server.bolt.enabled" -> "true",
        "server.routing.listen_address" -> "127.0.0.1:0",
        "server.routing.advertised_address" -> "127.0.0.1:0",
        "internal.dbms.sharded_property_database.shard_count" -> "3",
        "internal.dbms.sharded_property_database.read_only" -> "false"
      ),
      useBolt = true,
      useSpd = true,
      tagContext = Set("spd", "cypher-5")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object SpdParallel extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$SpdParallel$ObjectFactory"

    final override val conf: TestConf = TestConf(
      neo4jConf = Map(
        "server.bolt.enabled" -> "true",
        "server.routing.listen_address" -> "127.0.0.1:0",
        "server.routing.advertised_address" -> "127.0.0.1:0",
        "internal.dbms.sharded_property_database.shard_count" -> "3",
        "internal.dbms.sharded_property_database.read_only" -> "false"
      ),
      preparserOptions = Map("runtime" -> "parallel"),
      useSpd = true,
      useBolt = true,
      tagContext = Set("spd", "cypher-5", "parallel-runtime")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  object Legacy extends InjectedTestConf {
    final val ObjectFactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Legacy$ObjectFactory"

    final override val conf: TestConf = TestConf(
      // Avoid multiversion store format override (NEO4J_OVERRIDE_STORE_FORMAT) in community
      neo4jConf = Map("db.format" -> "aligned"),
      useEnterprise = false,
      preparserOptions = Map("runtime" -> "legacy"),
      tagContext = Set("cypher-5", "legacy-runtime")
    )
    final class ObjectFactory extends GuiceObjectFactory(injector)
  }

  private def preParserPrefix(options: Map[String, String]): String = {
    if (options.isEmpty) ""
    else options
      .map { case (k, v) => s"$k=$v" }
      .mkString("CYPHER ", " ", "\n")
  }
}
