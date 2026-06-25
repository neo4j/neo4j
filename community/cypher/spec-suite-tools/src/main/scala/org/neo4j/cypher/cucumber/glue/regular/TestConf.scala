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

import java.util.Locale
import java.util.concurrent.ThreadLocalRandom

import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * Cypher Cucumber test configuration.
 *
 * @param neo4jConf base neo4j settings, individual scenarios can add to this
 * @param useBolt true if queries should go though driver
 * @param useEnterprise true if enterprise edition should be used
 * @param useSpd true if spd should be used
 * @param preparserOptions pre-parser options
 * @param tagContext base for '@fails:...' and '@ignore:...' tags. Examples: 'bolt', 'parallel-runtime', 'community', 'db-format-multiversion'.
 */
case class TestConf(
  neo4jConf: Settings,
  useBolt: Boolean,
  useEnterprise: Boolean,
  useSpd: Boolean,
  useGraphEngine: Boolean,
  readOnlyUser: Boolean,
  preparserOptions: Map[String, String],
  private val tagContext: Set[String],
  serverLogsConfResource: Option[String],
  maxDbmsReuse: Option[Int]
) {
  val preparserPrefix: String = TestConf.preParserPrefix(preparserOptions)

  /** Scenarios with these tags are expected to fail. */
  val expectFailureTags: Set[String] = tagContext.map(name => Tag.FailsPrefix + name) + Tag.FailsAll

  /** Scenarios with these tags are not run. */
  val ignoreTags: Set[String] = tagContext.map(name => Tag.IgnorePrefix + name) + Tag.IgnoreAll

  def runtime: String = preparserOptions.getOrElse("runtime", "")
}

object TestConf {
  type Settings = Map[String, String]

  def apply(
    neo4jConf: Settings = Map.empty,
    useBolt: Boolean = false,
    useEnterprise: Boolean = true,
    useSpd: Boolean = false,
    readOnlyUser: Boolean = false,
    preparserOptions: Map[String, String] = Map.empty,
    additionalTagContext: Set[String] = Set.empty,
    serverLogsConfResource: Option[String] = None,
    maxDbmsReuse: Option[Int] = None
  ): TestConf = {
    val fullNeo4jConf = Seq(
      Some("server.memory.query_cache.per_db_cache_num_entries" -> "64"),
      Some("internal.cypher.enable_non_fused_merge" -> "true"),
      Some("internal.dbms.debug.track_cursor_close" -> "true"),
      Some("internal.dbms.debug.track_tx_statement_close" -> "true"),
      Some("dbms.security.auth_enabled" -> "true"),
      Option.when(useEnterprise)("server.metrics.enabled" -> "false"),
      Option.when(useBolt)("server.bolt.enabled" -> "true"),
      // This setting is overridden in Executors (but provided here for visibility in test failures)
      serverLogsConfResource.map(resource => "server.logs.config" -> s"CLASSPATH_RESOURCE/$resource")
    ).flatten.toMap ++ neo4jConf

    // Make these tests work with FormatOverrideMigrator.
    val configWithOverrides = Config.newBuilder()
      .setRaw((fullNeo4jConf + ("server.config.strict_validation.enabled" -> "false")).asJava)
      .build()
    val cypherVersionTag = configWithOverrides.get(GraphDatabaseSettings.default_language).name()
      .toLowerCase(Locale.ROOT)
      .replace("_", "-")
    val useVirtualGraph =
      if (configWithOverrides.getDeclaredSettings.containsKey("internal.virtual_graph.enabled")) {
        val setting = configWithOverrides.getSetting("internal.virtual_graph.enabled")
        java.lang.Boolean.TRUE == configWithOverrides.get(setting)
      } else {
        false
      }
    val tagContext = additionalTagContext +
      (if (useEnterprise) "enterprise" else "community") +
      cypherVersionTag ++
      Option.when(useSpd)("spd") ++
      Option.when(useBolt)("bolt") ++
      preparserOptions.get("runtime").map(runtime => s"$runtime-runtime") ++
      preparserOptions.get("operatorEngine").map(operatorEngine => s"$operatorEngine-operatorEngine") +
      s"db-format-${configWithOverrides.get(GraphDatabaseSettings.db_format)}" +
      (if (useVirtualGraph) "graph-engine" else "graph-engine-disabled")

    new TestConf(
      neo4jConf = fullNeo4jConf,
      useBolt = useBolt,
      useEnterprise = useEnterprise,
      useSpd = useSpd,
      useGraphEngine = useVirtualGraph,
      readOnlyUser = readOnlyUser,
      preparserOptions = preparserOptions,
      tagContext = tagContext,
      serverLogsConfResource = serverLogsConfResource,
      maxDbmsReuse = maxDbmsReuse
    )
  }

  def withCypher5(base: TestConf): TestConf = base.copy(
    tagContext = base.tagContext.incl("cypher-5")
  )

  def withCypher25(base: TestConf): TestConf = base.copy(
    neo4jConf = base.neo4jConf ++ Seq(
      "db.query.default_language" -> "cypher_25"
    ),
    tagContext = base.tagContext.incl("cypher-25")
  )

  def withReadOnlyUser(base: TestConf): TestConf = base.copy(
    readOnlyUser = true,
    tagContext = base.tagContext.incl("read-only-user")
  )

  object Default {
    private def baseConf: TestConf = TestConf()

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Default$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Default$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object DefaultBolt {

    private def baseConf: TestConf = TestConf(
      neo4jConf = Map("server.bolt.enabled" -> "true"),
      useBolt = true
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$DefaultBolt$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$DefaultBolt$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object Pipelined {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "pipelined")
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Pipelined$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Pipelined$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object PipelinedNonFused {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "pipelined", "operatorEngine" -> "interpreted")
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$PipelinedNonFused$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$PipelinedNonFused$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object PipelinedRandMorselSize {

    private def baseConf: TestConf = {
      val morselSize = ThreadLocalRandom.current().nextInt(8) + 1
      TestConf(
        neo4jConf = Map(
          "internal.cypher.pipelined.batch_size_small" -> morselSize.toString,
          "internal.cypher.pipelined.batch_size_big" -> morselSize.toString
        ),
        preparserOptions = Map("runtime" -> "pipelined", "operatorEngine" -> "interpreted")
      )
    }

    object Cypher25 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$PipelinedRandMorselSize$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$PipelinedRandMorselSize$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object PipelinedFallback {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map(
        "runtime" -> "pipelined",
        "interpretedPipesFallback" -> "all"
      ),
      additionalTagContext = Set("pipelined-fallback")
    )

    object Cypher25 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$PipelinedFallback$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$PipelinedFallback$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object Slotted {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "slotted")
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Slotted$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Slotted$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object SlottedBolt {

    private def baseConf: TestConf = TestConf(
      useBolt = true,
      preparserOptions = Map("runtime" -> "slotted")
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$SlottedBolt$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$SlottedBolt$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object SlottedCompiled {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map(
        "runtime" -> "slotted",
        "expressionEngine" -> "compiled"
      )
    )

    object Cypher25 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$SlottedCompiled$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$SlottedCompiled$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object Parallel {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "parallel")
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Parallel$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Parallel$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object ParallelNonFused {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "parallel", "operatorEngine" -> "interpreted")
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelNonFused$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelNonFused$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object ParallelLeverageOrder {

    private def baseConf: TestConf = TestConf(
      preparserOptions = Map("runtime" -> "parallel", "parallelRuntimeConfig" -> "leverageOrder")
    )

    object Cypher25 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelLeverageOrder$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelLeverageOrder$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object ParallelLeverageOrderNonFused {

    private def baseConf: TestConf = TestConf(
      preparserOptions =
        Map("runtime" -> "parallel", "parallelRuntimeConfig" -> "leverageOrder", "operatorEngine" -> "interpreted")
    )

    object Cypher25 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelLeverageOrderNonFused$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelLeverageOrderNonFused$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object ParallelBolt {

    private def baseConf: TestConf = TestConf(
      neo4jConf = Map("server.bolt.enabled" -> "true"),
      useBolt = true,
      preparserOptions = Map("runtime" -> "parallel")
    )

    object Cypher25 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelBolt$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$ParallelBolt$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  private val spdConf: Map[String, String] = Map(
    "db.query.default_language" -> "cypher_25",
    // For unknown reasons multiversion store format override (NEO4J_OVERRIDE_STORE_FORMAT) fails here
    "db.format" -> "block",
    "server.bolt.enabled" -> "true",
    "server.routing.listen_address" -> "127.0.0.1:0",
    "server.routing.advertised_address" -> "127.0.0.1:0",
    "server.cluster.listen_address" -> "127.0.0.1:0",
    "server.cluster.raft.listen_address" -> "127.0.0.1:0",
    "server.cluster.advertised_address" -> "127.0.0.1:0",
    "server.cluster.raft.advertised_address" -> "127.0.0.1:0",
    "internal.dbms.single_raft_enabled" -> "true",
    "internal.dbms.replication_enabled" -> "true",
    "internal.initial.dbms.default_database.enable" -> "false",
    "internal.dbms.sharded_property_database.enabled" -> "true"
  )

  object SpdBolt extends InjectedTestConf {
    final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$SpdBolt$ObjectFactory"

    final override val conf: TestConf = TestConf.withCypher25(TestConf(
      neo4jConf = spdConf,
      useBolt = true,
      useSpd = true,
      maxDbmsReuse = Some(256) // Workaround: We have seen OOMs because SPD uses too much ephemeral disk space.
    ))
    final class ObjectFactory extends SingletonInjector(injector)
  }

  object SpdParallel extends InjectedTestConf {
    final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$SpdParallel$ObjectFactory"

    final override val conf: TestConf = TestConf.withCypher25(TestConf(
      neo4jConf = spdConf,
      preparserOptions = Map("runtime" -> "parallel"),
      useSpd = true,
      useBolt = true,
      maxDbmsReuse = Some(256) // Workaround: We have seen OOMs because SPD uses too much ephemeral disk space.
    ))
    final class ObjectFactory extends SingletonInjector(injector)
  }

  object CommunityDefaultBolt {

    private def baseConf: TestConf = TestConf(
      // Avoid multiversion store format override (NEO4J_OVERRIDE_STORE_FORMAT) in community
      neo4jConf = Map("db.format" -> "aligned"),
      useEnterprise = false,
      useBolt = true
    )

    object Cypher25 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$CommunityDefaultBolt$Cypher25$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher25(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$CommunityDefaultBolt$Cypher5$ObjectFactory"
      final override val conf: TestConf = TestConf.withCypher5(baseConf)
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object Legacy extends InjectedTestConf {
    final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Legacy$ObjectFactory"

    final override val conf: TestConf = TestConf.withCypher5(TestConf(
      // Avoid multiversion store format override (NEO4J_OVERRIDE_STORE_FORMAT) in community
      neo4jConf = Map("db.format" -> "aligned"),
      useEnterprise = false,
      preparserOptions = Map("runtime" -> "legacy")
    ))
    final class ObjectFactory extends SingletonInjector(injector)
  }

  object Planner {

    object InferLabels extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Planner$InferLabels$ObjectFactory"

      final override val conf: TestConf = TestConf.withCypher25(TestConf(
        neo4jConf = Map("dbms.cypher.infer_schema_parts" -> "MOST_SELECTIVE_LABEL")
      ))
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object SmallIdpTableSize extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$Planner$SmallIdpTableSize$ObjectFactory"

      final override val conf: TestConf = {
        val idpTableSize = ThreadLocalRandom.current().nextInt(16, 128)
        TestConf.withCypher25(TestConf(
          neo4jConf = Map("internal.cypher.idp_solver_table_threshold" -> idpTableSize.toString)
        ))
      }
      final class ObjectFactory extends SingletonInjector(injector)
    }

    object UpdateStrategyEager extends InjectedTestConf {

      final val FactoryName =
        "org.neo4j.cypher.cucumber.glue.regular.TestConf$Planner$UpdateStrategyEager$ObjectFactory"

      final override val conf: TestConf = TestConf.withCypher25(TestConf(
        preparserOptions = Map("updateStrategy" -> "eager")
      ))
      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  object PlannerVersion {

    object Experimental {

      private def baseConf: TestConf = TestConf(
        preparserOptions = Map("plannerVersion" -> "experimental")
      )

      object Cypher25 extends InjectedTestConf {

        final val FactoryName =
          "org.neo4j.cypher.cucumber.glue.regular.TestConf$PlannerVersion$Experimental$Cypher25$ObjectFactory"
        final override val conf: TestConf = TestConf.withCypher25(baseConf)
        final class ObjectFactory extends SingletonInjector(injector)
      }

      object Cypher5 extends InjectedTestConf {

        final val FactoryName =
          "org.neo4j.cypher.cucumber.glue.regular.TestConf$PlannerVersion$Experimental$Cypher5$ObjectFactory"
        final override val conf: TestConf = TestConf.withCypher5(baseConf)
        final class ObjectFactory extends SingletonInjector(injector)
      }
    }
  }

  object ReadOnlyUser {

    private def baseConf: TestConf = TestConf(
      neo4jConf = Map("server.bolt.enabled" -> "true"),
      useBolt = true
    )

    object Cypher25 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$ReadOnlyUser$Cypher25$ObjectFactory"
      final override val conf: TestConf = withReadOnlyUser(TestConf.withCypher25(baseConf))

      final class ObjectFactory extends SingletonInjector(injector)
    }

    object Cypher5 extends InjectedTestConf {
      final val FactoryName = "org.neo4j.cypher.cucumber.glue.regular.TestConf$ReadOnlyUser$Cypher5$ObjectFactory"
      final override val conf: TestConf = withReadOnlyUser(TestConf.withCypher5(baseConf))

      final class ObjectFactory extends SingletonInjector(injector)
    }
  }

  private def preParserPrefix(options: Map[String, String]): String = {
    if (options.isEmpty) ""
    else options
      .map { case (k, v) => s"$k=$v" }
      .mkString("CYPHER ", " ", "\n")
  }
}
