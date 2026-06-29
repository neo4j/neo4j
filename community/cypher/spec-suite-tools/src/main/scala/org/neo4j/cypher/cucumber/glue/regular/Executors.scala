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

import com.google.inject.Inject
import com.google.inject.Provider
import io.cucumber.scala.Scenario
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertTrue
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.cucumber.CypherCucumber.Tag.ConfPrefix
import org.neo4j.cypher.cucumber.glue.regular.TestConf.Settings
import org.neo4j.cypher.cucumber.util.KernelOperation
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.cypher.testing.impl.driver.DriverCypherExecutorFactory
import org.neo4j.cypher.testing.impl.embedded.EmbeddedCypherExecutorFactory
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.driver.AuthTokens
import org.neo4j.graphdb.Result
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.util.Preconditions.checkState

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try
import scala.util.Using

trait Executors {

  /** Acquire a query executor for the specified scenario. The acquired [[DbAccessor]]s must be released. */
  def acquire(scenario: Scenario, dynamicSettings: Settings): DbAccessor

  /** Release an acquired [[DbAccessor]]. */
  def release(dbms: DbAccessor): Unit

  /** Start the [[Executors]] service. */
  def start(): Unit

  /** Shutdown the [[Executors]] service. */
  def shutdown(): Unit
}

case class DbAccessor(dbms: FeatureDatabaseManagementService, extraSettings: Settings, reUseCount: Int) {
  def isCompatible(extraSettings: Settings): Boolean = this.extraSettings == extraSettings
}

trait ExecutorPool extends Executors {
  private[this] val executors = new ArrayBlockingQueue[Option[DbAccessor]](ExecutorPool.PoolSize)
  @volatile private[this] var started = false

  def conf: TestConf

  final override def acquire(scenario: Scenario, dynamicSettings: Settings): DbAccessor = {
    checkState(started, "ExecutorPool is not started")

    val extraSettings = extraSettingsFor(scenario) ++ dynamicSettings
    executors.poll(5, TimeUnit.MINUTES) match {
      case Some(executor) =>
        try {
          if (isCompatible(executor, extraSettings, scenario)) {
            DbAccessor(executor.dbms.withNewExecutor(), executor.extraSettings, executor.reUseCount + 1)
          } else {
            shutdownExecutor(executor, deleteFiles = true)
            createExecutor(extraSettings)
          }
        } catch {
          case t: Throwable =>
            Try(shutdownExecutor(executor, deleteFiles = false))
            executors.offer(None)
            throw t
        }
      case None =>
        try {
          createExecutor(extraSettings)
        } catch {
          case t: Throwable =>
            executors.offer(None)
            throw t
        }
      case null =>
        throw new IllegalStateException(s"Timed out while waiting for executor (not supposed to happen)")
    }
  }

  final override def release(executor: DbAccessor): Unit = {
    checkState(started, "ExecutorPool is not started")
    try {
      // We do cleanup on release to make sure the correct test fails if it stopped the db.
      assertTrue(executor.dbms.database.isAvailable, "Database is not available after test")

      executor.dbms.terminateAllTransactions() // Can we fail test if there are open transactions instead?

      if (!conf.useGraphEngine) {
        executor.dbms.dropIndexesAndConstraints()
        // executor.dbms.clearQueryCaches() We could clear cache, but why
        KernelOperation.detachDeleteAllNodes(executor.dbms.database)
      }

      executor.dbms.closeExecutor()
      executors.offer(Some(executor))
    } catch {
      case t: Throwable =>
        Try(shutdownExecutor(executor, deleteFiles = false))
        executors.offer(None)
        throw t
    }
  }

  private def isCompatible(accessor: DbAccessor, extraSettings: Settings, scenario: Scenario): Boolean = {
    val forceRestart = scenario.getSourceTagNames.contains("@force-restart")
    !forceRestart && accessor.isCompatible(extraSettings) && conf.maxDbmsReuse.forall(_ > accessor.reUseCount)
  }

  private def createExecutor(extraSettings: Settings): DbAccessor = {
    accessorFrom(startDbms(extraSettings), extraSettings, None)
  }

  override def start(): Unit = {
    checkState(!started, "Tried starting already started ExecutorPool")
    while (executors.offer(None)) {}
    started = true
  }

  override def shutdown(): Unit = this.synchronized {
    checkState(started, "Tried stopping already stopped ExecutorPool")
    started = false
    executors.forEach(_.foreach(a => Try(shutdownExecutor(a, deleteFiles = true))))
    executors.clear()
  }

  protected def startDbms(extraSettings: Settings): DatabaseManagementService = {
    var dbmsBuilder = if (conf.useEnterprise) {
      val cls = getClass.getClassLoader.loadClass("com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder")
      cls.getDeclaredConstructor().newInstance().asInstanceOf[TestDatabaseManagementServiceBuilder]
    } else {
      new TestDatabaseManagementServiceBuilder()
    }

    val neo4jConf = conf.neo4jConf ++ extraSettings
    val homePath = Path.of("target", "test data", UUID.randomUUID().toString)
    dbmsBuilder = dbmsBuilder.setDatabaseRootDirectory(homePath)

    conf.serverLogsConfResource match {
      case Some(serverLogsConfResource) =>
        val confFile = Files.createDirectories(homePath.resolve("conf/server-logs.xml"))
        Using.resource(getClass.getResourceAsStream(serverLogsConfResource))(Files.copy(_, confFile, REPLACE_EXISTING))

        // Note, we can't use `.impermanent()` here because that overrides log configuration.
        dbmsBuilder
          .setConfigRaw(neo4jConf.updated("server.logs.config", confFile.toAbsolutePath.toString).asJava)
          .build()
      case None =>
        dbmsBuilder
          .setConfigRaw(neo4jConf.asJava)
          .impermanent()
          .build()
    }
  }

  final private def accessorFrom(
    dbms: DatabaseManagementService,
    extraSettings: Settings,
    dbName: Option[String]
  ): DbAccessor = {
    setupSecurity(dbms)
    val neo4jConf = dbms.database(dbName.getOrElse("neo4j")).asInstanceOf[GraphDatabaseAPI]
      .getDependencyResolver
      .resolveDependency(classOf[Config])
    val authToken =
      if (conf.readOnlyUser && conf.useEnterprise) AuthTokens.basic("readonly", "readonly")
      else AuthTokens.basic("neo4j", "neo4j")
    val executorFactory =
      if (conf.useBolt)
        DriverCypherExecutorFactory(dbms, neo4jConf, Some(AuthTokens.basic("neo4j", "neo4j")), Some(authToken))
      else EmbeddedCypherExecutorFactory(dbms, neo4jConf)

    DbAccessor(
      dbms = FeatureDatabaseManagementService(dbms, executorFactory, dbName),
      extraSettings = extraSettings,
      reUseCount = 0
    )
  }

  private def setupSecurity(dbms: DatabaseManagementService): Unit = {
    Using.resource(dbms.database(SYSTEM_DATABASE_NAME).beginTx()) { tx =>
      // Close each Result so its statement is released before commit; otherwise the open
      // statements are flagged by track_tx_statement_close when the committed tx is closed.
      def exec(query: String): Unit = Using.resource(tx.execute(query))(_ => ())
      exec("ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED")
      if (conf.useEnterprise) {
        exec("CREATE USER readonly SET PASSWORD 'readonly' CHANGE NOT REQUIRED")
        exec("GRANT ROLE reader to readonly")
      }
      tx.commit()
    }
  }

  private def extraSettingsFor(scenario: Scenario): Settings = {
    val tags = scenario.getSourceTagNames
    if (!tags.isEmpty && tags.stream().anyMatch(tag => tag.startsWith(ConfPrefix))) {
      tags.asScala.view
        .collect {
          case tag if tag.startsWith(ConfPrefix) =>
            val equalsIndex = tag.indexOf('=')
            tag.substring(ConfPrefix.length, equalsIndex) -> tag.substring(equalsIndex + 1)
        }
        .toMap
    } else {
      Map.empty
    }
  }

  private def shutdownExecutor(accessor: DbAccessor, deleteFiles: Boolean): Unit = {
    Try(accessor.dbms.clearQueryCaches()) // The ANTLR parser keeps a static cache that survives dbms shutdowns
    val pathsToDelete =
      Option.when(deleteFiles)(Seq(GraphDatabaseSettings.neo4j_home, GraphDatabaseSettings.logs_directory))
        .getOrElse(Seq.empty)
        .flatMap(setting =>
          Try(accessor.dbms.database.getDependencyResolver.resolveDependency(classOf[Config]).get(setting)).toOption
        )

    accessor.dbms.shutdown()
    pathsToDelete.foreach(FileUtils.deleteDirectory)
  }
}

@com.google.inject.Singleton
final class DefaultExecutorPool @Inject() (override val conf: TestConf) extends ExecutorPool

@com.google.inject.Singleton
final class SpdExecutorPool @Inject() (override val conf: TestConf) extends ExecutorPool {

  override protected def startDbms(extraSettings: Settings): DatabaseManagementService = {
    val dbms = super.startDbms(extraSettings)
    val systemDb = dbms.database(SYSTEM_DATABASE_NAME)
    systemDb.executeTransactionally(
      s"CYPHER 25 CREATE DATABASE neo4j GRAPH SHARD { TOPOLOGY 1 PRIMARY 0 SECONDARIES } PROPERTY SHARDS { COUNT 3 TOPOLOGY 1 REPLICA}"
    )

    val spdAvailabilityQuery = "CALL internal.dbms.spd.available()"
    val emptyMap = java.util.Map.of[String, AnyRef]()

    // Wait until the SPD is available
    await()
      .atMost(120, TimeUnit.SECONDS)
      .pollDelay(1, TimeUnit.SECONDS)
      .pollInSameThread
      .untilAsserted { () =>
        assertThat(systemDb.executeTransactionally(spdAvailabilityQuery, emptyMap, (r: Result) => r.stream().toList))
          .containsExactly(java.util.Map.of("available", java.lang.Boolean.TRUE, "detail", "All started"))
      }

    dbms
  }
}

@com.google.inject.Singleton()
class ExecutorsProvider @com.google.inject.Inject() (conf: TestConf) extends Provider[Executors] {

  override def get(): Executors = {
    if (conf.useSpd) new SpdExecutorPool(conf)
    else new DefaultExecutorPool(conf)
  }
}

final class ExecutorsStartAndShutown @Inject() (executors: Executors) extends BeforeAndAfterAll {
  override def beforeAll(): Unit = executors.start()
  override def afterAll(): Unit = executors.shutdown()
}

object ExecutorPool {
  val PoolSize = math.max(1, Runtime.getRuntime.availableProcessors())
}
