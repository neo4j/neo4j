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
package org.neo4j.cypher.internal.procs

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings.seed_with_metadata_timeout
import org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.optionsmap.CreateDatabaseOptions
import org.neo4j.cypher.internal.optionsmap.CreateDatabaseOptionsConverter
import org.neo4j.cypher.internal.optionsmap.ExistingMetadataOption
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.dbms.archive.backup.BackupMetadataScriptProvider
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.QueryExecutionTimeoutException
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.logging.InternalLog
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import scala.jdk.OptionConverters.RichOption
import scala.language.reflectiveCalls
import scala.math.min

/**
 * This plan fetches the backup metadata script from a provided seedURI and executes the lines within as sub-queries.
 * In order to execute the DDL commands from the metadata, it needs access to an executionEngine that understands them.
 *
 * Intended to simplify migrating a database and its corresponding privileges from one DBMS to another.
 */
case class ApplyMetadataScriptIfSuppliedPlan(
  cypherVersion: CypherVersion,
  outerExecutionEngine: ExecutionEngine,
  options: Options,
  config: Config,
  source: ExecutionPlan,
  dbName: Either[String, Parameter],
  backupMetadataScriptProvider: BackupMetadataScriptProvider
) extends AdministrationChainedExecutionPlan(Some(source)) {

  override def runSpecific(
    ctx: SystemUpdateCountingQueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult = {
    // Since we don't have access to the options before runtime, we always plan an ApplyMetadataScriptIfSuppliedPlan,
    // then decide at this point if it should do anything or be a no-op
    val ops = CreateDatabaseOptionsConverter.convert(cypherVersion, options, params, Some(config))
    if (ops.flatMap(_.existingMetadata).contains(ExistingMetadataOption.VALID_VALUE)) {
      applyMetadataScript(ops.get, ctx, params, subscriber, previousNotifications, executionMode, prePopulateResults)
    } else {
      UpdatingSystemCommandRuntimeResult(
        ctx,
        previousNotifications ++ notifications
      )
    }
  }

  private def applyMetadataScript(
    ops: CreateDatabaseOptions,
    ctx: SystemUpdateCountingQueryContext,
    params: MapValue,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification],
    executionMode: ExecutionMode,
    prePopulateResults: Boolean
  ): UpdatingSystemCommandRuntimeResult = {
    lazy val debugLog: InternalLog = ctx.logProvider.getLog(getClass)

    val seedURI =
      ops.seedURI.getOrElse(
        throw new InvalidArgumentException("Missing input for OPTIONS. Missing option(s) `seedUri`.")
      )
    val credentials = ops.seedCredentials.toJava
    val seedConfig = ops.seedConfig.toJava

    val seedWithMetadataTimeout = config.get(seed_with_metadata_timeout).get(ChronoUnit.SECONDS)
    val transactionTimeout = config.get(transaction_timeout).get(ChronoUnit.SECONDS)
    val timeout =
      if (transactionTimeout == 0) seedWithMetadataTimeout else min(seedWithMetadataTimeout, transactionTimeout)
    val future = backupMetadataScriptProvider.backupMetadataScript(seedURI, credentials, seedConfig)
    if (future == null) {
      throw new CypherExecutionException("Metadata script not found or too large.")
    }
    val metadata =
      try {
        future.get(
          timeout,
          TimeUnit.SECONDS
        )
      } catch {
        case _: TimeoutException =>
          future.cancel(true)
          throw new QueryExecutionTimeoutException(
            s"Timed out. Fetching metadata script took longer than ${transaction_timeout.name()} or ${seed_with_metadata_timeout.name()}."
          )
      }

    val queries = prepareMetadataScript(metadata)

    val innerParams = {
      dbName match {
        case Left(literal) => MapValue.EMPTY.updatedWith("database", Values.utf8Value(literal))
        case Right(param)  => MapValue.EMPTY.updatedWith("database", params.get(param.name))
      }
    }

    val innerSubscriber = new QuerySubscriberAdapter() {
      var error: Option[Throwable] = None
      override def onResultCompleted(statistics: QueryStatistics): Unit = {
        ctx.systemUpdates.increase(statistics.getSystemUpdates)
      }

      override def onError(throwable: Throwable): Unit = {
        error = Some(throwable)
      }
    }

    val tc = ctx.kernelTransactionalContext
    queries.foreach { query =>
      val execution = outerExecutionEngine.executeSubquery(
        query,
        innerParams,
        tc,
        isOutermostQuery = false,
        executionMode == ProfileMode,
        prePopulateResults,
        innerSubscriber
      ).asInstanceOf[InternalExecutionResult]
      try {
        execution.consumeAll()
      } catch {
        case _: Throwable =>
        // do nothing, exceptions are handled by innerSubscriber
      }
      innerSubscriber.error.foreach(throwable => {
        debugLog.debug("Execution of metadata script during CREATE DATABASE threw exception", throwable)
        throw throwable
      })
    }

    UpdatingSystemCommandRuntimeResult(
      ctx,
      // The notifications from the inner queries are currently not appended here.
      // The inner query returns org.neo4j.graphdb.Notification (public API) whereas the RuntimeResult expects InternalNotification.
      previousNotifications ++ notifications
    )

  }

  private def prepareMetadataScript(metadata: String): Array[String] = {
    val (createDb, statements) = metadata
      .split(";")
      .map(_.strip())
      .partition(_.toUpperCase.contains("CREATE DATABASE"))
    if (createDb.length != 1) {
      throw new InvalidArgumentException("Metadata script did not contain exactly one `CREATE DATABASE` statement.")
    }
    if (!createDb.head.toUpperCase.contains("CREATE DATABASE $DATABASE")) {
      throw new InvalidArgumentException(
        "Metadata script contains `CREATE DATABASE` with unexpected parameter name. Expected `$database`."
      )
    }
    statements
  }

}
