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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExponentialBackoffRetryLogic.ExponentialBackoffRetryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionRetryLogic.RetryState

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit.SECONDS

import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS

/**
 * Cypher query inner transaction retry logic for CALL {} IN TRANSACTIONS ON ERROR RETRY.
 */
object TransactionRetryLogic {

  trait RetryState extends Comparable[RetryState] {
    def retryCount: Int
    def retryTimestamp: Long

    def computeNextRetryState(): RetryState
    def shouldRetryAgain(): Boolean
    def nanosUntilRetry(): Long

    def retryTimeout: Duration

    override def compareTo(other: RetryState): Int = {
      java.lang.Long.compare(retryTimestamp, other.retryTimestamp)
    }
  }
}

trait TransactionRetryLogic {
  def newRetryState(): RetryState
}

/**
 * A stateful retry logic that implements exponential backoff with jitter.
 */
case class ExponentialBackoffRetryLogic(
  maxRetryTimeNanos: Long = ExponentialBackoffRetryLogic.DEFAULT_MAX_RETRY_TIME_NANOS,
  initialRetryDelayNanos: Long = ExponentialBackoffRetryLogic.INITIAL_RETRY_DELAY_NANOS,
  multiplier: Double = ExponentialBackoffRetryLogic.RETRY_DELAY_MULTIPLIER,
  jitterFactor: Double = ExponentialBackoffRetryLogic.RETRY_DELAY_JITTER_FACTOR,
  maxRetryDelayNanos: Long = ExponentialBackoffRetryLogic.MAX_RETRY_DELAY_NANOS
) extends TransactionRetryLogic {

  require(maxRetryTimeNanos >= 0, s"maxRetryTimeNanos should be >= 0: $maxRetryTimeNanos")
  require(initialRetryDelayNanos >= 0, s"initialRetryDelayNanos should be >= 0: $initialRetryDelayNanos")
  require(multiplier >= 1.0, s"multiplier should be >= 1.0: $multiplier")
  require(jitterFactor >= 0 && jitterFactor < 1, s"jitterFactor must be in the range [0.0, 1.0]: $jitterFactor")
  require(maxRetryDelayNanos >= 0, s"maxRetryDelayNanos should be >= 0: $maxRetryDelayNanos")

  override def newRetryState(): ExponentialBackoffRetryState = {
    val initialDelay = (initialRetryDelayNanos.toDouble / multiplier).toLong
    ExponentialBackoffRetryState(this, 0, 0L, 0L, retryDelayNanos = initialDelay)
  }

  def multiply(delayNanos: Long): Long = {
    val delay = (delayNanos * multiplier).toLong
    Math.min(delay, maxRetryDelayNanos)
  }

  def jitter(delayNanos: Long): Long = {
    val jitter = (delayNanos * jitterFactor).toLong
    val min = delayNanos - jitter
    val max = delayNanos + jitter
    ThreadLocalRandom.current.nextLong(min, max + 1)
  }
}

object ExponentialBackoffRetryLogic {
  final val DEFAULT_MAX_RETRY_TIME_NANOS: Long = SECONDS.toNanos(30)
  final val INITIAL_RETRY_DELAY_NANOS = MILLISECONDS.toNanos(10)
  final val RETRY_DELAY_MULTIPLIER = 2.0
  final val RETRY_DELAY_JITTER_FACTOR = 0.2
  final val MAX_RETRY_DELAY_NANOS = Long.MaxValue / 2

  final case class ExponentialBackoffRetryState(
    config: ExponentialBackoffRetryLogic,
    retryCount: Int,
    firstRetryTimestamp: Long,
    retryTimestamp: Long,
    retryDelayNanos: Long
  ) extends RetryState {

    override def toString: String = {
      s"ExponentialBackoffRetryState(retryCount=${retryCount}, retryTimestamp=${retryTimestamp}, " +
        s"retryDelayNanos=${retryDelayNanos})"
    }

    override def computeNextRetryState(): RetryState = {
      val newRetryDelayNanos = config.multiply(retryDelayNanos)
      val t = System.nanoTime()
      ExponentialBackoffRetryState(
        config = config,
        retryCount = retryCount + 1,
        retryDelayNanos = newRetryDelayNanos,
        firstRetryTimestamp = if (retryCount == 0) t else firstRetryTimestamp,
        retryTimestamp = t + config.jitter(newRetryDelayNanos)
      )
    }

    override def shouldRetryAgain(): Boolean = {
      retryTimestamp - firstRetryTimestamp < config.maxRetryTimeNanos
    }

    override def nanosUntilRetry(): Long = {
      val ts = retryTimestamp
      val now = System.nanoTime()
      val nanosUntilRetry = ts - now
      if (nanosUntilRetry >= 0)
        nanosUntilRetry
      else
        0L
    }

    override def retryTimeout: Duration = Duration.fromNanos(config.maxRetryTimeNanos)
  }
}
