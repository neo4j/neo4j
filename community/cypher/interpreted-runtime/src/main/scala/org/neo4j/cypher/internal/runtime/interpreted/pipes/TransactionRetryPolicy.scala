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

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.logical.plans.TransactionalPlan.ErrorHandling
import org.neo4j.cypher.internal.logical.plans.TransactionalPlan.RetryMode
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression

enum TransactionRetryPolicy {
  case DoNotRetry
  case ImplicitMultiVersionRetry
  case RetryFor(maybeDurationInSeconds: Option[Expression])

  /** Whether the failing batch is retried at all before the [[org.neo4j.cypher.internal.logical.plans.TransactionalPlan.RecoveryMode]] is applied. */
  def retryable: Boolean = this match {
    case DoNotRetry                              => false
    case RetryFor(_) | ImplicitMultiVersionRetry => true
  }

  /** Whether retries were requested explicitly (`ON ERROR RETRY`), as opposed to injected implicitly on MVCC. */
  def isExplicit: Boolean = this match {
    case RetryFor(_)                            => true
    case DoNotRetry | ImplicitMultiVersionRetry => false
  }
}

object TransactionRetryPolicy {

  /**
   * Bridge the normalized logical-plan [[ErrorHandling]] to the interpreted/slotted runtime's
   * ([[ErrorMode]], retry policy) pair. The mode drives which `TransactionPipeWrapper` is used; the policy
   * drives the retry logic. [[RetryMode.ImplicitOnMvcc]] maps to [[ImplicitMultiVersionRetry]].
   */
  def forRuntime(
    behaviour: ErrorHandling,
    expressionConverter: internal.expressions.Expression => commands.expressions.Expression
  ): TransactionRetryPolicy =
    behaviour.retry match {
      case RetryMode.NoRetry           => DoNotRetry
      case RetryMode.Explicit(timeout) => RetryFor(timeout.map(expressionConverter))
      case RetryMode.ImplicitOnMvcc    => ImplicitMultiVersionRetry
    }
}
