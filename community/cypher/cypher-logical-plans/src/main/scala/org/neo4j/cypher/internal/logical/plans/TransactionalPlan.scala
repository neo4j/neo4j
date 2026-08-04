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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Shared supertype of the two `CALL { ... } IN TRANSACTIONS` plans.
 */
trait TransactionalPlan {
  def onErrorBehaviour: TransactionalPlan.ErrorHandling
}

object TransactionalPlan {

  /**
   * Normalized error handling for `CALL { ... } IN TRANSACTIONS`, split into two independent axes:
   * what to do with the failing batch ([[RecoveryMode]]) and whether to retry it first ([[RetryMode]]).
   *
   * The conversion to/from the surface [[InTransactionsOnErrorBehaviour]] lives on
   * [[TransactionalPlan]], the shared companion of the plans that carry this.
   */
  case class ErrorHandling(recovery: RecoveryMode, retry: RetryMode) {
    def failWithoutRetries: Boolean = recovery == RecoveryMode.Fail && !shouldRetry
    def shouldRetry: Boolean = retry != RetryMode.NoRetry
  }

  object ErrorHandling {

    def fromAst(
      behaviour: InTransactionsOnErrorBehaviour,
      retryParameters: Option[InTransactionsRetryParameters],
      defaultRetryMode: RetryMode = RetryMode.NoRetry
    ): ErrorHandling = {
      val timeout = retryParameters.flatMap(_.timeout)
      behaviour match {
        case OnErrorContinue          => ErrorHandling(RecoveryMode.Continue, defaultRetryMode)
        case OnErrorBreak             => ErrorHandling(RecoveryMode.Break, defaultRetryMode)
        case OnErrorFail              => ErrorHandling(RecoveryMode.Fail, defaultRetryMode)
        case OnErrorRetryThenContinue => ErrorHandling(RecoveryMode.Continue, RetryMode.Explicit(timeout))
        case OnErrorRetryThenBreak    => ErrorHandling(RecoveryMode.Break, RetryMode.Explicit(timeout))
        case OnErrorRetryThenFail     => ErrorHandling(RecoveryMode.Fail, RetryMode.Explicit(timeout))
      }
    }

    def toAst(behaviour: ErrorHandling)
      : (InTransactionsOnErrorBehaviour, Option[InTransactionsRetryParameters]) = {
      def retryParams(timeout: Option[Expression]): Option[InTransactionsRetryParameters] =
        Some(InTransactionsRetryParameters(timeout)(InputPosition.NONE))

      behaviour match {
        case ErrorHandling(RecoveryMode.Continue, RetryMode.NoRetry) => (OnErrorContinue, None)
        case ErrorHandling(RecoveryMode.Break, RetryMode.NoRetry)    => (OnErrorBreak, None)
        case ErrorHandling(RecoveryMode.Fail, RetryMode.NoRetry)     => (OnErrorFail, None)
        case ErrorHandling(RecoveryMode.Continue, RetryMode.Explicit(t)) =>
          (OnErrorRetryThenContinue, retryParams(t))
        case ErrorHandling(RecoveryMode.Break, RetryMode.Explicit(t)) =>
          (OnErrorRetryThenBreak, retryParams(t))
        case ErrorHandling(RecoveryMode.Fail, RetryMode.Explicit(t)) =>
          (OnErrorRetryThenFail, retryParams(t))

        // implicit MVCC retry is invisible
        case ErrorHandling(RecoveryMode.Continue, RetryMode.ImplicitOnMvcc) => (OnErrorContinue, None)
        case ErrorHandling(RecoveryMode.Break, RetryMode.ImplicitOnMvcc)    => (OnErrorBreak, None)
        case ErrorHandling(RecoveryMode.Fail, RetryMode.ImplicitOnMvcc)     => (OnErrorFail, None)
      }
    }
  }

  enum RecoveryMode {
    case Continue, Break, Fail
  }

  enum RetryMode {
    case NoRetry
    case Explicit(timeout: Option[Expression])

    /**
     * Implicit retry injected on multi-versioned stores when no explicit `ON ERROR RETRY` was given.
     * Retries a fixed number of attempts, mirroring the whole-query retries of `MultiVersionExecutionEngine`.
     * Deliberately invisible in plan descriptions (see [[org.neo4j.cypher.internal.logical.plans]] rendering).
     */
    case ImplicitOnMvcc
  }
}
