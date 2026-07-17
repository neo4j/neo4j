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
package org.neo4j.cypher.internal.runtime.debug.events

import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.internal.helpers.Exceptions

import java.util.concurrent.atomic.AtomicLong

import scala.compiletime.erasedValue

/**
 * Compile-time switches for runtime debug logging; completely removed in production builds
 */
object Debug {

  /**
   * Change this manually for a different logging format
   */
  private val logger: DebugLog = StdoutDebugLog.default

  /**
   * Toggle these on manually during development to enable log events for the given event category
   */
  inline def isEnabled[E <: DebugEvent]: Boolean =
    inline erasedValue[E] match {
      case _: DebugCategory.Asm                          => false
      case _: DebugCategory.BatchFormation               => false
      case _: DebugCategory.Buffers                      => false
      case _: DebugCategory.Cleanup                      => false
      case _: DebugCategory.ConcurrentTransactions       => false
      case _: DebugCategory.ConcurrentTransactionsWorker => false
      case _: DebugCategory.Cursors                      => false
      case _: DebugCategory.ErrorHandling                => false
      case _: DebugCategory.GeneratedIrCode              => false
      case _: DebugCategory.Locks                        => false
      case _: DebugCategory.MemoryTracking               => false
      case _: DebugCategory.MorselReuse                  => false
      case _: DebugCategory.PhysicalPlanning             => false
      case _: DebugCategory.Pipelines                    => false
      case _: DebugCategory.Progress                     => false
      case _: DebugCategory.Queries                      => false
      case _: DebugCategory.Scheduling                   => false
      case _: DebugCategory.Tracker                      => false
      case _: DebugCategory.TransactionalContext         => false
      case _: DebugCategory.Workers                      => false
      // if you enable this, enable DebugCategory.Workers too so it can be dynamically unmuted
      case _: DebugCategory.Workers.OnlyWhenStalled => false
      case _                                        => false
    }

  /** Perform code only when debug flag is enabled, otherwise emit nothing */
  inline def ifEnabled[E <: DebugEvent](inline block: Unit): Unit =
    inline if (isEnabled[E]) {
      block
    }

  /** Emit the first block if the category's debug flag is enabled, else the second block.

   *  {{{
   *  Debug.ifElse[Workers] {
   *    new DebugRunner(...)
   *  } {
   *    new Runner(...)
   *  }
   *  }}}
   */
  transparent inline def ifElse[E <: DebugEvent](inline ifEnabled: Any)(inline ifDisabled: Any) =
    inline if (isEnabled[E]) ifEnabled else ifDisabled

  /**
   * Constant bit identifying `E`'s category in the runtime mute mask.
   */
  private inline def categoryBit[E <: DebugEvent]: Long =
    inline erasedValue[E] match {
      case _: DebugCategory.Asm                          => 1L << 0
      case _: DebugCategory.BatchFormation               => 1L << 1
      case _: DebugCategory.Buffers                      => 1L << 2
      case _: DebugCategory.Cleanup                      => 1L << 3
      case _: DebugCategory.ConcurrentTransactions       => 1L << 4
      case _: DebugCategory.ConcurrentTransactionsWorker => 1L << 5
      case _: DebugCategory.Cursors                      => 1L << 6
      case _: DebugCategory.ErrorHandling                => 1L << 7
      case _: DebugCategory.GeneratedIrCode              => 1L << 8
      case _: DebugCategory.Locks                        => 1L << 9
      case _: DebugCategory.MemoryTracking               => 1L << 10
      case _: DebugCategory.MorselReuse                  => 1L << 11
      case _: DebugCategory.PhysicalPlanning             => 1L << 12
      case _: DebugCategory.Pipelines                    => 1L << 13
      case _: DebugCategory.Progress                     => 1L << 14
      case _: DebugCategory.Queries                      => 1L << 15
      case _: DebugCategory.Scheduling                   => 1L << 16
      case _: DebugCategory.Tracker                      => 1L << 17
      case _: DebugCategory.TransactionalContext         => 1L << 18
      case _: DebugCategory.Workers                      => 1L << 19
      case _                                             => 0L
    }

  /**
   * Runtime mute mask over the categories, consulted by [[log]] after the
   * compile-time [[isEnabled]] check. [[DebugCategory.Workers]] starts
   * muted when [[DebugCategory.Workers.OnlyWhenStalled]] is enabled —
   * the stall logic in `QueryCompletionTracker` unmutes it while a stall
   * is being investigated and mutes it again once progress resumes.
   */
  private val dynamicMuteMask: AtomicLong = new AtomicLong(
    if (isEnabled[DebugCategory.Workers.OnlyWhenStalled]) categoryBit[DebugCategory.Workers] else 0L
  )

  /** Suppress logging of category `E` until [[unmute]]. Categories start unmuted. */
  inline def mute[E <: DebugEvent](): Unit =
    ifElse[E] {
      dynamicMuteMask.getAndUpdate(m => m | categoryBit[E])
      ()
    } {
      if (AssertionRunner.ASSERTIONS_ENABLED) {
        throw new DebugException(s"Attempted to mute DebugEvent but it is not enabled at compile-time;" +
          s" update Debug.isEnabled")
      }
    }

  /** Resume logging of category `E` after a [[mute]]. */
  inline def unmute[E <: DebugEvent](): Unit =
    ifElse[E] {
      dynamicMuteMask.getAndUpdate(m => m & ~categoryBit[E])
      ()
    } {
      if (AssertionRunner.ASSERTIONS_ENABLED) {
        throw new DebugException(s"Attempted to unmute DebugEvent but it is not enabled at compile-time;" +
          s" update Debug.isEnabled")
      }
    }

  private inline def isMuted[E <: DebugEvent]: Boolean = (dynamicMuteMask.get() & categoryBit[E]) != 0L

  private inline def emit[E <: DebugEvent](inline event: E, inline stack: CaptureStack): Unit =
    logger.log(
      event,
      LogContext(
        CallSite.here,
        Thread.currentThread().getName,
        captureStack(stack)
      )
    )

  private inline def captureStack(inline stack: CaptureStack): Option[Array[StackTraceElement]] =
    if (stack != null) {
      Some(Exceptions.getPartialStackTrace(
        1 + stack.from,
        1 + stack.from + stack.limit
      ))
    } else {
      None
    }

  /**
   * Log a Debug event. Category must be enabled in [[isEnabled]] and not
   * runtime-[[mute]]d.
   *
   * {{{Debug.log(Category.Event(...))}}}
   */
  inline def log[E <: DebugEvent](inline event: E, inline stack: CaptureStack = null): Unit = {
    inline if (isEnabled[E]) {
      if (!isMuted[E]) {
        emit(event, stack)
      }
    }
  }

  final case class LogContext(site: CallSite, threadName: String, stackSlice: Option[Array[StackTraceElement]]) {

    def renderStackFrames: String =
      stackSlice.fold("")(elements => elements.mkString("\n\t at ", "\n\t at ", ""))
  }

  /**Optional additional parameter to Debug.log to instruct the logger to capture a slice of the stack */
  final case class CaptureStack(limit: Int = 10, from: Int = 0)

  class DebugException(msg: String) extends Exception(msg)
}
