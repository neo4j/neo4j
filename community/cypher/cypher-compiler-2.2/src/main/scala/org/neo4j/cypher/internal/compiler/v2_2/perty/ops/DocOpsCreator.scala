package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.compiler.v2_2.perty.{BaseDocOps, DocOps}

import scala.reflect.runtime.universe._

// DSL for the easy and well-formed construction of DocOps
//
// The DSL constructs and combines Appenders (functions from DocOps[T] => DocOps[T]).
//
// This approach removes otherwise annoying differences between DocOp[T] and Seq[DocOp[T]]
// as well as minimizes the amount of sequence concatenation.
//
// The DSL uses the stack' however stack depth shouldn't be a concern here as we support
// AddContent on this level and everything is flattened away by expandDocOps
//
trait DocOpsCreator[T] {
  def apply(appender: Appender) = appender.asOps

  def group(ops: Appender): Appender =
    new Appender {
      def apply(append: DocOps[T]) = PushGroupFrame +: ops(PopFrame +: append)
    }

  def break: Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddBreak +: append
    }

  def content[S <: T : TypeTag](content: S): Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddContent(content) +: append
    }


  implicit def text(text: String): Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddText(text) +: append
    }

  abstract sealed protected class Appender extends (DocOps[T] => DocOps[T]) {
    self =>

    def ::(hd: Appender) = new Appender {
      def apply(append: DocOps[T]) = hd(self(append))
    }

    def :/:(hd: Appender) = new Appender {
      def apply(append: DocOps[T]) = hd(AddBreak +: self(append))
    }

    def asOps: DocOps[T] = apply(Seq.empty)
  }
}

case object DocOps extends DocOpsCreator[Any]

