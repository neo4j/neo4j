package org.neo4j.cypher.internal.frontend.v3_2.helpers

import scala.reflect.ClassTag

object PartialFunctionSupport {

  def reduceAnyDefined[A, B](functions: Seq[PartialFunction[A, B]])(init: B)(combine: (B, B) => B) = foldAnyDefined[A, B, B](functions)(init)(combine)

  def foldAnyDefined[A, B, C](functions: Seq[PartialFunction[A, B]])(init: C)(combine: (C, B) => C): PartialFunction[A, C] = new PartialFunction[A, C] {
    override def isDefinedAt(v: A) = functions.exists(_.isDefinedAt(v))
    override def apply(v: A) = functions.foldLeft(init)((acc, pf) => if (pf.isDefinedAt(v)) combine(acc, pf(v)) else acc)
  }

  def composeIfDefined[A](functions: Seq[PartialFunction[A, A]]) = new PartialFunction[A, A] {
    override def isDefinedAt(v: A): Boolean = functions.exists(_.isDefinedAt(v))
    override def apply(init: A): A = functions.foldLeft[Option[A]](None)({ (current: Option[A], pf: PartialFunction[A, A]) =>
      val v = current.getOrElse(init)
      if (pf.isDefinedAt(v)) Some(pf(v)) else current
    }).get
  }

  def uplift[A: ClassTag, B, S >: A](f: PartialFunction[A, B]): PartialFunction[S, B] = {
    case v: A if f.isDefinedAt(v) => f(v)
  }

  def fix[A, B](f: PartialFunction[A, PartialFunction[A, B] => B]): PartialFunction[A, B] = new fixedPartialFunction(f)

  class fixedPartialFunction[-A, +B](f: PartialFunction[A, PartialFunction[A, B] => B]) extends PartialFunction[A, B] {
    def isDefinedAt(v: A) = f.isDefinedAt(v)
    def apply(v: A) = f(v)(this)
  }
}
