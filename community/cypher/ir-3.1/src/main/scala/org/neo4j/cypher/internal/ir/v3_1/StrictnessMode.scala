package org.neo4j.cypher.internal.ir.v3_1


sealed trait StrictnessMode extends (Strictness => Boolean) {
  self: Product =>

  def apply(havingStrictness: Strictness) = havingStrictness.strictness == self

  override def toString: String = self.productPrefix
}

case object LazyMode extends StrictnessMode

case object EagerMode extends StrictnessMode

trait Strictness {
  def strictness: StrictnessMode
}
