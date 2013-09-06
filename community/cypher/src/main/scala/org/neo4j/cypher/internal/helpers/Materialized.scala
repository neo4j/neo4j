package org.neo4j.cypher.internal.helpers

import scala.collection.{TraversableLike, mutable, immutable}

object Materialized {
  def mapValues[A, B, C](m: collection.Map[A, B], f: B => C): Map[A, C] = {
    val builder: mutable.Builder[(A, C), Map[A, C]] = mapBuilder(m)

    for ( ((k, v)) <- m )
      builder += k -> f(v)
    builder.result()
  }

  def mapBuilder[A, B](underlying: TraversableLike[_, _]): mutable.Builder[(A, B), Map[A, B]] = {
    val builder = immutable.Map.newBuilder[A, B]
    builder.sizeHint(underlying)
    builder
  }
}