package org.neo4j.cypher.internal.compiler.v2_2.perty

trait ToPretty {
  def toPretty: Option[DocRecipe[Any]]
}
