package org.neo4j.cypher.internal.compiler.v2_2.ast.conditions

import org.neo4j.cypher.internal.compiler.v2_2.ast.ReturnAll

case object containsNoReturnStar extends (Any => Seq[String]) {

  import org.neo4j.cypher.internal.compiler.v2_2.Foldable._

  def apply(that: Any): Seq[String] =
    that
      .fold[Seq[ReturnAll]](Seq.empty) { case x: ReturnAll => (acc) => acc :+ x }
      .map { (item: ReturnAll) => s"Expected none but found ReturnAll at position ${item.position}"}
}
