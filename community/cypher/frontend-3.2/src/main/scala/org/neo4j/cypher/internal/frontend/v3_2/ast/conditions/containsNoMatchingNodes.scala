package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.Foldable.FoldableAny
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_2.ast.ASTNode

case class containsNoMatchingNodes(matcher: PartialFunction[ASTNode, String]) extends (Any => Seq[String]) {

  def apply(that: Any): Seq[String] = {
    that.fold(Seq.empty[(String, InputPosition)]) {
      case node: ASTNode if matcher.isDefinedAt(node) =>
        (acc) => acc :+ ((matcher(node), node.position))
    }.map{ case (name, position) => s"Expected none but found $name at position $position" }
  }
}
