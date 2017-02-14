package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.Foldable.FoldableAny
import org.neo4j.cypher.internal.frontend.v3_2.ast.ASTNode

import scala.reflect.ClassTag

case class collectNodesOfType[T <: ASTNode](implicit tag: ClassTag[T]) extends (Any => Seq[T]) {
  def apply(that: Any): Seq[T] = that.fold(Seq.empty[T]) {
    case node: ASTNode if node.getClass == tag.runtimeClass =>
      (acc) => acc :+ node.asInstanceOf[T]
  }
}
