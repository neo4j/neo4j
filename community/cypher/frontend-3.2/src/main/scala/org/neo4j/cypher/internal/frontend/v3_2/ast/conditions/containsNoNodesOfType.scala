package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.ast.ASTNode
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

import scala.reflect.ClassTag

case class containsNoNodesOfType[T <: ASTNode](implicit tag: ClassTag[T]) extends Condition {
  def apply(that: Any): Seq[String] = collectNodesOfType[T].apply(that).map {
    node => s"Expected none but found ${node.getClass.getSimpleName} at position ${node.position}"
  }

  override def name = s"$productPrefix[${tag.runtimeClass.getSimpleName}]"
}
