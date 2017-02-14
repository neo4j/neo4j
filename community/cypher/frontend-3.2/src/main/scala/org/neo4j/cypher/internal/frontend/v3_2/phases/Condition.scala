package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.SemanticState
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement

import scala.reflect.ClassTag

trait Condition {
  def check(state: AnyRef): Seq[String]
}

case class BaseContains[T: ClassTag](implicit manifest: Manifest[T]) extends Condition {
  private val acceptableTypes: Set[Class[_]] = Set(
    classOf[Statement],
    classOf[SemanticState]
  )

  assert(acceptableTypes.contains(manifest.runtimeClass))

  override def check(in: AnyRef): Seq[String] = in match {
    case state: BaseState =>
      manifest.runtimeClass match {
        case x if classOf[Statement] == x && state.maybeStatement.isEmpty => Seq("Statement missing")
        case x if classOf[SemanticState] == x && state.maybeSemantics.isEmpty => Seq("Semantic State missing")
        case _ => Seq.empty
      }
    case x => throw new IllegalArgumentException(s"Unknown state: $x")
  }
}
