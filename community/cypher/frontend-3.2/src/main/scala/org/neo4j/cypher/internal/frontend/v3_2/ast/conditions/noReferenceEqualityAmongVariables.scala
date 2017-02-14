package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.Ref
import org.neo4j.cypher.internal.frontend.v3_2.ast.Variable
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object noReferenceEqualityAmongVariables extends Condition {
  def apply(that: Any): Seq[String] = {
    val ids = collectNodesOfType[Variable].apply(that).map(Ref[Variable])
    ids.groupBy(x => x).collect {
      case (id, others) if others.size > 1 => s"The instance ${id.value} is used ${others.size} times"
    }.toIndexedSeq
  }

  override def name: String = productPrefix
}
