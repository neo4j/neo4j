package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.ast.ReturnItems
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object containsNoReturnAll extends Condition {
  private val matcher = containsNoMatchingNodes({
    case ri: ReturnItems if ri.includeExisting => "ReturnItems(includeExisting = true, ...)"
  })
  def apply(that: Any) = matcher(that)

  override def name: String = productPrefix
}
