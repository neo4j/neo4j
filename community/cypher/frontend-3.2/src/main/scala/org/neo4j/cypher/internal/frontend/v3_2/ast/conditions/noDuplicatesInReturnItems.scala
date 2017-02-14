package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.ast.ReturnItems
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object noDuplicatesInReturnItems extends Condition {
  def apply(that: Any): Seq[String] = {
    val returnItems = collectNodesOfType[ReturnItems].apply(that)
    returnItems.collect {
      case ris@ReturnItems(_, items) if items.toSet.size != items.size =>
        s"ReturnItems at ${ris.position} contain duplicate return item: $ris"
    }
  }

  override def name: String = productPrefix
}
