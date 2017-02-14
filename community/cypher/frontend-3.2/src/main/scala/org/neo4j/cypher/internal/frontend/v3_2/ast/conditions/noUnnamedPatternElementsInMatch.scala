package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Match, NodePattern, RelationshipPattern}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object noUnnamedPatternElementsInMatch extends Condition {
  def apply(that: Any): Seq[String] = {
    val into = collectNodesOfType[Match].apply(that).map(_.pattern)
    into.flatMap(unnamedNodePatterns) ++ into.flatMap(unnamedRelationshipPatterns)
  }

  private def unnamedRelationshipPatterns(that: Any): Seq[String] = {
    collectNodesOfType[RelationshipPattern].apply(that).collect {
      case rel@RelationshipPattern(None, _, _, _, _) =>
        s"RelationshipPattern at ${rel.position} is unnamed"
    }
  }

  private def unnamedNodePatterns(that: Any): Seq[String] = {
    collectNodesOfType[NodePattern].apply(that).collect {
      case node@NodePattern(None, _, _) =>
        s"NodePattern at ${node.position} is unnamed"
    }
  }

  override def name: String = productPrefix
}
