package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.NodeByLabel
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.graphdb.Node

case class NodeByLabelEntityProducer(nodeByLabel: NodeByLabel, labelId: Int) extends EntityProducer[Node] {

  def apply(m: ExecutionContext, q: QueryState) = q.query.getNodesByLabel(labelId)

  override def producerType: String = nodeByLabel.producerType

  override def arguments: Seq[Argument] = nodeByLabel.arguments
}
