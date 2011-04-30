package org.neo4j.lab.cypher.pipes

import org.neo4j.graphdb.{DynamicRelationshipType, Node, NotFoundException, Direction}
import scala.collection.JavaConverters._

/**
 * Created by Andres Taylor
 * Date: 4/20/11
 * Time: 08:37
 */

class RelatedToPipe(fromColumn: String, nodeColumn: String, relColumn: String, relType: String, direction: Direction) extends Pipe {
  def foreach[U](f: (Map[String, Any]) => U) {
    getInput.foreach((m) => {
      val node = m.getOrElse(fromColumn, throw new NotFoundException()).asInstanceOf[Node]
      node.getRelationships(DynamicRelationshipType.withName(relType), direction).asScala.foreach((r) => {
        val otherNode = r.getOtherNode(node)
        f.apply(m ++ Map(nodeColumn -> otherNode, relColumn -> r))
      })
    })
  }

  def columnNames: List[String] = getInput.columnNames ++ List(nodeColumn, relColumn)

  def dependsOn: List[String] = List(fromColumn)
}