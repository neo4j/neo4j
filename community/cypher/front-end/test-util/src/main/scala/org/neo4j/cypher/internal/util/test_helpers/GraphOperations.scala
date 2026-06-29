/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

/**
 * Abstraction over graph modifications so that graph creation functions can be easily reused between implementations.
 */
trait GraphOperations {
  type Node <: GraphOperations.Node[Node]
  def createNode(labels: String*): Node
}

object GraphOperations {

  trait Node[NODE] {
    def id: Long

    def createRelationshipTo(other: NODE, relType: String): Unit

    def addLabel(label: String): Unit
  }

}

/***
 * Wrap a GraphOperations instance with this class to capture all modifications to the graph as a scala statement.
 * Useful in generated test contexts when reducing an issue to a smaller graph.
 *
 * @param inner The wrapped GraphOperations
 * @param log The output sink
 * @param operationsName The name of the GraphOperations instance in the replay
 */
class LogReplayableOperations(inner: GraphOperations, log: String => Unit, operationsName: String = "graph")
    extends GraphOperations {

  override type Node = ReconstructableNode

  class ReconstructableNode private[LogReplayableOperations] (node: inner.Node)
      extends GraphOperations.Node[ReconstructableNode] {
    def varName: String = s"n$id"

    override def id: Long = node.id

    private def getNode: inner.Node = node

    override def createRelationshipTo(other: ReconstructableNode, relType: String): Unit = {
      node.createRelationshipTo(other.getNode, relType)
      log(s"$varName.createRelationshipTo(${other.varName}, \"$relType\")")
    }

    override def addLabel(label: String): Unit = {
      node.addLabel(label)
      log(s"$varName.addLabel(\"$label\")")
    }
  }

  override def createNode(labels: String*): ReconstructableNode = {
    val res = new ReconstructableNode(inner.createNode(labels: _*))
    log(s"val ${res.varName} = $operationsName.createNode(${labels.map(s => s"\"$s\"").mkString(", ")})")
    res
  }
}
