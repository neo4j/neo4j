package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_3.InternalException

case class PrimitiveExecutionContext(pipeline: PipelineInformation) extends ExecutionContext {
  def copyFrom(input: ExecutionContext): Unit = input match {
    case other@PrimitiveExecutionContext(otherPipeline) =>
      if (otherPipeline.numberOfLongs > pipeline.numberOfLongs)
        throw new InternalException("Tried to copy more data into less.")
      else
        System.arraycopy(other.longs, 0, longs, 0, otherPipeline.numberOfLongs)

    case _ => fail()
  }

  private val longs = new Array[Long](pipeline.numberOfLongs)

  def setLongAt(offset: Int, value: Long): Unit = longs(offset) = value

  def getLongAt(offset: Int): Long = longs(offset)

  override def +=(kv: (String, Any)) = fail()

  override def -=(key: String) = fail()

  override def get(key: String) = fail()

  override def iterator = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a primitive context as a map")

  override def newWith1(key1: String, value1: Any): ExecutionContext = fail()

  override def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext = fail()

  override def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext = fail()

  override def mergeWith(other: ExecutionContext): ExecutionContext = fail()

  override def createClone(): ExecutionContext = fail()

  override def newWith(newEntries: Seq[(String, Any)]): ExecutionContext = fail()
}
