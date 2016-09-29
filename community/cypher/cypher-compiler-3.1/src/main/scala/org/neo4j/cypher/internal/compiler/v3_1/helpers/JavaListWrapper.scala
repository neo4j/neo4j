package org.neo4j.cypher.internal.compiler.v3_1.helpers

/**
  * Simple wrapper for a java.util.List which preserves the original list
  * while lazily converts to scala values if needed.
  * @param inner the inner java list
  * @param converter
  */
case class JavaListWrapper[T](inner: java.util.List[T], converter: RuntimeScalaValueConverter) extends Seq[Any] {

  override def length = inner.size()

  override def iterator: Iterator[Any] = new Iterator[Any] {
    private val innerIterator = inner.iterator()
    override def hasNext: Boolean = innerIterator.hasNext

    override def next(): Any = converter.asDeepScalaValue(innerIterator.next())
  }

  override def apply(idx: Int) = converter.asDeepScalaValue(inner.get(idx))

}
