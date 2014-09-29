package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.compiler.v2_2.perty._

case object formatErrors {
  def apply(inner: => Option[DocOps[Any]]): Option[DocOps[Any]] = {
    import org.neo4j.cypher.internal.compiler.v2_2.perty.ops.DocOps._

    try {
      inner
    } catch {
      case _: NotImplementedError =>
        Some(DocOps("???"))

      case _: MatchError =>
        None

      case e: Exception =>
        Some(DocOps(group(s"${e.getClass.getSimpleName}:" :/: e.toString)))

      case other: Throwable =>
        throw other
    }
  }
}
