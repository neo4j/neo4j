package org.neo4j.cypher.internal.compiler.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{IsCollection, IsMap}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.spi.QueryContext
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.Map

trait CypherSerializer {

  protected def serializeProperties(x: PropertyContainer, qtx: QueryContext): String = {
    val (ops, id) = x match {
      case n: Node => (qtx.nodeOps, n.getId)
      case r: Relationship => (qtx.relationshipOps, r.getId)
    }

    val keyValStrings = ops.propertyKeyIds(id).
      map(pkId => qtx.getPropertyKeyName(pkId) + ":" + serialize(ops.getProperty(id, pkId), qtx))

    keyValStrings.mkString("{", ",", "}")
  }

  protected def serialize(a: Any, qtx: QueryContext): String = a match {
    case x: Node            => x.toString + serializeProperties(x, qtx)
    case x: Relationship    => ":" + x.getType.name() + "[" + x.getId + "]" + serializeProperties(x, qtx)
    case IsMap(m)           => makeString(m, qtx)
    case IsCollection(coll) => coll.map(elem => serialize(elem, qtx)).mkString("[", ",", "]")
    case x: String          => "\"" + x + "\""
    case v: KeyToken        => v.name
    case Some(x)            => x.toString
    case null               => "<null>"
    case x                  => x.toString
  }

  protected def serializeWithType(x: Any)(implicit qs: QueryState) = s"${serialize(x, qs.query)} (${x.getClass.getSimpleName})"

  private def makeString(m: QueryContext => Map[String, Any], qtx: QueryContext) = m(qtx).map {
    case (k, v) => k + " -> " + serialize(v, qtx)
  }.mkString("{", ", ", "}")

  def makeSize(txt: String, wantedSize: Int): String = {
    val actualSize = txt.length()
    if (actualSize > wantedSize) {
      txt.slice(0, wantedSize)
    } else if (actualSize < wantedSize) {
      txt + repeat(" ", wantedSize - actualSize)
    } else txt
  }

  def repeat(x: String, size: Int): String = (1 to size).map((i) => x).mkString
}
