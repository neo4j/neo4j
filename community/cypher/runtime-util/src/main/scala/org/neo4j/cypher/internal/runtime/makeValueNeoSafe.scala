package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.v4_0.util.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{ArrayValue, Value, Values}
import org.neo4j.values.virtual.ListValue

import scala.collection.JavaConverters._

object makeValueNeoSafe extends (AnyValue => Value) with ListSupport {

  def apply(a: AnyValue): Value = a match {
    case value: Value => value
    case IsList(l) => transformTraversableToArray(l)
    case _ => throw new CypherTypeException("Property values can only be of primitive types or arrays thereof")
  }
  /*
  This method finds the type that we can use for the primitive array that Neo4j wants
  We can't just find the nearest common supertype - we need a type that the other values
  can be coerced to according to Cypher coercion rules
   */
  private def transformTraversableToArray(a: ListValue): ArrayValue = {
    if (a.storable()) {
      a.toStorableArray
    } else if (a.isEmpty) {
      Values.stringArray(Array.empty[String]:_*)
    } else {
      val typeValue = a.iterator().asScala.reduce(CastSupport.merge)
      val converter = CastSupport.getConverter(typeValue)
      converter.arrayConverter(a)
    }
  }
}
