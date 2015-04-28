package org.neo4j.cypher.internal.compiler.v2_3.birk.codegen


case object setStaticField {
  def apply(clazz: Class[_], name: String, value: AnyRef) = {
    clazz.getDeclaredField(name).set(null, value)
  }
}
