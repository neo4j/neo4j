package org.neo4j.cypher.internal.compiler.v2_3.birk.codegen


case object setStaticField {
  def apply(clazz: Class[_], name: String, value: AnyRef) = {
    clazz.getDeclaredFields.foreach(f => println("DECLARED FIELD " + f))
    clazz.getFields.foreach(f => println("FIELD " + f))
    clazz.getMethods.foreach(m => println("METHOD " + m))


    clazz.getField(name).set(null, value)
  }
}
