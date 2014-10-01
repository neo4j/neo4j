package org.neo4j.cypher.internal.compiler.v2_2.perty.print

import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.handler.DefaultDocHandler

import scala.reflect.runtime.universe.TypeTag

object pprint {
  // Print value to PrintStream after converting to a doc using the given generator and formatter
  def apply[T : TypeTag](value: T,
                         formatter: DocFormatter = DocFormatters.defaultPageFormatter)
                        (docGen: DocGenStrategy[T] = DefaultDocHandler.docGen): Unit = {
    Console.print(pprintToString(value, formatter)(docGen))
  }
}
