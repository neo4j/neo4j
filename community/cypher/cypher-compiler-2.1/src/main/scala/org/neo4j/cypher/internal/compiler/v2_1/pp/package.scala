package org.neo4j.cypher.internal.compiler.v2_1

import scala.collection.mutable

package object pp {
  type DocFormatter = Doc => Seq[PrintCommand]
  type DocGenerator[T] = T => Doc
  type CommandPrinter[T] = mutable.Builder[PrintCommand, T]
}
