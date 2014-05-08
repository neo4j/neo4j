package org.neo4j.cypher.internal.compiler.v2_1.pp

import scala.collection.mutable

class StringPrinter(builder: mutable.StringBuilder = new mutable.StringBuilder()) extends CommandPrinter[String] {
  def clear() {
    builder.clear()
  }

  def result() = builder.result()

  def +=(elem: PrintCommand) = {
    elem match {
      case PrintText(text) =>
        builder ++= text

      case PrintNewLine(indent) =>
        builder += '\n'
        var remaining = indent
        while (remaining > 0) {
          builder += ' '
          remaining  -= 1
        }


    }
    this
  }
}

object printString extends (Seq[PrintCommand] => String) {
  def apply(commands: Seq[PrintCommand]) =
    (new StringPrinter() ++= commands).result()
}
