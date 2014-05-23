package org.neo4j.cypher.internal.compiler.v2_1.pprint

import scala.annotation.tailrec

object condense extends (Seq[PrintCommand] => Seq[PrintCommand]) {
  def apply(commands: Seq[PrintCommand]) = apply(Seq.newBuilder, commands).result()

  @tailrec
  def apply(builder: PrintingConverter[Seq[PrintCommand]],
            commands: Seq[PrintCommand]): PrintingConverter[Seq[PrintCommand]]= commands match {
    case PrintText(lhs) +: PrintText(rhs) +: tail => apply(builder, PrintText(lhs ++ rhs) +: tail)
    case head +: tail                             => apply(builder += head, tail)
    case _                                        => builder
  }
}
