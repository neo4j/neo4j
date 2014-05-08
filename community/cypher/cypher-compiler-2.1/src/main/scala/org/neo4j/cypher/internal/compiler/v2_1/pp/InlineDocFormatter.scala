package org.neo4j.cypher.internal.compiler.v2_1.pp

object InlineDocFormatter extends DocFormatter {
  def apply(doc: Doc): Seq[PrintCommand] =  {
    doc match {
      case NilDoc                => Seq(PrintText(""))
      case BreakDoc              => Seq(PrintText(" "))
      case BreakWith(value)      => Seq(PrintText(value))
      case TextDoc(value)        => Seq(PrintText(value))
      case NestDoc(_, inner)     => apply(inner)
      case GroupDoc(inner)       => apply(inner)
      case ConsDoc(head, NilDoc) => apply(head)
      case ConsDoc(head, tail)   => apply(head) ++ apply(tail)
    }
  }
}
