package org.neo4j.cypher.internal.compiler.v2_1.pp

sealed abstract class Doc {
  override def toString = printString(InlineDocFormatter(DocStructureDocGenerator(this)))
}

case object NilDoc extends Doc

case object BreakDoc extends Doc

case class BreakWith(value: String) extends Doc

case class TextDoc(value: String) extends Doc

case class NestDoc(indent: Int, content: Doc) extends Doc

case class GroupDoc(content: Doc) extends Doc

case class ConsDoc(head: Doc, tail: Doc = NilDoc) extends Doc
