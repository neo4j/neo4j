package org.neo4j.cypher.internal.compiler.v2_1.pp

object DocSupport {
  def end: Doc = NilDoc
  def break: Doc = BreakDoc
  def breakWith(value: String): Doc = BreakWith(value)
  def text(value: String): Doc = TextDoc(value)
  def nest(indent: Int, content: Doc): Doc = NestDoc(indent, content)
  def group(doc: Doc): Doc = GroupDoc(doc)
  def cons(head: Doc, tail: Doc = end): Doc = ConsDoc(head, tail)
}
