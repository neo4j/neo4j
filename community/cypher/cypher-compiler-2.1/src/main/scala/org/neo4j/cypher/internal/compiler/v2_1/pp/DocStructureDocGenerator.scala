package org.neo4j.cypher.internal.compiler.v2_1.pp

object DocStructureDocGenerator extends DocGenerator[Doc] {
 import DocSupport._

 def apply(data: Doc): Doc = data match {
   case NilDoc           => text("ø")
   case BreakDoc         => breakWith("_")
   case BreakWith(value) => breakWith(s"_${value}_")
   case TextDoc(value)   => text(s"${"\""}$value${"\""}")
   case ConsDoc(hd, tl)  => cons(apply(hd), cons(TextDoc("·"), apply(tl)))
   case GroupDoc(doc)    => group(cons(text("["), cons(apply(doc), text("]"))))
   case NestDoc(i, doc)  => group(cons(text(s"($i)<"), cons(apply(doc), text(">"))))
 }
}
