package org.neo4j.cypher.internal.v4_0.ast.prettifier

import org.neo4j.cypher.internal.v4_0.expressions.{EveryPath, Expression, NamedPatternPart, NodePattern, Pattern, PatternElement, PatternPart, Range, RelationshipChain, RelationshipPattern, SemanticDirection, ShortestPaths}

case class PatternStringifier(expr: ExpressionStringifier) {

  def apply(p: Pattern): String =
    p.patternParts.map(apply).mkString(", ")

  def apply(p: PatternPart): String = p match {
    case e: EveryPath        => apply(e.element)
    case s: ShortestPaths    => s"${s.name}(${apply(s.element)})"
    case n: NamedPatternPart => s"${expr(n.variable)} = ${apply(n.patternPart)}"
  }

  def apply(element: PatternElement): String = element match {
    case r: RelationshipChain => apply(r)
    case n: NodePattern       => apply(n)
  }

  def apply(nodePattern: NodePattern): String = {
    val name = nodePattern.variable.map(expr).getOrElse("")
    val base = nodePattern.baseNode.map(expr).map(" COPY OF " + _).getOrElse("")
    val labels = if (nodePattern.labels.isEmpty) "" else
      nodePattern.labels.map(expr(_)).mkString(":", ":", "")
    val e = props(s"$name$base$labels", nodePattern.properties)
    s"($e)"
  }

  def apply(relationshipChain: RelationshipChain): String = {
    val r = apply(relationshipChain.rightNode)
    val middle = apply(relationshipChain.relationship)
    val l = apply(relationshipChain.element)

    s"$l$middle$r"
  }

  def apply(relationship: RelationshipPattern): String = {
    val lArrow = if (relationship.direction == SemanticDirection.INCOMING) "<" else ""
    val rArrow = if (relationship.direction == SemanticDirection.OUTGOING) ">" else ""
    val types = if (relationship.types.isEmpty)
      ""
    else
      relationship.types.map(expr(_)).mkString(":", "|", "")
    val name = relationship.variable.map(expr).getOrElse("")
    val base = relationship.baseRel.map(expr).map(" COPY OF " + _).getOrElse("")
    val length = relationship.length match {
      case None              => ""
      case Some(None)        => "*"
      case Some(Some(range)) => apply(range)

    }
    val info = props(s"$name$base$types$length", relationship.properties)
    if (info == "")
      s"$lArrow--$rArrow"
    else
      s"$lArrow-[$info]-$rArrow"
  }

  private def apply(r: Range) =
    s"*${r.lower.map(_.stringVal).getOrElse("")}..${r.upper.map(_.stringVal).getOrElse("")}"

  private def props(prepend: String, e: Option[Expression]): String = {
    e.map(e => {
      val separator = if (prepend.isEmpty) "" else " "
      s"$prepend$separator${expr(e)}"
    }).getOrElse(prepend)
  }
}
