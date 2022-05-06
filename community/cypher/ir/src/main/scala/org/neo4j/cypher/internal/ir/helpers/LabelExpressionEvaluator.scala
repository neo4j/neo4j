package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.Xor

object LabelExpressionEvaluator {

  case class NodesToCheckOverlap(updatedNode: Option[String], matchedNode: String) {

    def contains(node: String): Boolean =
      updatedNode.contains(node) || matchedNode == node
  }

  /**
   * Evaluates a label expression given a set of nodes and labels for which "HasLabels" evaluates to true.
   *
   * E.g.
   * Given
   * labels: "A"
   * nodes:  "x", "y"
   *
   * Expression            Return value      Comment
   * (x:A)                 true
   * (x:A&B) AND (y:A)     false             (x:B) is evaluated to false, because "B" is not a given label
   * (x:A|B) AND (y:!B)    true
   * x.prop = 5            None              Expression is unknown
   * (x:A) AND (z:A)       None              z is in the given set of nodes
   *
   * @param labelExpression - the label expression to evaluate.
   * @param nodes - the nodes of interest, returns None if any other node is encountered.
   * @param labels - the labels evaluated to true, all other labels will be evaluated to false.
   * @return - the evaluated expression value or None if the expression is unknown.
   */
  def labelExpressionEvaluator(
    labelExpression: Expression,
    nodes: NodesToCheckOverlap,
    labels: Set[String]
  ): Option[Boolean] = {
    labelExpression match {
      case HasLabels(Variable(node), hasLabels) if nodes.contains(node) =>
        Some(labels.exists(hasLabels.map(_.name).contains))
      case And(lhs, rhs) => evalBinFunc(nodes, lhs, rhs, labels, (lhs, rhs) => lhs && rhs)
      case Or(lhs, rhs)  => evalBinFunc(nodes, lhs, rhs, labels, (lhs, rhs) => lhs || rhs)
      case Not(expr)     => labelExpressionEvaluator(expr, nodes, labels).map(!_)
      case Ors(exprs) =>
        val evaluatedExprs = exprs.map(expr => labelExpressionEvaluator(expr, nodes, labels))
        if (evaluatedExprs.contains(None)) {
          None
        } else {
          Some(evaluatedExprs.flatten.contains(true))
        }
      case Ands(exprs) =>
        val evaluatedExprs = exprs.map(expr => labelExpressionEvaluator(expr, nodes, labels))
        if (evaluatedExprs.contains(None)) {
          None
        } else {
          Some(!evaluatedExprs.flatten.contains(false))
        }
      case Xor(lhs, rhs)       => evalBinFunc(nodes, lhs, rhs, labels, (lhs, rhs) => lhs ^ rhs)
      case Equals(lhs, rhs)    => evalBinFunc(nodes, lhs, rhs, labels, (lhs, rhs) => lhs == rhs)
      case NotEquals(lhs, rhs) => evalBinFunc(nodes, lhs, rhs, labels, (lhs, rhs) => lhs != rhs)
      case _                   => None
    }
  }

  private def evalBinFunc(
    nodes: NodesToCheckOverlap,
    a: Expression,
    b: Expression,
    labels: Set[String],
    op: (Boolean, Boolean) => Boolean
  ): Option[Boolean] =
    for {
      lhs <- labelExpressionEvaluator(a, nodes, labels)
      rhs <- labelExpressionEvaluator(b, nodes, labels)
    } yield op(lhs, rhs)
}
