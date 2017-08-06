package org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, bottomUp}

case object nameAllGraphs extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case graphDef: GraphDef if graphDef.alias.isEmpty =>
      val pos = graphDef.position.bumped()
      graphDef.withNewName(Variable(UnNamedNameGenerator.name(pos))(pos))
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])
}
