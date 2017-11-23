package org.neo4j.cypher.internal.compatibility.v3_3

import java.lang.reflect.Constructor

import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{ast => astV3_3}
import org.neo4j.cypher.internal.util.v3_4.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, InternalException, RewriterWithArgs, bottomUpWithArgs}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4}

object ASTConverter {

  def convertExpression[T <: ExpressionV3_4](expression: ExpressionV3_3): T = {
    val rewriter: RewriterWithArgs = bottomUpWithArgs(RewriterWithArgs.lift {
      case (expressionV3_3: astV3_3.Expression, children: Seq[AnyRef]) => {
        val classNameV3_3 = expressionV3_3.getClass.getName
        val classNameV3_4 = classNameV3_3.replace("frontend.v3_3.ast", "v3_4.expressions")
        val constructor = getConstructor(Class.forName(classNameV3_4))

        val params = constructor.getParameterTypes
        val args = children.toVector
        val hasExtraParam = params.length == args.length + 1
        val lastParamIsPos = params.last.isAssignableFrom(classOf[InputPosition])
        val ctorArgs = if (hasExtraParam && lastParamIsPos) args :+ helpers.as3_4(expressionV3_3.position) else args

        constructor.newInstance(ctorArgs: _*).asInstanceOf[AnyRef]
      }
    })

    new RewritableAny[ExpressionV3_3](expression).rewrite(rewriter, Seq.empty).asInstanceOf[T]
  }

  private def getConstructor(clazz: Class[_]): Constructor[_] = {
    try {
      clazz.getConstructors.head
    } catch {
      case e: NoSuchElementException =>
        throw new InternalException(
          s"Failed trying to rewrite $clazz - this class does not have a constructor"
        )
    }
  }
}