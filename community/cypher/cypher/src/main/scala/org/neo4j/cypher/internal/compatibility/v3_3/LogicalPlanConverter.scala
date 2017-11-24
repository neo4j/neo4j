package org.neo4j.cypher.internal.compatibility.v3_3

import java.lang.reflect.Constructor

import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{ast => astV3_3}
import org.neo4j.cypher.internal.ir.v3_3.{IdName => IdNameV3_3}
import org.neo4j.cypher.internal.ir.v3_4.{PlannerQuery, IdName => IdNameV3_4}
import org.neo4j.cypher.internal.util.v3_4.Rewritable.{DuplicatableProduct, RewritableAny}
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, InternalException, RewriterWithArgs, bottomUpWithArgs}
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan => LogicalPlanV3_3}
import org.neo4j.cypher.internal.v3_3.logical.{plans => plansV3_3}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan => LogicalPlanV3_4}

import scala.collection.mutable.{HashMap => MutableHashMap}

object LogicalPlanConverter {
  private val rewriter: RewriterWithArgs = bottomUpWithArgs(RewriterWithArgs.lift {
    case (plan: plansV3_3.LogicalPlan, children: Seq[AnyRef]) =>
      val newPlan = convertVersion("v3_3", "v3_4", plan, children, null, classOf[PlannerQuery])
      newPlan.asInstanceOf[LogicalPlanV3_4].setIdTo(helpers.as3_4(plan.assignedId))
      newPlan
    case (expressionV3_3: astV3_3.ASTNode, children: Seq[AnyRef]) =>
      convertVersion("frontend.v3_3.ast", "v3_4.expressions", expressionV3_3, children, helpers.as3_4(expressionV3_3.position), classOf[InputPosition])
    case (IdNameV3_3(name), _) => IdNameV3_4(name)
    case (_: Seq[_], children: Seq[AnyRef]) => children.toIndexedSeq
    case (_: Set[_], children: Seq[AnyRef]) => children.toSet
    case (_: Map[_, _], children: Seq[AnyRef]) => Map(children.map(_.asInstanceOf[(_, _)]): _*)
    case (None, _) => None
    case (p: Product, children: Seq[AnyRef]) => new DuplicatableProduct(p).copyConstructor.invoke(p, children: _*)
  })

  def convertLogicalPlan[T <: LogicalPlanV3_4](logicalPlan: LogicalPlanV3_3): LogicalPlanV3_4 = {
    new RewritableAny[LogicalPlanV3_3](logicalPlan).rewrite(rewriter, Seq.empty).asInstanceOf[T]
  }

  def convertExpression[T <: ExpressionV3_4](expression: ExpressionV3_3): T = {
    new RewritableAny[ExpressionV3_3](expression).rewrite(rewriter, Seq.empty).asInstanceOf[T]
  }

  private val constructors = new ThreadLocal[MutableHashMap[(String, String, String), Constructor[_]]]() {
    override def initialValue: MutableHashMap[(String, String, String), Constructor[_]] =
      new MutableHashMap[(String, String, String), Constructor[_]]
  }

  private def getConstructor(classNameV3_3: String, oldPackage: String, newPackage: String): Constructor[_] = {
    constructors.get.getOrElseUpdate((classNameV3_3, oldPackage, newPackage), {
      val classNameV3_4 = classNameV3_3.replace(oldPackage, newPackage)
      val clazz = Class.forName(classNameV3_4)
      try {
        clazz.getConstructors.head
      } catch {
        case _: NoSuchElementException =>
          throw new InternalException(
            s"Failed trying to rewrite $clazz - this class does not have a constructor"
          )
      }
    })
  }

  private def convertVersion(oldPackage: String, newPackage: String, thing: AnyRef, children: Seq[AnyRef], extraArg: AnyRef, assignableClazzForArg: Class[_]): AnyRef = {
    val classNameV3_3 = thing.getClass.getName
    val constructor = getConstructor(classNameV3_3, oldPackage, newPackage)

    val params = constructor.getParameterTypes
    val args = children.toVector
    val hasExtraParam = params.length == args.length + 1
    val lastParamIsPos = params.last.isAssignableFrom(assignableClazzForArg)
    val ctorArgs = if (hasExtraParam && lastParamIsPos) args :+ extraArg else args

    constructor.newInstance(ctorArgs: _*).asInstanceOf[AnyRef]
  }
}
