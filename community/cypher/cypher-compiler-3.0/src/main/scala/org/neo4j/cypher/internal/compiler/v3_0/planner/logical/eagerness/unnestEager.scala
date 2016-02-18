package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.helpers.fixedPoint

case object unnestEager extends Rewriter {

  /*
  Based on unnestApply (which references a paper)

  This rewriter does _not_ adhere to the contract of moving from a valid
  plan to a valid plan, but it is crucial to get eager plans placed correctly.

  Glossary:
    Ax : Apply
    L,R: Arbitrary operator, named Left and Right
    SR : SingleRow - operator that produces single row with no columns
    CN : CreateNode
    Dn : Delete node
    Dr : Delete relationship
    E : Eager
    Sp : SetProperty
    Sm : SetPropertiesFromMap
    Sl : SetLabels
    U : Unwind
   */

  private val instance: Rewriter = fixedPoint(bottomUp(Rewriter.lift {

    // L Ax (E R) => E Ax (L R)
    case apply@Apply(lhs, eager@Eager(inner)) =>
      eager.copy(inner = Apply(lhs, inner)(apply.solved))(apply.solved)

    // L Ax (CN R) => CN Ax (L R)
    case apply@Apply(lhs, create@CreateNode(rhs, name, labels, props)) =>
      create.copy(source = Apply(lhs, rhs)(apply.solved), name, labels, props)(apply.solved)

    // L Ax (CR R) => CR Ax (L R)
    case apply@Apply(lhs, create@CreateRelationship(rhs, _, _, _, _, _)) =>
      create.copy(source = Apply(lhs, rhs)(apply.solved))(apply.solved)

    // L Ax (Dn R) => Dn Ax (L R)
    case apply@Apply(lhs, delete@DeleteNode(rhs, expr)) =>
      delete.copy(source = Apply(lhs, rhs)(apply.solved), expr)(apply.solved)

    // L Ax (Dr R) => Dr Ax (L R)
    case apply@Apply(lhs, delete@DeleteRelationship(rhs, expr)) =>
      delete.copy(source = Apply(lhs, rhs)(apply.solved), expr)(apply.solved)

    // L Ax (Sp R) => Sp Ax (L R)
    case apply@Apply(lhs, set@SetNodeProperty(rhs, idName, key, value)) =>
      set.copy(source = Apply(lhs, rhs)(apply.solved), idName, key, value)(apply.solved)

    // L Ax (Sm R) => Sm Ax (L R)
    case apply@Apply(lhs, set@SetNodePropertiesFromMap(rhs, idName, expr, removes)) =>
      set.copy(source = Apply(lhs, rhs)(apply.solved), idName, expr, removes)(apply.solved)

    // L Ax (Sl R) => Sl Ax (L R)
    case apply@Apply(lhs, set@SetLabels(rhs, idName, labelNames)) =>
      set.copy(source = Apply(lhs, rhs)(apply.solved), idName, labelNames)(apply.solved)

    // L Ax (Rl R) => Rl Ax (L R)
    case apply@Apply(lhs, remove@RemoveLabels(rhs, idName, labelNames)) =>
      remove.copy(source = Apply(lhs, rhs)(apply.solved), idName, labelNames)(apply.solved)

  }))

  override def apply(input: AnyRef) = instance.apply(input)
}
