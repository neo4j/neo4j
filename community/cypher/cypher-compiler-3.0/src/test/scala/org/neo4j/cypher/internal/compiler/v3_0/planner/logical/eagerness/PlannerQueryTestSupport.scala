/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast._

trait PlannerQueryTestSupport {
  self: LogicalPlanConstructionTestSupport with AstConstructionTestSupport =>
  implicit class richRel(r: CreateRelationshipPattern) {
    def andSetProperty(propName: String, intVal: Int): CreateRelationshipPattern =
      r.copy(properties = Some(MapExpression(Seq((PropertyKeyName(propName)(pos), literalInt(intVal))))(pos)))
  }
  implicit class richNode(r: CreateNodePattern) {
    def andSetProperty(propName: String, intVal: Int): CreateNodePattern =
      r.copy(properties = Some(MapExpression(Seq((PropertyKeyName(propName)(pos), literalInt(intVal))))(pos)))
  }

  implicit class qgHelper(qg: QueryGraph) {
    def withPredicate(e: Expression): QueryGraph = qg.withSelections(qg.selections ++ Selections.from(e))

    def withMutation(patterns: MutatingPattern*): QueryGraph = qg.addMutatingPatterns(patterns: _*)
  }

  implicit class crazyDslStart(in: Symbol) {
    def ->(rel: VarAndType) = new LeftNodeAndRel(VarWithoutType(in), rel)
    def --(rel: VarAndType) = new LeftNodeAndRel(VarWithoutType(in), rel)
    def -<-(rel: VarAndType) = new LeftNodeAndRel(VarWithoutType(in), rel)

    def ->(rel: Symbol) = new LeftNodeAndRel(VarWithoutType(in), VarWithoutType(rel))
    def --(rel: Symbol) = new LeftNodeAndRel(VarWithoutType(in), VarWithoutType(rel))
    def -<-(rel: Symbol) = new LeftNodeAndRel(VarWithoutType(in), VarWithoutType(rel))

    def ::(other: Symbol) = new VarAndType(other, in)
  }

  sealed case class LeftNodeAndRel(from: Var, rel: Var) {
    def -> (to: Symbol) = new Pattern(from, rel, VarWithoutType(to), SemanticDirection.OUTGOING)
    def -- (to: Symbol) = new Pattern(from, rel, VarWithoutType(to), SemanticDirection.BOTH)
    def -<- (to: Symbol) = new Pattern(from, rel, VarWithoutType(to), SemanticDirection.INCOMING)
  }

  trait Var {
    def variableName: Symbol
  }
  sealed case class VarAndType(variableName: Symbol, relType: Symbol) extends Var {
    def ->(rel: VarAndType) = new LeftNodeAndRel(rel, this)
  }
  sealed case class VarWithoutType(variableName: Symbol) extends Var {
    def ->(rel: VarAndType) = new LeftNodeAndRel(this, rel)
  }

  sealed case class Pattern(from: Var, rel: Var, to: Var, dir: SemanticDirection)

  // capitalized because `match` is a Scala keyword
  def MATCH(pattern: Pattern): QueryGraph = {
    matchRel(pattern.from.variableName.name, pattern.rel.variableName.name, pattern.to.variableName.name, readTypes(pattern), pattern.dir)
  }

  def merge(pattern: Pattern): QueryGraph = {
    val readQG = MATCH(pattern)
    val from = IdName(pattern.from.variableName.name)
    val r = IdName(pattern.rel.variableName.name)
    val to = IdName(pattern.to.variableName.name)
    val typ = RelTypeName(pattern.rel.asInstanceOf[VarAndType].relType.name)(pos)
    val relationshipPattern = CreateRelationshipPattern(r, from, typ, to, None, SemanticDirection.OUTGOING)
    val fromP = createNode(from.name)
    val toP = createNode(to.name)
    QueryGraph.empty withMutation MergeRelationshipPattern(Seq(fromP, toP), Seq(relationshipPattern), readQG, Seq.empty, Seq.empty)
  }

  def readTypes(xx: Pattern) = xx.rel match {
    case VarAndType(_, typ) => Seq(typ.name)
    case VarWithoutType(_) => Seq.empty
  }
  def writeType(xx: Pattern) = xx.rel match {
    case VarAndType(_, typ) => RelTypeName(typ.name)(pos)
    case VarWithoutType(_) => ???
  }

  def createRel(pattern: Pattern) =
    CreateRelationshipPattern(pattern.rel.variableName, pattern.from.variableName, writeType(pattern), pattern.to.variableName, None, SemanticDirection.OUTGOING)

  def eager(inner: LogicalPlan) = Eager(inner)(solved)

  def createNode(name: String, labels: Seq[String] = Seq.empty): CreateNodePattern =
    CreateNodePattern(IdName(name), labels.map(x => LabelName(x)(pos)), None)
  def setLabel(name: String, labels: String*): SetLabelPattern =
    SetLabelPattern(IdName(name), labels.map(x => LabelName(x)(pos)))
  def removeLabel(name: String, labels: String*): RemoveLabelPattern =
    RemoveLabelPattern(IdName(name), labels.map(x => LabelName(x)(pos)))
  def setNodeProperty(name: String, propKey: String): SetNodePropertyPattern =
    SetNodePropertyPattern(IdName(name), PropertyKeyName(propKey)(pos), StringLiteral("new property value")(pos))
  def setRelProperty(name: String, propKey: String) =
    SetRelationshipPropertyPattern(IdName(name), PropertyKeyName(propKey)(pos), StringLiteral("new property value")(pos))
  def delete(variable: String) =
    DeleteExpressionPattern(varFor(variable), forced = false)

  def delete(pattern: Pattern) = {
    val from = varFor(pattern.from.variableName.name)
    val r = varFor(pattern.rel.variableName.name)
    val to = varFor(pattern.to.variableName.name)
    val path = NodePathStep(from, SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, NodePathStep(to, NilPathStep)))

    DeleteExpressionPattern(PathExpression(path)(pos), forced = false)
  }

  def createNodeQG(name: String) = QueryGraph(mutatingPatterns = Seq(createNode(name)))
  def createNodeQG(name: String, label: String) = QueryGraph(mutatingPatterns = Seq(createNode(name, Seq(label))))
  def matchNode(name: String) = QueryGraph(patternNodes = Set(IdName(name)))
  def matchNodeQG(name: String, label: String) = QueryGraph(patternNodes = Set(IdName(name))).withSelections(Selections.from(hasLabels(name, label)))
  def argumentQG(name: String) = QueryGraph(argumentIds = Set(IdName(name)))
  def mergeNodeQG(name: String, label: String) = {
    val readQG = matchNode(name) withPredicate hasLabels(name, label) withPredicate propEquality(name, "prop", 42)
    val createNodePattern = createNode(name, Seq(label)) andSetProperty("prop", 42)
    QueryGraph.empty withMutation MergeNodePattern(createNodePattern, readQG, Seq.empty, Seq.empty)
  }

  def mergeNodeQG(name: String, label: String, prop: String, value: Int) = {
    val readQG = matchNode(name) withPredicate hasLabels(name, label) withPredicate propEquality(name, prop, value)
    val createNodePattern = createNode(name, Seq(label)) andSetProperty(prop, value)
    QueryGraph.empty withMutation MergeNodePattern(createNodePattern, readQG, Seq.empty, Seq.empty)
  }


  private def matchRel(from: String, name: String, to: String, typ: Seq[String], dir: SemanticDirection) = {
    val fromId = IdName(from)
    val toId = IdName(to)
    val types = typ.map(x => RelTypeName(x)(pos))
    val rel = PatternRelationship(IdName(name), (fromId, toId), dir, types, SimplePatternLength)
    QueryGraph(patternNodes = Set(fromId, toId), patternRelationships = Set(rel))
  }


  val solved: PlannerQuery with CardinalityEstimation = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality.SINGLE)

}
