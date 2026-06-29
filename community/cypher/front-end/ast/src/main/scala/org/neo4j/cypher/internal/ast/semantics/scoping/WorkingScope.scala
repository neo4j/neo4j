/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.semantics.scoping

import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState.RecordedScopes
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope.noLocalCallables
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope.unitVariables
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternAtom
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref

case class SymbolGroup(
  declaration: Ref[LogicalVariable],
  uses: Set[Ref[LogicalVariable]],
  renaming: Option[String]
) {

  def getRenamedUses: Option[Map[Ref[LogicalVariable], LogicalVariable]] =
    renaming.map(renaming => uses.map(x => x -> x.value.renameId(renaming)).toMap)

  def pastablePrint: String =
    s"ExpectedSymbolGroup(varOf(\"${declaration.value.name}\",${declaration.value.position.offset}), List(${uses.toSeq.map(
        u =>
          s"varOf(\"${u.value.name}\",${u.value.position.offset})"
      ).mkString(", ")}), ${renaming.map(s => s"Some(\"$s\")").getOrElse("None")})"

}

case object DummyASTNode extends ASTNode {
  self =>
  override def position: InputPosition = InputPosition.NONE
  override def canEqual(that: Any): Boolean = false
  override def productArity: Int = 0
  override def productElement(n: Int): Any = ()
}

sealed trait WorkingScope extends Product with Foldable {
  def astNode: ASTNode
  def incoming: WorkingContext

  /**
   * The references this scope publishes to its parent — a map from each variable use to the
   * declaration it resolves to; parents aggregate these via [[WorkingScope.referencedInChildren]].
   *
   * For ordinary scopes these are the variables literally used here. The exception is a
   * recognized leaf (see [[WorkingContext.recognizedLeafScope]], e.g. a GROUP BY grouping key):
   * there this set is the recognized *column identity* (the projection alias, or the bare variable
   * itself), so the parent sees the scope as that output column rather than as the raw variables
   * inside the expression. Those raw variables are then kept in [[hiddenReferences]].
   */
  def referenced: References

  /**
   * Real variable uses deliberately kept OUT of [[referenced]], so they survive whole-tree
   * resolution ([[collectAllReferences]] / [[getSymbolGroups]]) without appearing in the scope's
   * published set. Empty for ordinary scopes. Populated in two cases:
   *   - a recognized leaf over a non-variable expression, where [[referenced]] holds the alias and
   *     the variables actually written in the expression live here;
   *   - a UNION branch, where a `UnionMapping` links the branch's output column to the union column
   *     without leaking into the branch's published [[referenced]].
   *
   * Derived from [[referenced]]'s hidden channel, so it survives every `copy`.
   */
  final def hiddenReferences: References = referenced.hiddenRefs

  def declared: Declarations
  def outgoing: RegularContext
  def result: Result
  def children: Seq[WorkingScope]
  def inImportingWith: Boolean = false
  def foreachIterVar: Option[LogicalVariable] = None

  def withChildren(children: Seq[WorkingScope]): WorkingScope
  def withDeclared(declared: Declarations): WorkingScope
  def addReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): WorkingScope
  def addHiddenReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): WorkingScope

  def tagForCache(inImportingWith: Boolean, foreachIterVar: Option[LogicalVariable]): WorkingScope = this

  private def getScopes: Seq[(Ref[ASTNode], WorkingScope)] =
    (Ref(astNode), this) +: children.flatMap(_.getScopes)

  def getRecordedScopes: RecordedScopes = getScopes.toMap

  def collectAllReferences: Map[Ref[LogicalVariable], Ref[LogicalVariable]] = {
    val acc = scala.collection.mutable.HashMap.empty[Ref[LogicalVariable], Ref[LogicalVariable]]
    WorkingScope.collectReferencesInto(this, acc)
    acc.toMap
  }

  def collectAllDeclarations: Set[Ref[LogicalVariable]] =
    children.foldLeft(declared.allSymbols.map(Ref.apply)) { case (declarations, child) =>
      declarations ++ child.collectAllDeclarations
    }.toSet

  def collectAllReturnAliases: Set[Ref[LogicalVariable]] = {
    astNode.folder.treeCollect[Set[Ref[LogicalVariable]]]({
      case Return(_, ReturnItems(_, items, _), _, _, _, _, _, _, _) =>
        items.flatMap(x => if (x.isPassThrough) None else Some(x.alias)).toSet.flatten.map(Ref.apply)
      case _ => Set.empty
    }).flatten.toSet
  }

  def getSymbolGroups(anonVarGen: AnonymousVariableNameGenerator): Seq[SymbolGroup] = {
    val allDecls: Set[Ref[LogicalVariable]] = collectAllDeclarations
    val returnAliases: Set[Ref[LogicalVariable]] = collectAllReturnAliases
    val normalizedRefs: Map[Ref[LogicalVariable], Ref[LogicalVariable]] =
      WorkingScope.normalizeReferences(collectAllReferences)

    val anchors: Set[Ref[LogicalVariable]] = allDecls ++ returnAliases
    val anchoredRefs: Map[Ref[LogicalVariable], Ref[LogicalVariable]] =
      normalizedRefs.filter { case (_, target) => anchors.contains(target) }

    // Every anchor that isn't already used as a caller contributes a self-reference.
    val declarationsAsSelfRefs: Map[Ref[LogicalVariable], Ref[LogicalVariable]] =
      (anchors -- anchoredRefs.keySet).iterator.map(v => v -> v).toMap
    val allRefs: Map[Ref[LogicalVariable], Ref[LogicalVariable]] = anchoredRefs ++ declarationsAsSelfRefs

    val groupsByDecl: Map[Ref[LogicalVariable], Map[Ref[LogicalVariable], Ref[LogicalVariable]]] =
      allRefs.groupBy(_._2)

    val walkOrder: Map[Ref[LogicalVariable], Int] =
      astNode.folder.treeCollect[LogicalVariable] {
        case lv: LogicalVariable => lv
      }.iterator.map(Ref.apply).zipWithIndex.toMap

    val sortedGroups = groupsByDecl.toSeq.sortBy { case (dec, _) =>
      (walkOrder.getOrElse(dec, Int.MaxValue), dec.value.position)
    }

    // A declaration needs renaming iff another declaration shares its name.
    val ambiguous: Set[Ref[LogicalVariable]] = {
      val byName = sortedGroups.iterator.map(_._1).toSeq.groupBy(_.value.name)
      byName.valuesIterator.filter(_.sizeIs > 1).flatten.toSet
    }

    val renamings: Map[Ref[LogicalVariable], String] =
      sortedGroups.iterator
        .map(_._1)
        .filter(ambiguous.contains)
        .map(dec => dec -> AnonymousVariableNameGenerator.genName(anonVarGen, dec.value.name))
        .toMap

    sortedGroups.map { case (dec, callers) =>
      SymbolGroup(dec, callers.keySet + dec, renamings.get(dec))
    }
  }
}

object WorkingScope {
  val noChildren = Seq.empty[WorkingScope]
  val unitVariables = Set.empty[LogicalVariable]
  val noLocalCallables: Set[LocalCallableScopeSignature] = Set.empty[LocalCallableScopeSignature]

  @inline def apriori(outgoing: RegularContext): WorkingScope =
    AprioriScope(RegularContext.unit, outgoing)

  @inline def apriori(incoming: RegularContext, outgoing: RegularContext): WorkingScope =
    AprioriScope(incoming, outgoing)

  @inline def aprioriPattern(incoming: PatternIncomingContext, outgoing: RegularContext): PatternScope =
    PatternScope(
      astNode = DummyASTNode,
      patternIncoming = incoming,
      referenced = References.empty,
      declared = Declarations.noDeclarations,
      // outgoing = outgoing,
      result = TableResult(Seq.empty),
      children = WorkingScope.noChildren
    )

  def referencedInChildren(children: Seq[WorkingScope]): References =
    children.foldLeft(References.empty) {
      (referenced, c2) => referenced union c2.referenced.publishedOnly
    }

  private[scoping] def collectReferencesInto(
    scope: WorkingScope,
    acc: scala.collection.mutable.HashMap[Ref[LogicalVariable], Ref[LogicalVariable]]
  ): Unit = {
    mergeReferencesInto(acc, scope.referenced.references)
    mergeReferencesInto(acc, scope.hiddenReferences.references)
    scope.children.foreach(child => collectReferencesInto(child, acc))
  }

  private def mergeReferencesInto(
    acc: scala.collection.mutable.HashMap[Ref[LogicalVariable], Ref[LogicalVariable]],
    b: Map[Ref[LogicalVariable], Ref[LogicalVariable]]
  ): Unit = {
    if (b.nonEmpty) {
      b.foreach { case (k, v) =>
        acc.get(k) match {
          case Some(existing) if existing == k && v != k =>
            // incumbent is a self-ref, challenger is a real chain → chain wins
            acc.update(k, v)
          case Some(existing) if existing != k && v == k =>
            // incumbent is a real chain, challenger is a self-ref → keep incumbent
            ()
          case _ =>
            // both chains, both self-refs, or no incumbent — right-biased default
            acc.update(k, v)
        }
      }
    }
  }

  def normalizeReferences(
    refs: Map[Ref[LogicalVariable], Ref[LogicalVariable]]
  ): Map[Ref[LogicalVariable], Ref[LogicalVariable]] = {
    val normalized = scala.collection.mutable.HashMap.from(refs)
    val visiting = scala.collection.mutable.Set.empty[Ref[LogicalVariable]]
    def root(v: Ref[LogicalVariable]): Ref[LogicalVariable] = normalized.get(v) match {
      case None                                  => v
      case Some(next) if next == v               => v
      case Some(next) if visiting.contains(next) => v // cycle — break by treating v as its own root
      case Some(next) =>
        visiting += v
        val r = root(next)
        visiting -= v
        normalized.update(v, r)
        r
    }
    refs.keys.foreach(root)
    normalized.toMap
  }
}

case class AprioriScope(incoming: RegularContext, outgoing: RegularContext) extends WorkingScope {
  override def astNode: ASTNode = DummyASTNode
  override def referenced: References = References.empty
  override def declared: Declarations = Declarations.noDeclarations
  override def result: Result = NoResult
  override def children: Seq[WorkingScope] = WorkingScope.noChildren

  override def withChildren(children: Seq[WorkingScope]): AprioriScope = this
  override def withDeclared(declared: Declarations): AprioriScope = this

  override def addReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): AprioriScope = this

  override def addHiddenReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): AprioriScope =
    this
}

case class StatementScope(
  astNode: ASTNode,
  incoming: RegularContext,
  referenced: References,
  declared: Declarations,
  outgoing: RegularContext,
  result: Result = NoResult,
  children: Seq[WorkingScope] = WorkingScope.noChildren,
  override val inImportingWith: Boolean = false
) extends WorkingScope {
  private var _foreachIterVar: Option[LogicalVariable] = None
  override def foreachIterVar: Option[LogicalVariable] = _foreachIterVar

  override def withChildren(children: Seq[WorkingScope]): StatementScope = copy(children = children)
  override def withDeclared(declared: Declarations): StatementScope = copy(declared = declared)

  override def addReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): StatementScope =
    copy(referenced = referenced.union(References(references.toMap)))

  override def addHiddenReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): StatementScope =
    copy(referenced = referenced.addHidden(references.toMap))

  override def tagForCache(
    inImportingWith: Boolean,
    foreachIterVar: Option[LogicalVariable]
  ): StatementScope = {
    val c = this.copy(inImportingWith = inImportingWith)
    c._foreachIterVar = foreachIterVar
    c
  }
}

case class ExpressionScope(
  astNode: ASTNode,
  incoming: RegularContext,
  referenced: References,
  declared: Declarations,
  children: Seq[WorkingScope] = WorkingScope.noChildren
) extends WorkingScope {
  override def result: Result = ExpressionResult
  override def outgoing: RegularContext = RegularContext.unit
  def withAstNode(astNode: ASTNode): ExpressionScope = copy(astNode = astNode)
  override def withChildren(children: Seq[WorkingScope]): ExpressionScope = copy(children = children)
  override def withDeclared(declared: Declarations): ExpressionScope = copy(declared = declared)

  override def addReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): ExpressionScope =
    copy(referenced = referenced.union(References(references.toMap)))

  override def addHiddenReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): ExpressionScope =
    copy(referenced = referenced.addHidden(references.toMap))
}

case class PatternScope(
  astNode: ASTNode,
  patternIncoming: PatternIncomingContext,
  referenced: References,
  declared: Declarations,
  result: TableResult,
  children: Seq[WorkingScope] = WorkingScope.noChildren
) extends WorkingScope {
  private var _foreachIterVar: Option[LogicalVariable] = None
  override def foreachIterVar: Option[LogicalVariable] = _foreachIterVar

  override def incoming: RegularContext = patternIncoming.toRegularContext

  override def outgoing: RegularContext =
    RegularContext(unitVariables, result.columns.toSet, noLocalCallables)
  def withAstNode(astNode: ASTNode): PatternScope = copy(astNode = astNode)
  override def withChildren(children: Seq[WorkingScope]): PatternScope = copy(children = children)
  override def withDeclared(declared: Declarations): PatternScope = copy(declared = declared)

  override def addReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): PatternScope =
    copy(referenced = referenced.union(References(references.toMap)))

  override def addHiddenReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])]): PatternScope =
    copy(referenced = referenced.addHidden(references.toMap))

  override def tagForCache(
    inImportingWith: Boolean,
    foreachIterVar: Option[LogicalVariable]
  ): PatternScope = {
    val c = this.copy()
    c._foreachIterVar = foreachIterVar
    c
  }
}

object PatternScope {

  object PatternVariable {

    def unapply(elem: PatternAtom): Option[LogicalVariable] = elem match {
      case NodePattern(variable, _, _, _)               => variable
      case RelationshipPattern(variable, _, _, _, _, _) => variable
      case _                                            => None
    }
  }

  object Topo {

    def unapply(elem: PatternIncomingContext): Option[Set[LogicalVariable]] = Some(elem.topologicalConstants)
  }

  object Group {

    def unapply(elem: PatternIncomingContext): Option[Set[LogicalVariable]] = Some(elem.groupConstants)
  }
}

case class UnexpectedAstNodeScopingError(astNode: ASTNode, incoming: RegularContext) extends WorkingScope {
  override def referenced: References = References.empty
  override def declared: Declarations = Declarations.noDeclarations
  override def outgoing: RegularContext = incoming
  override def result: Result = NoResult
  override def children: Seq[WorkingScope] = WorkingScope.noChildren

  override def withChildren(children: Seq[WorkingScope]): UnexpectedAstNodeScopingError = this
  override def withDeclared(declared: Declarations): UnexpectedAstNodeScopingError = this

  override def addReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])])
    : UnexpectedAstNodeScopingError = this

  override def addHiddenReferences(references: Seq[(Ref[LogicalVariable], Ref[LogicalVariable])])
    : UnexpectedAstNodeScopingError = this
}

sealed trait Result {

  def getColumns: Seq[LogicalVariable] = this match {
    case TableResult(columns) => columns
    case _                    => Seq.empty
  }

  def isTableResult: Boolean = this match {
    case TableResult(_) => true
    case _              => false
  }

  def replaceVariables(replacements: Seq[LogicalVariable]): Result = this match {
    case TableResult(_) => TableResult(replacements)
    case _              => this
  }
}

case class TableResult(columns: Seq[LogicalVariable]) extends Result {

  def replaceVariables(removals: Seq[LogicalVariable], additions: Seq[LogicalVariable]): TableResult =
    copy(columns = columns.filter(!removals.contains(_)) ++ additions)
}
case object TableResultWithNotYetKnownColumns extends Result
case object OmittedResult extends Result
case object NoResult extends Result
case object ExpressionResult extends Result

case class Declarations(
  constants: Seq[LogicalVariable],
  variables: Seq[LogicalVariable],
  localCallables: Seq[LocalCallableScopeSignature] = Seq.empty
) {
  @inline def isEmpty: Boolean = isVariablesEmpty && isConstantsEmpty
  @inline def isConstantsEmpty: Boolean = constants.isEmpty
  @inline def isVariablesEmpty: Boolean = variables.isEmpty

  def allSymbols: Seq[LogicalVariable] = constants ++ variables

  def withoutAnonymousDeclaration: Declarations =
    copy(
      constants = constants.filter(c => SurveyorNameGenerator.named(c.name)),
      variables = variables.filter(v => SurveyorNameGenerator.named(v.name))
    )
}

object Declarations {

  def noDeclarations: Declarations =
    Declarations(Seq.empty[LogicalVariable], Seq.empty[LogicalVariable], Seq.empty[LocalCallableScopeSignature])

  def ofLocalCallable(localCallableScopeSignature: LocalCallableScopeSignature) =
    Declarations(Seq.empty[LogicalVariable], Seq.empty[LogicalVariable], Seq(localCallableScopeSignature))
}

/**
 * @param references the published channel — uses this scope exposes to its parent.
 * @param hidden     real uses kept OUT of the published channel but retained for whole-tree
 *                   resolution (see [[WorkingScope.hiddenReferences]]). A parent must never
 *                   aggregate a child's hidden entries, so [[publishedOnly]] strips this channel
 *                   before [[WorkingScope.referencedInChildren]] merges children upward.
 */
case class References(
  references: Map[Ref[LogicalVariable], Ref[LogicalVariable]],
  hidden: Map[Ref[LogicalVariable], Ref[LogicalVariable]] = Map.empty
) {

  def getDeclaration(variable: Ref[LogicalVariable]): Ref[LogicalVariable] = references.getOrElse(variable, variable)

  def getVariables: Seq[LogicalVariable] = references.keySet.toSeq.map(_.value)

  /** The hidden channel viewed as a `References`, for whole-tree resolution and the checkers. */
  def hiddenRefs: References = if (hidden.isEmpty) References.empty else References(hidden)

  /** Add real uses to the hidden channel, keeping them out of the published set. */
  def addHidden(those: Map[Ref[LogicalVariable], Ref[LogicalVariable]]): References =
    copy(hidden = hidden ++ those)

  /** This scope's references with the hidden channel cleared — what a parent is allowed to see. */
  def publishedOnly: References = if (hidden.isEmpty) this else copy(hidden = Map.empty)

  // TODO is this behavior we want to rely on further
  def hasSelfReference: Boolean = references.exists { case (reference, declaration) => reference == declaration }

  def getSelfReferences: Set[LogicalVariable] =
    references
      .view
      .collect { case (reference, declaration) if reference == declaration => reference.value }
      .toSet

  def union(that: References): References =
    References(references ++ that.references, hidden ++ that.hidden)

  def union(those: Seq[References]): References =
    References(
      those.foldLeft(references) { case (ref, ref2) => ref ++ ref2.references },
      those.foldLeft(hidden) { case (h, ref2) => h ++ ref2.hidden }
    )

  def intersect(those: Set[LogicalVariable]): References =
    copy(references = references.filter(those contains _._1.value))

  def intersectByTarget(those: Set[LogicalVariable]): References = {
    val targets: Set[(String, Int)] = those.iterator.map(v => (v.name, v.position.offset)).toSet
    copy(references = references.filter { case (_, decl) =>
      targets.contains((decl.value.name, decl.value.position.offset))
    })
  }

  /**
   * Filter the reference map by a predicate on each target (declaration). Returns a
   * `References` containing only those entries whose declaration satisfies `p`.
   */
  def filterTargets(p: LogicalVariable => Boolean): References =
    copy(references = references.filter { case (_, decl) => p(decl.value) })

  def diff(that: LogicalVariable): References =
    copy(references = references.filterNot(that == _._1.value))

  def diff(those: Set[LogicalVariable]): References =
    copy(references = references.filterNot(those contains _._1.value))

}

object References {

  def empty: References = References(Map.empty)

  def connect(caller: LogicalVariable, incoming: Set[LogicalVariable]): References =
    References(Map(Ref(caller) -> Ref(incoming.find(caller.equals).getOrElse(caller))))

  def connect(callers: Seq[LogicalVariable], incoming: Set[LogicalVariable]): References =
    References(Map.from(
      callers.map { caller =>
        val matched = incoming.find(caller.equals).getOrElse(caller)
        Ref(caller) -> Ref(matched)
      }
    ))

  def connectOrDrop(caller: LogicalVariable, incoming: Set[LogicalVariable]): References =
    References(incoming.find(caller.equals).map(matched => Ref(caller) -> Ref(matched)).toMap)

  def connectOrDrop(callers: Seq[LogicalVariable], incoming: Set[LogicalVariable]): References =
    References(callers.iterator.flatMap(c => incoming.find(c.equals).map(m => Ref(c) -> Ref(m))).toMap)

//  def intersectAndConnect(callers: Seq[LogicalVariable], incoming: Set[LogicalVariable]): References =
//    References(Map.from(
//      (callers filter incoming).map(caller =>
//        Ref(caller) -> Ref(incoming.find(caller.equals).getOrElse(caller))
//      )
//    ))

  def resolveByName(
    callers: Iterable[LogicalVariable],
    candidates: Iterable[LogicalVariable]
  ): Seq[(Ref[LogicalVariable], Ref[LogicalVariable])] = {
    val byName: Map[String, LogicalVariable] =
      candidates.groupBy(_.name).transform((_, vars) => vars.head)
    callers.flatMap(c => byName.get(c.name).map(dec => Ref(c) -> Ref(dec))).toSeq
  }
}
