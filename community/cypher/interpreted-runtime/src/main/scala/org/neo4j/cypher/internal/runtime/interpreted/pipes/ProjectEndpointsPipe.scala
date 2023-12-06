/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.OptionAsClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.ScalaSeqAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.IsList.makeTraversable
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectEndpoints.EndNodes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectEndpoints.genTypeCheck
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectEndpoints.validateRel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectEndpoints.validateRelUndirectedNothingInScope
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectEndpoints.validateRels
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectEndpoints.validateRelsUndirectedNothingInScope
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.Read.NO_ID
import org.neo4j.internal.kernel.api.RelationshipDataAccessor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.util.CalledFromGeneratedCode
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

case class ProjectEndpointsPipe(
  source: Pipe,
  relName: String,
  start: String,
  startInScope: Boolean,
  end: String,
  endInScope: Boolean,
  relTypes: RelationshipTypes,
  direction: SemanticDirection,
  simpleLength: Boolean
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with ListSupport {
  type Projector = CypherRow => ClosingIterator[CypherRow]

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] =
    input.flatMap(projector(state))

  private def projector(state: QueryState): Projector =
    if (simpleLength) project(state) else projectVarLength(state)

  private def projectVarLength(state: QueryState)(row: CypherRow): ClosingIterator[CypherRow] = {
    val relsOrNull = row.getByName(relName)
    if (startInScope || endInScope || direction != SemanticDirection.BOTH) {
      validateRels(
        relsOrNull,
        direction,
        Option.when(startInScope)(nodeId(row.getByName(start))),
        Option.when(endInScope)(nodeId(row.getByName(end))),
        state.query,
        state.cursors.relationshipScanCursor,
        genTypeCheck(relTypes.types(state.query))
      ).map(setInRow(row, _)).asClosingIterator

    } else {
      validateRelsUndirectedNothingInScope(
        relsOrNull,
        state.query,
        state.cursors.relationshipScanCursor,
        genTypeCheck(relTypes.types(state.query))
      ) match {
        case Seq() =>
          ClosingIterator.empty
        case Seq(endNodes) =>
          ClosingIterator.single(setInRow(row, endNodes))
        case endNodesSeq =>
          endNodesSeq.map(setInRow(rowFactory.copyWith(row), _)).asClosingIterator
      }
    }
  }

  private def project(state: QueryState)(row: CypherRow): ClosingIterator[CypherRow] = {
    row.getByName(relName) match {
      case relValue: VirtualRelationshipValue =>
        if (direction != SemanticDirection.BOTH || startInScope || endInScope) {
          validateRel(
            relValue.id(),
            direction,
            Option.when(startInScope)(nodeId(row.getByName(start))),
            Option.when(endInScope)(nodeId(row.getByName(end))),
            state.query,
            state.cursors.relationshipScanCursor,
            genTypeCheck(relTypes.types(state.query))
          ).map(setInRow(row, _)).asClosingIterator
        } else {
          validateRelUndirectedNothingInScope(
            relValue.id(),
            state.query,
            state.cursors.relationshipScanCursor,
            genTypeCheck(relTypes.types(state.query))
          ) match {
            case Seq() =>
              ClosingIterator.empty
            case Seq(endNodes) =>
              ClosingIterator.single(setInRow(row, endNodes))
            case endNodesSeq =>
              endNodesSeq.map(setInRow(rowFactory.copyWith(row), _)).asClosingIterator
          }

        }
      case _ => ClosingIterator.empty
    }
  }

  private def setInRow(row: CypherRow, endNodes: EndNodes): CypherRow = {
    if (!startInScope) {
      row.set(start, VirtualValues.node(endNodes.left))
    }
    if (!endInScope) {
      row.set(end, VirtualValues.node(endNodes.right))
    }
    row
  }

  private def nodeId(
    node: AnyValue
  ): Long =
    node match {
      case n: VirtualNodeValue       => n.id()
      case x if x eq Values.NO_VALUE => StatementConstants.NO_SUCH_NODE
      case value => throw new CypherTypeException(s"Expected NodeValue but got ${value.getTypeName}")
    }

}

case object ProjectEndpoints {

  abstract class RelationshipScanCursorPredicate {
    def test(rc: RelationshipDataAccessor): Boolean
  }

  def genTypeCheck(typesToCheck: Array[Int]): RelationshipScanCursorPredicate = {

    if (typesToCheck == null) {
      (_: RelationshipDataAccessor) => true
    } else {
      (t: RelationshipDataAccessor) =>
        {
          typesToCheck.contains(t.`type`())
        }
    }
  }

  def validateRel(
    relId: Long,
    direction: SemanticDirection,
    startIfInScope: Option[Long],
    endIfInScope: Option[Long],
    dbAccess: DbAccess,
    scanCursor: RelationshipScanCursor,
    typeCheck: RelationshipScanCursorPredicate
  ): Option[EndNodes] = {
    dbAccess.singleRelationship(relId, scanCursor)
    if (scanCursor.next()) {
      validateRel(
        direction,
        startIfInScope,
        endIfInScope,
        scanCursor,
        typeCheck
      )
    } else {
      None
    }
  }

  /**
   * This method assumes that [[scanCursor]] is already pointing at the correct
   * relationship. This method is used in fused pipelines where previous operators already
   * have produced correctly positioned cursors
   */
  def validateRel(
    direction: SemanticDirection,
    startIfInScope: Option[Long],
    endIfInScope: Option[Long],
    scanCursor: RelationshipDataAccessor,
    typeCheck: RelationshipScanCursorPredicate
  ): Option[EndNodes] = {

    def matchScope(left: Long, right: Long): Option[EndNodes] = {
      if (
        startIfInScope.forall(_ == left) &&
        endIfInScope.forall(_ == right)
      ) {
        Some(EndNodes(left, right))
      } else {
        None
      }
    }

    if (!typeCheck.test(scanCursor)) {
      None
    } else {
      val source = scanCursor.sourceNodeReference()
      val target = scanCursor.targetNodeReference()

      direction match {
        case SemanticDirection.OUTGOING => matchScope(source, target)
        case SemanticDirection.INCOMING => matchScope(target, source)
        case SemanticDirection.BOTH     => matchScope(source, target).orElse(matchScope(target, source))
      }
    }
  }

  def validateRels(
    relsOrNull: AnyValue,
    direction: SemanticDirection,
    startIfInScope: Option[Long],
    endIfInScope: Option[Long],
    dbAccess: DbAccess,
    scanCursor: RelationshipScanCursor,
    typeCheck: RelationshipScanCursorPredicate
  ): Option[EndNodes] = {
    if (relsOrNull == Values.NO_VALUE) {
      return None
    }

    val rels = makeTraversable(relsOrNull)

    val (iterator, start, end, effectiveDirection, reversed) = (startIfInScope, endIfInScope) match {
      // If end is in scope but not start, reverse the order of iteration to fail fast
      case (None, Some(end)) =>
        (rels.reverse().iterator(), end, NO_ID, direction.reversed, true)
      case _ =>
        (rels.iterator(), startIfInScope.getOrElse(NO_ID), endIfInScope.getOrElse(NO_ID), direction, false)
    }

    // Check that the path starts with startNode (if startNode is in scope)
    var prevNode = start
    var firstNode: Long = start

    while (iterator.hasNext) {
      val next: VirtualRelationshipValue = iterator.next() match {
        case relValue: VirtualRelationshipValue => relValue
        case _                                  => return None
      }
      dbAccess.singleRelationship(next.id(), scanCursor)
      if (!scanCursor.next()) {
        return None
      }
      if (!typeCheck.test(scanCursor)) {
        return None
      }

      val source = scanCursor.sourceNodeReference()
      val target = scanCursor.targetNodeReference()

      effectiveDirection match {

        case SemanticDirection.OUTGOING =>
          if (prevNode != NO_ID && prevNode != source) {
            return None
          }
          if (firstNode == NO_ID) {
            firstNode = source
          }
          prevNode = target

        case SemanticDirection.INCOMING =>
          if (prevNode != NO_ID && prevNode != target) {
            return None
          }
          if (firstNode == NO_ID) {
            firstNode = target
          }
          prevNode = source

        case SemanticDirection.BOTH =>
          // As something is in scope when we have BOTH here, and we iterate in reverse if it's the end node,
          // we know that prevNode and firstNode != NO_ID
          prevNode = prevNode match {
            case `source` => target
            case `target` => source
            case _ =>
              return None
          }
      }
    }

    // Check that the path ends with endNode (if endNode is in scope)
    if (end == NO_ID || end == prevNode) {
      if (!reversed) {
        Some(EndNodes(firstNode, prevNode))
      } else {
        // re-reverse start and end
        Some(EndNodes(prevNode, firstNode))
      }
    } else {
      None
    }
  }

  def validateRelUndirectedNothingInScope(
    relId: Long,
    dbAccess: DbAccess,
    scanCursor: RelationshipScanCursor,
    typeCheck: RelationshipScanCursorPredicate
  ): Seq[EndNodes] = {
    dbAccess.singleRelationship(relId, scanCursor)
    if (scanCursor.next()) {
      validateRelUndirectedNothingInScope(
        scanCursor,
        typeCheck
      )
    } else {
      Seq.empty
    }
  }

  /**
   * This method assumes that [[scanCursor]] is already pointing at the correct
   * relationship. This method is used in fused pipelines where previous operators already
   * have produced correctly positioned cursors
   */
  def validateRelUndirectedNothingInScope(
    scanCursor: RelationshipDataAccessor,
    typeCheck: RelationshipScanCursorPredicate
  ): Seq[EndNodes] = {

    if (scanCursor.reference() == NO_ID || !typeCheck.test(scanCursor)) {
      Seq.empty
    } else {
      val source = scanCursor.sourceNodeReference()
      val target = scanCursor.targetNodeReference()
      if (source != target) {
        Seq(
          EndNodes(source, target),
          EndNodes(target, source)
        )
      } else {
        Seq(
          EndNodes(source, target)
        )
      }
    }
  }

  def validateRelsUndirectedNothingInScope(
    relsOrNull: AnyValue,
    dbAccess: DbAccess,
    scanCursor: RelationshipScanCursor,
    typeCheck: RelationshipScanCursorPredicate
  ): Seq[EndNodes] = {
    if (relsOrNull == Values.NO_VALUE) {
      return Seq.empty
    }

    val rels = makeTraversable(relsOrNull)

    val iterator = rels.iterator()

    val DISQUALIFIED = -2L
    var prevNode1 = NO_ID
    var prevNode2 = NO_ID

    var start1 = NO_ID
    var start2 = NO_ID

    while (iterator.hasNext) {
      val next: VirtualRelationshipValue = iterator.next() match {
        case relValue: VirtualRelationshipValue => relValue
        case _                                  => return Seq.empty
      }
      dbAccess.singleRelationship(next.id(), scanCursor)
      if (!scanCursor.next()) {
        return Seq.empty
      }
      if (!typeCheck.test(scanCursor)) {
        return Seq.empty
      }

      val source = scanCursor.sourceNodeReference()
      val target = scanCursor.targetNodeReference()

      if (prevNode1 == NO_ID) {
        start1 = source
        start2 = target
        prevNode1 = target
        prevNode2 = source
      } else {
        prevNode1 = prevNode1 match {
          case `source` => target
          case `target` => source
          case _        => DISQUALIFIED
        }
        prevNode2 = prevNode2 match {
          case `source` => target
          case `target` => source
          case _        => DISQUALIFIED
        }
      }

      if (prevNode1 == DISQUALIFIED && prevNode2 == DISQUALIFIED) {
        return Seq.empty
      }
    }

    // Check that the path ends with endNode (if endNode is in scope)
    (prevNode1, prevNode2) match {
      case (end1, `DISQUALIFIED`) => Seq(EndNodes(start1, end1))
      case (`DISQUALIFIED`, end2) => Seq(EndNodes(start2, end2))
      case (end1, end2) => Seq(
          EndNodes(start1, end1),
          EndNodes(start2, end2)
        )
    }
  }

  @CalledFromGeneratedCode
  def validateRelOrNull(
    relId: Long,
    direction: SemanticDirection,
    startIfInScope: Option[Long],
    endIfInScope: Option[Long],
    dbAccess: DbAccess,
    scanCursor: RelationshipScanCursor,
    typeCheck: RelationshipScanCursorPredicate
  ): EndNodes = {
    validateRel(relId, direction, startIfInScope, endIfInScope, dbAccess, scanCursor, typeCheck).orNull
  }

  @CalledFromGeneratedCode
  def validateRelOrNull(
    relId: Long,
    direction: SemanticDirection,
    startIfInScope: Option[Long],
    endIfInScope: Option[Long],
    scanCursor: RelationshipDataAccessor,
    typeCheck: RelationshipScanCursorPredicate
  ): EndNodes = {
    if (relId == NO_ID) null
    else validateRel(direction, startIfInScope, endIfInScope, scanCursor, typeCheck).orNull
  }

  @CalledFromGeneratedCode
  def validateRelsOrNull(
    rels: AnyValue,
    direction: SemanticDirection,
    startIfInScope: Option[Long],
    endIfInScope: Option[Long],
    dbAccess: DbAccess,
    scanCursor: RelationshipScanCursor,
    typeCheck: RelationshipScanCursorPredicate
  ): EndNodes = {
    validateRels(
      rels,
      direction,
      startIfInScope,
      endIfInScope,
      dbAccess,
      scanCursor,
      typeCheck
    ).orNull
  }

  case class EndNodes(left: Long, right: Long)
}
