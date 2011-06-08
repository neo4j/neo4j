package org.neo4j.cypher

import commands.{RelatedTo, Match}
import org.apache.commons.lang.NotImplementedException
import org.neo4j.graphdb._
import org.neo4j.graphmatching._

/**
 * @author mh
 * @since 08.06.11
 */

class PatternContext(val symbolTable: SymbolTable) {

  val group = new PatternGroup
  val nodes = scala.collection.mutable.Map[String, PatternNode]()
  val rels = scala.collection.mutable.Map[String, PatternRelationship]()

  symbolTable.identifiers.foreach((kv) => kv match {
    case (name: String, symbolType: SymbolType.NodeType) => nodes(name) = getOrCreateNode(name)
    case (name: String, symbolType: SymbolType.RelationshipType) => rels(name) = getOrCreateRelationship(name)
  })


  def createPatterns(matching: Option[Match]) {
    matching match {
      case Some(m) => m.patterns.foreach((pattern) => {
        pattern match {
          case RelatedTo(left, right, relName, relationType, direction) => createRelationshipPattern(left, right, relationType, direction, relName)
        }
      })
      case None =>
    }
  }

  def createRelationshipPattern(left: String, right: String, relationType: Option[String], direction: Direction, relName: Option[String]) {
    val leftPattern = getOrCreateNode(left)
    val rightPattern = getOrCreateNode(right)
    val rel = relationType match {
      case Some(relType) => leftPattern.createRelationshipTo(rightPattern, DynamicRelationshipType.withName(relType), direction)
      case None => leftPattern.createRelationshipTo(rightPattern, direction)
    }

    relName match {
      case None =>
      case Some(name) => {
        addRelationship(name, rel)
        rel.setLabel(name)
      }
    }
  }

  def getOrCreateNode(name: String): PatternNode = {
    if (rels.contains(name)) {
      throw new SyntaxError("Variable \"" + name + "\" already defined as a relationship.")
    }

    nodes.getOrElse(name, {
      val pNode = new PatternNode(group, name)
      nodes(name) = pNode
      symbolTable.registerNode(name)
      pNode
    })
  }


  def checkConnectednessOfPatternGraph {
    val identifiers = symbolTable.identifiers
    val visited = scala.collection.mutable.HashSet[String]()

    def visit(visitedObject: AbstractPatternObject[_ <: PropertyContainer]) {
      val label = visitedObject.getLabel
      if (label == null || !visited.contains(label)) {
        if (label != null) {
          visited.add(label)
        }

        visitedObject match {
          case node: PatternNode => node.getAllRelationships.asScala.foreach(visit)
          case rel: PatternRelationship => {
            visit(rel.getFirstNode)
            visit(rel.getSecondNode)
          }
        }

      }
    }

    identifiers.keys.map((item) => patternObject(item)).foreach(_ match {
      case None => throw new SyntaxError("Encountered a part of the pattern that is not part of the pattern. If you see this, please report this problem!")
      case Some(obj) => visit(obj)
    })

    val notVisitedParts = identifiers -- visited
    if (notVisitedParts.nonEmpty) {
      throw new SyntaxError("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These variables were found to be disconnected: " +
        notVisitedParts.mkString("", ", ", ""))
    }

  }

  def getOrCreateRelationship(name: String): PatternRelationship = {
    symbolTable.registerRelationship(name)
    throw new NotImplementedException("graph-matching doesn't support this yet. Revisit when it does.")
    //     if (nodes.contains(name))
    //       throw new SyntaxError(name + " already defined as a node")
    //
    //     rels.getOrElse(name, {
    //       val pRel = new PatternRelationship(name)
    //       rels(name) = pRel
    //       pRel
    //     })
  }

  def addRelationship(name: String, rel: PatternRelationship) {
    if (nodes.contains(name)) {
      throw new SyntaxError("Variable \"" + name + "\" already defined as a node.")
    }

    rels(name) = rel
  }

  def getOrThrow(name: String): AbstractPatternObject[_ <: PropertyContainer] = nodes.get(name) match {
    case Some(x) => x.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]]
    case None => rels.get(name) match {
      case Some(x) => x.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]]
      case None => throw new SyntaxError("No variable named " + name + " has been defined")
    }
  }

  def nodesMap: Map[String, PatternNode] = nodes.toMap

  def relationshipsMap: Map[String, PatternRelationship] = rels.toMap

  def assertHas(variable: String) {
    if (!(nodes.contains(variable) || rels.contains(variable))) {
      throw new SyntaxError("Unknown variable \"" + variable + "\".")
    }
  }

  def identifiers = nodes.keySet ++ rels.keySet

  type PatternType = AbstractPatternObject[_ <: PropertyContainer]

  def patternObject(key: String): Option[PatternType] = nodes.get(key) match {
    case Some(node) => Some(node.asInstanceOf[PatternType])
    case None => rels.get(key) match {
      case Some(rel) => Some(rel.asInstanceOf[PatternType])
      case None => None
    }
  }

  def bindStartPoint[U](startPoint: (String, Any)) {
    startPoint match {
      case (identifier: String, node: Node) => nodes(identifier).setAssociation(node)
      case (identifier: String, rel: Relationship) => rels(identifier).setAssociation(rel)
    }
  }

  def getPatternMatches(fromRow: Map[String, Any]): Iterable[Map[String,Any]] = {
    val startKey = fromRow.keys.head
    val startPNode = nodes(startKey)
    val startNode = fromRow(startKey).asInstanceOf[Node]
    val matches : Iterable[PatternMatch] = PatternMatcher.getMatcher.`match`(startPNode, startNode).asScala
    matches.map(patternMatch =>
      nodes.map((name,node) => name -> patternMatch.getNodeFor(node)) ++
      rels.map((name,rel) => name -> patternMatch.getRelationshipFor(rel)))
    )
  }

}