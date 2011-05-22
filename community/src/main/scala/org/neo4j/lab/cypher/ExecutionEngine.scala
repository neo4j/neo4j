package org.neo4j.lab.cypher

import commands._
import pipes.{Pipe, FromPump}
import scala.collection.JavaConverters._
import org.neo4j.graphmatching.{PatternRelationship, CommonValueMatchers, PatternNode}
import org.neo4j.graphdb._

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {
  type MapTransformer = (Map[String, Any]) => Map[String, Any]

  def execute(query: Query): Projection = query match {
    case Query(select, start, matching, where) => {
      val patternKeeper = new PatternKeeper
      val sourcePump: Pipe = createSourcePumps(start).reduceLeft(_ ++ _)

      start.startItems.foreach( (item) => {
        item match {
          case relItem : RelationshipStartItem => patternKeeper.getOrCreateRelationship(item.placeholderName)
          case nodeItem : NodeStartItem => patternKeeper.getOrCreateNode(item.placeholderName)
        }
        patternKeeper.getOrCreateNode(item.placeholderName)
      })


      val projections = createProjectionTransformers(select)

      matching match {
        case Some(m) => m.patterns.foreach((p)=>{
          p match {
            case RelatedTo(left, right, relName, relationType, direction) => {
              val leftPattern =  patternKeeper.getOrCreateNode(left)
              val rightPattern = patternKeeper.getOrCreateNode(right)
              val rel: PatternRelationship = relationType match {
                case Some(relType) => leftPattern.createRelationshipTo(rightPattern, DynamicRelationshipType.withName(relType), direction)
                case None => leftPattern.createRelationshipTo(rightPattern, direction)
              }


              relName match {
                case None =>
                case Some(name) => patternKeeper.addRelationship(name, rel)
              }
            }
          }
        })
        case None =>
      }

      where match {
        case Some(w) => w.clauses.foreach((c) => {
          c match {
            case StringEquals(variable, propName, value) => {
              val patternPart = patternKeeper.getOrThrow(variable)
              patternPart.addPropertyConstraint(propName, CommonValueMatchers.exact(value) )
            }
          }
        })
        case None =>
      }

      new Projection(patternKeeper.nodesMap, patternKeeper.relationshipsMap, sourcePump, projections)
    }
  }

  def createProjectionTransformers(select: Select): Seq[MapTransformer] = select.selectItems.map((selectItem) => {
    selectItem match {
      case PropertyOutput(nodeName, propName) => nodePropertyOutput(nodeName, propName) _
      case EntityOutput(nodeName) => nodeOutput(nodeName) _
    }
  })

  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[Node]
    Map(column + "." + propName -> node.getProperty(propName))
  }

  private def createSourcePumps(from: Start): Seq[Pipe] = from.startItems.map(_ match {
    case NodeByIndex(varName, idxName, key, value) => {
      val indexHits: java.lang.Iterable[Node] = graph.index.forNodes(idxName).get(key, value)
      new FromPump(varName, indexHits.asScala)
    }
    case NodeById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getNodeById))
    case RelationshipById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getRelationshipById))
  })
}