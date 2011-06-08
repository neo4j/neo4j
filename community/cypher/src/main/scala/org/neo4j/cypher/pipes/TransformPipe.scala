package org.neo4j.cypher.pipes

import java.lang.String
import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.{NullablePropertyOutput, PropertyOutput, EntityOutput, ReturnItem}
import org.neo4j.graphdb.{NotFoundException, PropertyContainer}

/**
 * @author mh
 * @since 09.06.11
 */

class TransformPipe(val returnItems: Seq[ReturnItem], source: Pipe) extends Pipe {

  type MapTransformer = Map[String, Any] => Map[String, Any]

  def columnNames = returnItems.map(_.identifier).toList

  def createMapTransformers(returnItems: Seq[ReturnItem], symbolTable: SymbolTable): Seq[MapTransformer] = {

    returnItems.map((selectItem) => {
      selectItem match {
        case PropertyOutput(nodeName, propName) => {
          symbolTable.assertHas(nodeName)
          nodePropertyOutput(nodeName, propName) _
        }

        case NullablePropertyOutput(nodeName, propName) => {
          symbolTable.assertHas(nodeName)
          nullableNodePropertyOutput(nodeName, propName) _
        }

        case EntityOutput(nodeName) => {
          symbolTable.assertHas(nodeName)
          nodeOutput(nodeName) _
        }
      }
    })
  }

  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[PropertyContainer]
    Map(column + "." + propName -> node.getProperty(propName))
  }

  def nullableNodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[PropertyContainer]

    val property = try {
      node.getProperty(propName)
    } catch {
      case x: NotFoundException => null
    }

    Map(column + "." + propName -> property)
  }

  var transformers: Seq[MapTransformer] = null

  def prepare(symbolTable: SymbolTable) {
    transformers = createMapTransformers(returnItems, symbolTable)
    //returnItems.map(_.identifier).foreach((column) => symbolTable.registerColumn(column))
  }

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(row => {
      val projection = transformers.map(_(row)).reduceLeft(_ ++ _)
      f.apply(projection)
    })
  }
}

