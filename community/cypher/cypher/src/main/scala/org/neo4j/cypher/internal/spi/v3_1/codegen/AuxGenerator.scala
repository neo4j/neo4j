package org.neo4j.cypher.internal.spi.v3_1.codegen

import org.neo4j.codegen.{CodeGenerator, TypeReference}
import org.neo4j.cypher.internal.compiler.v3_1.helpers._
import org.neo4j.cypher.internal.frontend.v3_1.symbols.CypherType

import scala.collection.mutable

class AuxGenerator(val packageName: String, val generator: CodeGenerator) {

  import GeneratedQueryStructure.lowerType

  private val types: scala.collection.mutable.Map[Map[String, CypherType], TypeReference] = mutable.Map.empty
  private var nameId = 0


  def typeReference(structure: Map[String, CypherType]): TypeReference = {
    types.getOrElseUpdate(structure, using(generator.generateClass(packageName, newName())) { clazz =>
      structure.foreach {
        case (fieldName, fieldType: CypherType) => clazz.field(lowerType(fieldType), fieldName)
      }
      clazz.handle()
    })
  }

  private def newName() = {
    val name = "ValueType" + nameId
    nameId += 1
    name
  }
}
