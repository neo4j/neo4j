/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

abstract class ElementProperty(token: Option[Int], propName: String, elementIdVar: String, propKeyVar: String)
  extends CodeGenExpression {
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    if (token.isEmpty) generator.lookupPropertyKey(propName, propKeyVar)

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E = {
    val localName = context.namer.newVarName()
    structure.declareProperty(localName)
    if (token.isEmpty)
      propertyByName(structure, localName)
    else
      propertyById(structure, localName)
    structure.incrementDbHits()
    structure.loadVariable(localName)
  }

  def propertyByName[E](body: MethodStructure[E], localName: String): Unit

  def propertyById[E](body: MethodStructure[E], localName: String): Unit

  override def nullable(implicit context: CodeGenContext) = true
}

case class NodeProperty(token: Option[Int], propName: String, nodeIdVar: Variable, propKeyVar: String)
  extends ElementProperty(token, propName, nodeIdVar.name, propKeyVar) {

  override def propertyByName[E](body: MethodStructure[E], localName: String) =
    if (nodeIdVar.nullable)
      body.ifNotStatement(body.isNull(nodeIdVar.name, nodeIdVar.codeGenType)) {ifBody =>
        ifBody.nodeGetPropertyForVar(nodeIdVar.name, nodeIdVar.codeGenType, propKeyVar, localName)
      }
    else
      body.nodeGetPropertyForVar(nodeIdVar.name, nodeIdVar.codeGenType, propKeyVar, localName)

  override def propertyById[E](body: MethodStructure[E], localName: String) =
    if (nodeIdVar.nullable)
      body.ifNotStatement(body.isNull(nodeIdVar.name, nodeIdVar.codeGenType)) {ifBody =>
        ifBody.nodeGetPropertyById(nodeIdVar.name, nodeIdVar.codeGenType, token.get, localName)
      }
    else
      body.nodeGetPropertyById(nodeIdVar.name, nodeIdVar.codeGenType, token.get, localName)

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType.Value
}

case class RelProperty(token: Option[Int], propName: String, relIdVar: Variable, propKeyVar: String)
  extends ElementProperty(token, propName, relIdVar.name, propKeyVar) {

  override def propertyByName[E](body: MethodStructure[E], localName: String) =
    if (relIdVar.nullable)
      body.ifNotStatement(body.isNull(relIdVar.name, CodeGenType.primitiveRel)) { ifBody =>
        ifBody.relationshipGetPropertyForVar(relIdVar.name, relIdVar.codeGenType, propKeyVar, localName)
      }
    else
      body.relationshipGetPropertyForVar(relIdVar.name, relIdVar.codeGenType, propKeyVar, localName)

  override def propertyById[E](body: MethodStructure[E], localName: String) =
  if (relIdVar.nullable)
    body.ifNotStatement(body.isNull(relIdVar.name, CodeGenType.primitiveRel)) { ifBody =>
      ifBody.relationshipGetPropertyById(relIdVar.name, relIdVar.codeGenType, token.get, localName)
    }
    else
      body.relationshipGetPropertyById(relIdVar.name, relIdVar.codeGenType, token.get, localName)

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType.Value
}
