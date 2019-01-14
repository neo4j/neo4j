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
package org.neo4j.cypher.internal.compiled_runtime.spi.v3_4

import java.util

import org.neo4j.codegen.bytecode.ByteCode
import org.neo4j.codegen.source.SourceCode
import org.neo4j.codegen.{CodeGenerationStrategy, CodeGenerator, Expression, MethodDeclaration}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{CodeGenType, CypherCodeGenType, ReferenceType}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{Completable, Provider}
import org.neo4j.cypher.internal.frontend.v3_4.helpers._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.{ExecutionMode, QueryContext}
import org.neo4j.cypher.internal.spi.v3_4.codegen.GeneratedQueryStructure.typeRef
import org.neo4j.cypher.internal.spi.v3_4.codegen.{GeneratedMethodStructure, Methods, _}
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.{CursorFactory, NodeCursor, PropertyCursor, Read, _}
import org.neo4j.kernel.impl.core.EmbeddedProxySPI

/**
  * These are not test in the normal sense that they assert on some result,
  * they are merely testing that the constructed byte code is valid. The functionality
  * of the byte code are tested elsewhere.
  */
class GeneratedMethodStructureTest extends CypherFunSuite {

  val modes = Seq(SourceCode.SOURCECODE, ByteCode.BYTECODE)
  val ops = Seq(
        Operation("nullable object", m => {
          m.declareAndInitialize("foo", CodeGenType.Any)
          m.generator.assign(typeRef[Object], "bar",
                             m.nullablePrimitive("foo", CodeGenType.Any, Expression.constant("hello")))

        }),
        Operation("load node from parameters", m => {
          m.declareAndInitialize("a", CodeGenType.primitiveNode)
          m.generator.assign(typeRef[Long], "node", m.node("a", CodeGenType.primitiveNode))
        }),
        Operation("nullable node", m => {
          m.declareAndInitialize("foo", CodeGenType.primitiveNode)
          m.generator.assign(typeRef[Long], "bar",
                             m.nullablePrimitive("foo", CodeGenType.primitiveNode, m.loadVariable("foo")))

        }),
        Operation("mark variables as null", m => {
          m.declareFlag("flag", initialValue = false)
          m.updateFlag("flag", newValue = true)
          m.ifNotStatement(m.generator.load("flag")) { ifBody =>
            //mark variables as null
           ifBody.markAsNull("node", CodeGenType.primitiveNode)
           ifBody.markAsNull("object", CodeGenType.Any)
          }
        }),
        Operation("use a LongsToCount probe table", m => {
          m.declareAndInitialize("a", CodeGenType.primitiveNode)
          m.allocateProbeTable("table", LongsToCountTable)
          m.updateProbeTableCount("table", LongsToCountTable, Seq("a"))
          m.probe("table", LongsToCountTable, Seq("a")) { inner =>
            inner.allNodesScan("foo")
          }
        }),
       Operation("use a LongToCount key probe table", m => {
          m.declareAndInitialize("a", CodeGenType.primitiveNode)
          m.allocateProbeTable("table", LongToCountTable)
          m.updateProbeTableCount("table", LongToCountTable, Seq("a"))
          m.probe("table", LongToCountTable, Seq("a")) { inner =>
            inner.allNodesScan("foo")
          }
        }),
       Operation("use a LongToList probe table", m => {
         val table: LongToListTable =
           LongToListTable(SimpleTupleDescriptor(Map("a" -> CodeGenType.primitiveNode)),
                           localMap = Map("b" -> "a"))
         m.declareAndInitialize("a", CodeGenType.primitiveNode)
         m.allocateProbeTable("table", table)
         val value: Expression = m.newTableValue("value", table.tupleDescriptor)
         m.updateProbeTable(table.tupleDescriptor, "table", table, Seq("a"), value)
         m.probe("table", table, Seq("a")) { inner =>
           inner.allNodesScan("foo")
         }
       }),
       Operation("use a LongsToList probe table", m => {
         val table: LongsToListTable =
           LongsToListTable(SimpleTupleDescriptor(Map("a" -> CodeGenType.primitiveNode,
                                                      "b" -> CodeGenType.primitiveNode)),
                            localMap = Map("aa" -> "a", "bb" -> "b"))
         m.declareAndInitialize("a", CodeGenType.primitiveNode)
         m.declareAndInitialize("b", CodeGenType.primitiveNode)
         m.allocateProbeTable("table", table)
         val value: Expression = m.newTableValue("value", table.tupleDescriptor)
         m.updateProbeTable(table.tupleDescriptor, "table", table, Seq("a", "b"), value)
         m.probe("table", table, Seq("a", "b")) { inner =>
           inner.allNodesScan("foo")
         }
       }),
        Operation("Method invocation", m => {
          m.invokeMethod(LongToCountTable, "v1", "inner") { inner => {
            inner.allocateProbeTable("v1", LongToCountTable)
          }}
        }),
        Operation("look up rel type", _.lookupRelationshipTypeId("foo", "bar")),
        Operation("all relationships for node", (m) => {
          m.declareAndInitialize("node", CodeGenType.primitiveNode)
          m.nodeGetRelationshipsWithDirection("foo", "node", CodeGenType.primitiveInt, SemanticDirection.OUTGOING)
        }),
        Operation("has label", m => {
          m.lookupLabelId("label", "A")
          m.declarePredicate("predVar")
          m.declareAndInitialize("node", CodeGenType.primitiveNode)
          m.hasLabel("node", "label", "predVar")
        }),
        Operation("property by name for node", m => {
          m.lookupPropertyKey("prop", "prop")
          m.declareAndInitialize("node", CodeGenType.primitiveNode)
          m.declareProperty("propVar")
          m.nodeGetPropertyForVar("node", CodeGenType.primitiveNode, "prop", "propVar")
        }),
        Operation("property by id for node", m => {
          m.declareAndInitialize("node", CodeGenType.primitiveNode)
          m.declareProperty("propVar")
          m.nodeGetPropertyById("node", CodeGenType.primitiveNode, 13, "propVar")
        }),
        Operation("property by name for relationship", m => {
          m.lookupPropertyKey("prop", "prop")
          m.declareAndInitialize("rel", CodeGenType.primitiveRel)
          m.declareProperty("propVar")
          m.relationshipGetPropertyForVar("rel", CodeGenType.primitiveNode, "prop", "propVar")
        }),
        Operation("property by id for relationship", m => {
          m.declareAndInitialize("rel", CodeGenType.primitiveRel)
          m.declareProperty("propVar")
          m.nodeGetPropertyById("rel", CodeGenType.primitiveNode, 13, "propVar")
        }),
        Operation("rel type", m => {
          m.declareAndInitialize("foo", CypherCodeGenType(symbols.CTString, ReferenceType))
          m.declareAndInitialize("node", CodeGenType.primitiveNode)
          m.nodeGetRelationshipsWithDirection("barIter", "node", CodeGenType.primitiveInt, SemanticDirection.OUTGOING)
          m.relType("bar", "foo")
        }),
        Operation("all relationships for node and types", (m) => {
          m.declareAndInitialize("node", CodeGenType.primitiveNode)
          m.lookupRelationshipTypeId("a", "A")
          m.lookupRelationshipTypeId("b", "B")
          m.nodeGetRelationshipsWithDirectionAndTypes("foo", "node", CodeGenType.primitiveInt, SemanticDirection.OUTGOING, Seq("a", "b"))
        }),
        Operation("next relationship", (m) => {
          m.declareAndInitialize("node", CodeGenType.primitiveNode)
          m.nodeGetRelationshipsWithDirection("fooIter", "node", CodeGenType.primitiveInt, SemanticDirection.OUTGOING)
          m.nextRelationshipAndNode("nextNode", "fooIter", SemanticDirection.OUTGOING, "node", "foo")
        }),
    Operation("expand into", (m) => {
      m.declareAndInitialize("from", CodeGenType.primitiveNode)
      m.declareAndInitialize("to", CodeGenType.primitiveNode)
      val local = m.generator.declare(typeRef[RelationshipSelectionCursor], "iter")
      m.generator.assign(local, Expression.invoke(Methods.allConnectingRelationships,
                                             Expression.get(m.generator.self(), m.fields.dataRead),
                                             Expression.get(m.generator.self(), m.fields.cursors),
                                             Expression.get(m.generator.self(), m.fields.nodeCursor),
                                             m.generator.load("from"),
                                             Templates.outgoing,
                                             m.generator.load("to")))
    }),
    Operation("expand into with types", (m) => {
      m.declareAndInitialize("from", CodeGenType.primitiveNode)
      m.declareAndInitialize("to", CodeGenType.primitiveNode)
      val local = m.generator.declare(typeRef[RelationshipSelectionCursor], "iter")
      m.generator.assign(local, Expression.invoke(Methods.connectingRelationships,
                                                  Expression.get(m.generator.self(), m.fields.dataRead),
                                                  Expression.get(m.generator.self(), m.fields.cursors),
                                                  Expression.get(m.generator.self(), m.fields.nodeCursor),
                                                  m.generator.load("from"),
                                                  Templates.outgoing,
                                                  m.generator.load("to"),
                                             Expression.newArray(typeRef[Int], Expression.constant(1))))
    }),
    Operation("expand from all node", (m) => {
      m.allNodesScan("nodeIter")
      m.whileLoop(m.advanceNodeCursor("nodeIter")) { b1 =>
        b1.nodeFromNodeCursor("node", "nodeIter")
        b1.nodeGetRelationshipsWithDirection("relIter", "node", CodeGenType.primitiveInt, SemanticDirection.OUTGOING)
        b1.whileLoop(b1.advanceRelationshipSelectionCursor("relIter")) { b2 =>
          b2.nextRelationshipAndNode("nextNode", "relIter", SemanticDirection.OUTGOING, "node", "rel")
        }
      }
    }),
    Operation("all node scan", _.allNodesScan("foo"))
  )

  for {
    op <- ops
    mode <- modes
  } {
    test(s"${op.name} $mode") {
      codeGenerator(op.block, mode)
    }
  }

  case class Operation[E](name: String, block: GeneratedMethodStructure => Unit)

  private def codeGenerator[E](block: GeneratedMethodStructure => Unit, mode: CodeGenerationStrategy[_]) = {
    val codeGen = CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, mode)
    val packageName = "foo"
    implicit val context = new CodeGenContext(SemanticTable(), Map.empty)
    val clazz = using(codeGen.generateClass(packageName, "Test")) { body =>
      val fields = Fields(
        entityAccessor = body.field(typeRef[EmbeddedProxySPI], "proxySpi"),
        executionMode = body.field(typeRef[ExecutionMode], "executionMode"),
        description = body.field(typeRef[Provider[InternalPlanDescription]], "description"),
        tracer = body.field(typeRef[QueryExecutionTracer], "tracer"),
        params = body.field(typeRef[util.Map[String, Object]], "params"),
        closeable = body.field(typeRef[Completable], "closeable"),
        queryContext = body.field(typeRef[QueryContext], "queryContext"),
        cursors = body.field(typeRef[CursorFactory], "cursors"),
        nodeCursor = body.field(typeRef[NodeCursor], "nodeCursor"),
        relationshipScanCursor = body.field(typeRef[RelationshipScanCursor], "relationshipScanCursor"),
        propertyCursor = body.field(typeRef[PropertyCursor], "propertyCursor"),
        dataRead = body.field(typeRef[Read], "dataRead"),
        tokenRead = body.field(typeRef[TokenRead], "tokenRead"),
        schemaRead = body.field(typeRef[SchemaRead], "schemaRead")
      )
      // the "COLUMNS" static field
      body.staticField(typeRef[util.List[String]], "COLUMNS", Templates.asList[String](Seq.empty))
      using(body.generate(MethodDeclaration.method(typeRef[Unit], "foo"))) { methodBody =>
        block(new GeneratedMethodStructure(fields, methodBody, new AuxGenerator(packageName, codeGen)))
      }
      Templates.getOrLoadDataRead(body, fields)
      Templates.getOrLoadCursors(body, fields)
      Templates.getOrLoadTokenRead(body, fields)
      Templates.getOrLoadSchemaRead(body, fields)
      Templates.nodeCursor(body, fields)
      Templates.relationshipScanCursor(body, fields)
      Templates.propertyCursor(body, fields)
      body.handle()
    }
    clazz.newInstance()
  }
}
