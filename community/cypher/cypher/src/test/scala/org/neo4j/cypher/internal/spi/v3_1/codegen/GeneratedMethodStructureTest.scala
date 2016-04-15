package org.neo4j.cypher.internal.spi.v3_1.codegen

import java.util

import org.neo4j.codegen.source.{Configuration, SourceCode}
import org.neo4j.codegen.{CodeGenerationStrategy, CodeGenerator, MethodDeclaration}
import org.neo4j.cypher.internal.compiler.v3_1.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Provider, SuccessfulCloseable}
import org.neo4j.cypher.internal.compiler.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_1.{ExecutionMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_1.{symbols, SemanticDirection, SemanticTable}
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.core.NodeManager

/**
  * These are not test in the normal sense that they assert on some result,
  * they are merely testing that the constructed byte code is valid. The functionality
  * of the byte code are tested elsewhere.
  */
class GeneratedMethodStructureTest extends CypherFunSuite {

  import GeneratedQueryStructure.typeRef

  val modes = Seq(SourceCode.SOURCECODE, SourceCode.BYTECODE)
  val ops = Seq(
    Operation("create rel extractor", _.createRelExtractor("foo")),
    Operation("look up rel type", _.lookupRelationshipTypeId("foo", "bar")),
    Operation("all relationships for node", (m) => {
      m.declare("node", symbols.CTNode)
      m.nodeGetAllRelationships("foo", "node", SemanticDirection.OUTGOING)
    }),
    Operation("has label", m => {
      m.lookupLabelId("label", "A")
      m.declarePredicate("predVar")
      m.declare("node", symbols.CTNode)
      m.hasLabel("node", "label", "predVar")
    }),
    Operation("all relationships for node and types", (m) => {
      m.declare("node", symbols.CTNode)
      m.lookupRelationshipTypeId("a", "A")
      m.lookupRelationshipTypeId("b", "B")
      m.nodeGetRelationships("foo", "node", SemanticDirection.OUTGOING, Seq("a", "b"))
    }),
    Operation("next relationship", (m) => {
      m.createRelExtractor("r")
      m.declare("node", symbols.CTNode)
      m.nodeGetAllRelationships("foo", "node", SemanticDirection.OUTGOING)
      m.nextRelationshipAndNode("nextNode", "foo", SemanticDirection.OUTGOING, "node", "r")
    }),
    Operation("expand from all node", (m) => {
      m.createRelExtractor("r")
      m.allNodesScan("nodeIter")
      m.whileLoop(m.hasNextNode("nodeIter")) { b1 =>
        b1.nextNode("node", "nodeIter")
        b1.nodeGetAllRelationships("relIter", "node", SemanticDirection.OUTGOING)
        b1.whileLoop(b1.hasNextRelationship("relIter")) { b2 =>
          b2.nextRelationshipAndNode("nextNode", "relIter", SemanticDirection.OUTGOING, "node", "r")
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

  private def codeGenerator[E](block: GeneratedMethodStructure => Unit, mode: CodeGenerationStrategy[Configuration] ) = {
    val codeGen = CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, mode, SourceCode.PRINT_SOURCE)
    val packageName = "foo"
    implicit val context = new CodeGenContext(SemanticTable(), Map.empty)
    val clazz = using(codeGen.generateClass(packageName, "Test")) { body =>
      val fields = Fields(
        closer = body.field(typeRef[TaskCloser], "closer"),
        ro = body.field(typeRef[ReadOperations], "ro"),
        entityAccessor = body.field(typeRef[NodeManager], "nodeManager"),
        executionMode = body.field(typeRef[ExecutionMode], "executionMode"),
        description = body.field(typeRef[Provider[InternalPlanDescription]], "description"),
        tracer = body.field(typeRef[QueryExecutionTracer], "tracer"),
        params = body.field(typeRef[util.Map[String, Object]], "params"),
        closeable = body.field(typeRef[SuccessfulCloseable], "closeable"),
        success = body.generate(Templates.SUCCESS),
        close = body.generate(Templates.CLOSE))
      // the "COLUMNS" static field
      body.staticField(typeRef[util.List[String]], "COLUMNS", Templates.asList[String](Seq.empty))
      using(body.generate(MethodDeclaration.method(typeRef[Unit], "foo"))) { methodBody =>
        block(GeneratedMethodStructure(fields, methodBody, new AuxGenerator(packageName, codeGen)))
      }
      body.handle()
    }
    clazz.newInstance()
  }
}
