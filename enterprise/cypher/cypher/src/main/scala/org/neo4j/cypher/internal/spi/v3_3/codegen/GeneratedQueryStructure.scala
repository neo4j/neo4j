/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.spi.v3_3.codegen

import java.lang.reflect.Modifier
import java.util.stream.{DoubleStream, IntStream, LongStream}

import org.neo4j.codegen.Expression.{constant, invoke, newArray, newInstance}
import org.neo4j.codegen.MethodReference.constructorReference
import org.neo4j.codegen.TypeReference._
import org.neo4j.codegen.bytecode.ByteCode.{BYTECODE, VERIFY_GENERATED_BYTECODE}
import org.neo4j.codegen.source.SourceCode.SOURCECODE
import org.neo4j.codegen.source.{SourceCode, SourceVisitor}
import org.neo4j.codegen.{CodeGenerator, Parameter, _}
import org.neo4j.cypher.internal.codegen.{PrimitiveNodeStream, PrimitiveRelationshipStream}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.spi.{CodeStructure, CodeStructureResult, MethodStructure}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.{Completable, Provider}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionMode, TaskCloser}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.cypher.internal.frontend.v3_3.helpers.using
import org.neo4j.cypher.internal.frontend.v3_3.symbols
import org.neo4j.cypher.internal.javacompat.ResultRecord
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.v3_3.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_3.executionplan.{GeneratedQuery, GeneratedQueryExecution}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

object GeneratedQueryStructure extends CodeStructure[GeneratedQuery] {

  case class GeneratedQueryStructureResult(query: GeneratedQuery, source: Seq[(String, String)], bytecode: Seq[(String, String)])
    extends CodeStructureResult[GeneratedQuery]

  private def createGenerator(conf: CodeGenConfiguration, code: CodeSaver) = {
    val mode = conf.mode match {
      case SourceCodeMode => SOURCECODE
      case ByteCodeMode => BYTECODE
    }
    val options = mutable.ListBuffer.empty[CodeGeneratorOption]
    if(conf.showSource) {
      options += code.saveSourceCode
    }
    if(conf.showByteCode) {
      options += code.saveByteCode
    }
    if(getClass.desiredAssertionStatus()) {
      options += VERIFY_GENERATED_BYTECODE
    }
    conf.saveSource.foreach(path => {
      options += SourceCode.sourceLocation(path)
    })

    CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, mode, options:_*)
  }

  class CodeSaver {
    private var _source: Seq[(String, String)] = Seq.empty
    private var _bytecode: Seq[(String, String)] = Seq.empty

    def saveSourceCode = new SourceVisitor {
      override protected def visitSource(reference: TypeReference, sourceCode: CharSequence): Unit =
        _source = _source :+ (reference.name(), sourceCode.toString)
    }

    def saveByteCode = new DisassemblyVisitor {
      override protected def visitDisassembly(className: String, disassembly: CharSequence): Unit =
        _bytecode = _bytecode :+ (className, disassembly.toString)
    }

    def sourceCode: Seq[(String, String)] = _source
    def bytecode: Seq[(String, String)] = _bytecode
  }

  override def generateQuery(className: String,
                             columns: Seq[String],
                             operatorIds: Map[String, LogicalPlanId],
                             conf: CodeGenConfiguration)
                            (methodStructure: MethodStructure[_] => Unit)
                            (implicit codeGenContext: CodeGenContext): GeneratedQueryStructureResult = {

    val sourceSaver = new CodeSaver
    val generator = createGenerator(conf, sourceSaver)
    val execution = using(
      generator.generateClass(conf.packageName, className + "Execution", typeRef[GeneratedQueryExecution])) { clazz =>
      val fields: Fields = createFields(columns, clazz)
      setOperatorIds(clazz, operatorIds)
      addSimpleMethods(clazz, fields)
      addAccept(methodStructure, generator, clazz, fields, conf)
      clazz.handle()
    }
    val query = using(generator.generateClass(conf.packageName, className, typeRef[GeneratedQuery])) { clazz =>
      using(clazz.generateMethod(typeRef[GeneratedQueryExecution], "execute",
        param[TaskCloser]("closer"),
        param[QueryContext]("queryContext"),
        param[ExecutionMode]("executionMode"),
        param[Provider[InternalPlanDescription]]("description"),
        param[QueryExecutionTracer]("tracer"),
        param[MapValue]("params"))) { execute =>
        execute.returns(
          invoke(
            newInstance(execution),
            constructorReference(execution,
              typeRef[TaskCloser],
              typeRef[QueryContext],
              typeRef[ExecutionMode],
              typeRef[Provider[InternalPlanDescription]],
              typeRef[QueryExecutionTracer],
              typeRef[MapValue]),
            execute.load("closer"),
            execute.load("queryContext"),
            execute.load("executionMode"),
            execute.load("description"),
            execute.load("tracer"),
            execute.load("params")))
      }
      clazz.handle()
    }.newInstance().asInstanceOf[GeneratedQuery]

    val clazz: Class[_] = execution.loadClass()
    operatorIds.foreach {
      case (key, id) =>
        val anyRefId = id.asInstanceOf[AnyRef]
        setStaticField(clazz, key, anyRefId)
    }
    GeneratedQueryStructureResult(query, sourceSaver.sourceCode, sourceSaver.bytecode)
  }

  private def addAccept(methodStructure: (MethodStructure[_]) => Unit,
                        generator: CodeGenerator,
                        clazz: ClassGenerator,
                        fields: Fields,
                        conf: CodeGenConfiguration)(implicit codeGenContext: CodeGenContext) = {
    using(clazz.generate(MethodDeclaration.method(typeRef[Unit], "accept",
      Parameter.param(parameterizedType(classOf[QueryResultVisitor[_]],
        typeParameter("E")), "visitor")).
      parameterizedWith("E", extending(typeRef[Exception])).
      throwsException(typeParameter("E")))) { (codeBlock: CodeBlock) =>
      val structure = new GeneratedMethodStructure(fields, codeBlock, new AuxGenerator(conf.packageName, generator), onClose =
        Seq((success: Boolean) => (block: CodeBlock) => {
          val target = Expression.get(block.self(), fields.closeable)
          val reference = method[Completable, Unit]("completed", typeRef[Boolean])
          val constant1 = Expression.constant(success)
          val invoke1 = invoke(target, reference, constant1)
          block.expression(invoke1)
        }))
      codeBlock.assign(typeRef[ResultRecord], "row",
                       invoke(newInstance(typeRef[ResultRecord]),
                              MethodReference.constructorReference(typeRef[ResultRecord], typeRef[Int]),
                              constant(codeGenContext.numberOfColumns()))
                      )
      methodStructure(structure)
      structure.finalizers.foreach(_ (true)(codeBlock))
    }
  }

  private def addSimpleMethods(clazz: ClassGenerator, fields: Fields) = {
    clazz.generate(Templates.constructor(clazz.handle()))
    Templates.getOrLoadReadOperations(clazz, fields)
    clazz.generate(Templates.setCompletable(clazz.handle()))
    clazz.generate(Templates.executionMode(clazz.handle()))
    clazz.generate(Templates.executionPlanDescription(clazz.handle()))
    clazz.generate(Templates.FIELD_NAMES)
  }

  private def setOperatorIds(clazz: ClassGenerator, operatorIds: Map[String, LogicalPlanId]) = {
    operatorIds.keys.foreach { opId =>
      clazz.staticField(typeRef[LogicalPlanId], opId)
    }
  }

  private def createFields(columns: Seq[String], clazz: ClassGenerator) = {
    clazz.staticField(TypeReference.typeReference(classOf[Array[String]]),
                      "COLUMNS", newArray(typeRef[String], columns.map(key => constant(key)):_*))

    Fields(
      closer = clazz.field(typeRef[TaskCloser], "closer"),
      ro = clazz.field(typeRef[ReadOperations], "ro"),
      entityAccessor = clazz.field(typeRef[NodeManager], "nodeManager"),
      executionMode = clazz.field(typeRef[ExecutionMode], "executionMode"),
      description = clazz.field(typeRef[Provider[InternalPlanDescription]], "description"),
      tracer = clazz.field(typeRef[QueryExecutionTracer], "tracer"),
      params = clazz.field(typeRef[MapValue], "params"),
      closeable = clazz.field(typeRef[Completable], "closeable"),
      queryContext = clazz.field(typeRef[QueryContext], "queryContext"),
      skip = clazz.field(typeRef[Boolean], "skip"))
  }

  def method[O <: AnyRef, R](name: String, params: TypeReference*)
                            (implicit owner: Manifest[O], returns: Manifest[R]): MethodReference =
    MethodReference.methodReference(typeReference(owner), typeReference(returns), name, Modifier.PUBLIC, params: _*)

  def staticField[O <: AnyRef, R](name: String)(implicit owner: Manifest[O], fieldType: Manifest[R]): FieldReference =
    FieldReference.staticField(typeReference(owner), typeReference(fieldType), name)

  def param[T <: AnyRef](name: String)(implicit manifest: Manifest[T]): Parameter =
    Parameter.param(typeReference(manifest), name)

  def typeRef[T](implicit manifest: Manifest[T]): TypeReference = typeReference(manifest)

  def typeReference(manifest: Manifest[_]): TypeReference = {
    val arguments = manifest.typeArguments
    val base = TypeReference.typeReference(manifest.runtimeClass)
    if (arguments.nonEmpty) {
      TypeReference.parameterizedType(base, arguments.map(typeReference): _*)
    } else {
      base
    }
  }

  def lowerType(cType: CodeGenType): TypeReference = cType match {
    case CypherCodeGenType(symbols.CTNode, LongType) => typeRef[Long]
    case CypherCodeGenType(symbols.CTRelationship, LongType) => typeRef[Long]
    case CypherCodeGenType(symbols.CTInteger, LongType) => typeRef[Long]
    case CypherCodeGenType(symbols.CTFloat, FloatType) => typeRef[Double]
    case CypherCodeGenType(symbols.CTBoolean, BoolType) => typeRef[Boolean]
    case CypherCodeGenType(symbols.ListType(symbols.CTNode), ListReferenceType(LongType)) => typeRef[PrimitiveNodeStream]
    case CypherCodeGenType(symbols.ListType(symbols.CTRelationship), ListReferenceType(LongType)) => typeRef[PrimitiveRelationshipStream]
    case CypherCodeGenType(symbols.ListType(_), ListReferenceType(LongType)) => typeRef[LongStream]
    case CypherCodeGenType(symbols.ListType(_), ListReferenceType(FloatType)) => typeRef[DoubleStream]
    case CypherCodeGenType(symbols.ListType(_), ListReferenceType(BoolType)) => typeRef[IntStream]
    case CodeGenType.javaInt => typeRef[Int]
    case _ => typeRef[Object]
  }

  def nullValue(cType: CodeGenType): Expression = cType match {
    case CypherCodeGenType(symbols.CTNode, LongType) => constant(-1L)
    case CypherCodeGenType(symbols.CTRelationship, LongType) => constant(-1L)
    case _ => constant(null)
  }
}
