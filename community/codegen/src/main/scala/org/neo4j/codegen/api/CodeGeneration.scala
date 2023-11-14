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
package org.neo4j.codegen.api

import org.neo4j.codegen
import org.neo4j.codegen.ClassHandle
import org.neo4j.codegen.CodeGenerationNotSupportedException
import org.neo4j.codegen.CodeGenerator
import org.neo4j.codegen.CodeGenerator.generateCode
import org.neo4j.codegen.CodeGeneratorOption
import org.neo4j.codegen.DisassemblyVisitor
import org.neo4j.codegen.Expression
import org.neo4j.codegen.Expression.constant
import org.neo4j.codegen.Expression.getStatic
import org.neo4j.codegen.Expression.invoke
import org.neo4j.codegen.Expression.invokeSuper
import org.neo4j.codegen.Expression.newInitializedArray
import org.neo4j.codegen.FieldReference
import org.neo4j.codegen.FieldReference.field
import org.neo4j.codegen.FieldReference.staticField
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.TypeReference.OBJECT
import org.neo4j.codegen.api.CodeGeneration.ByteCodeGeneration
import org.neo4j.codegen.api.CodeGeneration.CodeGenerationMode
import org.neo4j.codegen.api.CodeGeneration.CodeGenerationMode.modeFromDebugOptions
import org.neo4j.codegen.api.CodeGeneration.DEBUG_PRINT_BYTECODE
import org.neo4j.codegen.api.CodeGeneration.DEBUG_PRINT_SOURCE
import org.neo4j.codegen.api.CodeGeneration.SourceCodeGeneration
import org.neo4j.codegen.api.SizeEstimation.estimateByteCodeSize
import org.neo4j.codegen.bytecode.ByteCode.BYTECODE
import org.neo4j.codegen.bytecode.ByteCode.PRINT_BYTECODE
import org.neo4j.codegen.source.SourceCode.PRINT_SOURCE
import org.neo4j.codegen.source.SourceCode.SOURCECODE
import org.neo4j.codegen.source.SourceCode.sourceLocation
import org.neo4j.codegen.source.SourceVisitor
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.exceptions.CantCompileQueryException

import java.nio.file.Path
import java.nio.file.Paths

import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

/**
 * Produces runnable code from an IntermediateRepresentation
 */
object CodeGeneration {

  // the jvm doesn't allow methods bigger than 65535 bytes,
  final val MAX_METHOD_LIMIT: Int = 65535

  // Use these options for Debugging. They will print generated code to stdout
  private val DEBUG_PRINT_SOURCE = false
  private val DEBUG_PRINT_BYTECODE = false

  final val GENERATE_JAVA_SOURCE_DEBUG_OPTION = CypherDebugOption.generateJavaSource.name
  final val GENERATED_SOURCE_LOCATION_PROPERTY = "org.neo4j.cypher.DEBUG.generated_source_location"

  def fromDebugOptions(methodLimit: Int = MAX_METHOD_LIMIT, debugOptions: CypherDebugOptions): CodeGeneration =
    codeGeneration(methodLimit, modeFromDebugOptions(debugOptions))

  def codeGeneration(
    methodLimit: Int = MAX_METHOD_LIMIT,
    mode: CodeGenerationMode =
      ByteCodeGeneration(new CodeSaver(false, false))
  ): CodeGeneration = new CodeGeneration(methodLimit, mode)

  sealed trait CodeGenerationMode {
    def saver: CodeSaver
  }

  case class ByteCodeGeneration(saver: CodeSaver) extends CodeGenerationMode

  case class SourceCodeGeneration(saver: CodeSaver) extends CodeGenerationMode

  object CodeGenerationMode {

    def modeFromDebugOptions(debugOptions: CypherDebugOptions): CodeGenerationMode = {
      if (debugOptions.generateJavaSourceEnabled) {
        val saveSourceToFileLocation = Option(System.getProperty(GENERATED_SOURCE_LOCATION_PROPERTY)).map(Paths.get(_))
        val saver =
          new CodeSaver(debugOptions.showJavaSourceEnabled, debugOptions.showBytecodeEnabled, saveSourceToFileLocation)
        SourceCodeGeneration(saver)
      } else {
        val saver = new CodeSaver(false, debugOptions.showBytecodeEnabled)
        ByteCodeGeneration(saver)
      }
    }
  }

  class CodeSaver(saveSource: Boolean, saveByteCode: Boolean, saveSourceToFileLocation: Option[Path] = None) {
    private val _source: ArrayBuffer[(String, String)] = new ArrayBuffer()
    private val _bytecode: ArrayBuffer[(String, String)] = new ArrayBuffer()

    private def sourceVisitor: SourceVisitor =
      (reference: TypeReference, sourceCode: CharSequence) => _source += (reference.name() -> sourceCode.toString)

    private def byteCodeVisitor: DisassemblyVisitor =
      (className: String, disassembly: CharSequence) => _bytecode += (className -> disassembly.toString)

    def options: List[CodeGeneratorOption] = {
      var l: List[CodeGeneratorOption] = Nil
      if (saveSource) l ::= sourceVisitor
      if (saveByteCode) l ::= byteCodeVisitor
      saveSourceToFileLocation.foreach(path => l ::= sourceLocation(path))
      l
    }

    def sourceCode: Seq[(String, String)] = _source.toSeq

    def bytecode: Seq[(String, String)] = _bytecode.toSeq
  }
}

class CodeGeneration(methodLimit: Int, val codeGenerationMode: CodeGenerationMode) {

  def createGenerator(): CodeGenerator = {
    var (strategy, options) = (codeGenerationMode, DEBUG_PRINT_SOURCE) match {
      case (SourceCodeGeneration(saver), _)   => (SOURCECODE, saver.options)
      case (ByteCodeGeneration(saver), true)  => (SOURCECODE, saver.options)
      case (ByteCodeGeneration(saver), false) => (BYTECODE, saver.options)
    }
    if (DEBUG_PRINT_SOURCE) options ::= PRINT_SOURCE
    if (DEBUG_PRINT_BYTECODE) options ::= PRINT_BYTECODE

    generateCode(classOf[IntermediateRepresentation].getClassLoader, strategy, options: _*)
  }

  def compileClass[T](c: ClassDeclaration[T], generator: CodeGenerator): ClassHandle = {
    compileClassDeclaration(c, generator)
  }

  def loadAndSetConstants[T](handle: ClassHandle, declaration: ClassDeclaration[T]): Class[T] = {
    val clazz = handle.loadClass()
    setConstants(clazz, declaration.fields)
    clazz.asInstanceOf[Class[T]]
  }

  def compileAnonymousClass[T](c: ClassDeclaration[T], generator: CodeGenerator): Class[T] = {
    val handle = compileClassDeclaration(c, generator)
    val clazz = handle.loadAnonymousClass()
    setConstants(clazz, c.fields)
    clazz.asInstanceOf[Class[T]]
  }

  private def setConstants(clazz: Class[_], fields: collection.Seq[Field]): Unit = {
    if (fields.isEmpty) {
      return
    }
    val declaredFields = clazz.getDeclaredFields

    @nowarn("msg=return statement")
    def findField(fields: Array[java.lang.reflect.Field], name: String): java.lang.reflect.Field = {
      for (field <- fields) {
        if (field.getName == name) return field
      }
      throw new NoSuchFieldException(name)
    }

    fields.distinct.foreach {
      case StaticField(_, name, Some(value)) =>
        findField(declaredFields, name).set(null, value)
      case _ =>
    }
  }

  private def beginBlock[BlockType <: AutoCloseable, T](block: BlockType)(exhaustBlock: BlockType => T): T = {
    /*
     * In the java API we are using try-with-resources for this. This is slightly problematic since we
     * are then always calling close which potentially will hide errors thrown in code generation.
     */
    val result = exhaustBlock(block)
    block.close()
    result
  }

  private def generateConstructor(
    clazz: codegen.ClassGenerator,
    fields: collection.Seq[Field],
    params: collection.Seq[Parameter],
    initializationCode: codegen.CodeBlock => codegen.Expression,
    parent: Option[TypeReference]
  ): Unit = {
    beginBlock(clazz.generateConstructor(params.map(_.asCodeGen).toSeq: _*)) { block =>
      block.expression(invokeSuper(parent.getOrElse(OBJECT)))
      fields.distinct.foreach {
        case field @ InstanceField(typ, name) =>
          val reference = clazz.field(typ, name)
          field.initializer.map(ir => compileExpression(ir(), block)).foreach { value =>
            block.put(block.self(), reference, value)
          }
        case StaticField(typ, name, _) =>
          clazz.publicStaticField(typ, name)
      }
      initializationCode(block)
    }
  }

  private def compileExpression(ir: IntermediateRepresentation, block: codegen.CodeBlock): codegen.Expression =
    ir match {
      // Foo.method(p1, p2,...)
      case InvokeStatic(method, params) =>
        invoke(method.asReference, params.map(p => compileExpression(p, block)): _*)

      // Foo.method(p1, p2,...)
      case InvokeStaticSideEffect(method, params) =>
        val invocation = invoke(method.asReference, params.map(p => compileExpression(p, block)): _*)
        if (method.returnType.isVoid) {
          block.expression(invocation)
        } else {
          block.expression(codegen.Expression.pop(invocation))
        }
        codegen.Expression.EMPTY

      // target.method(p1,p2,...)
      case Invoke(target, method, params) =>
        invoke(compileExpression(target, block), method.asReference, params.map(p => compileExpression(p, block)): _*)

      // target.method(p1,p2,...)
      case InvokeLocal(method, params) =>
        invoke(
          block.self(),
          codegen.MethodReference.methodReference(block.owner(), method.returnType, method.name, method.params: _*),
          params.map(p => compileExpression(p, block)): _*
        )

      // target.method(p1,p2,...)
      case InvokeSideEffect(target, method, params) =>
        val invocation =
          invoke(compileExpression(target, block), method.asReference, params.map(p => compileExpression(p, block)): _*)
        if (method.returnType.isVoid) {
          block.expression(invocation)
        } else {
          block.expression(codegen.Expression.pop(invocation))
        }
        codegen.Expression.EMPTY

      // this.method(p1,p2,...)
      case InvokeLocalSideEffect(method, params) =>
        val invocation = invoke(
          block.self(),
          codegen.MethodReference.methodReference(block.owner(), method.returnType, method.name, method.params: _*),
          params.map(p => compileExpression(p, block)): _*
        )
        if (method.returnType.isVoid) {
          block.expression(invocation)
        } else {
          block.expression(codegen.Expression.pop(invocation))
        }
        codegen.Expression.EMPTY

      // loads local variable by name
      case Load(variable, _) => block.load(variable)

      // loads field
      case LoadField(f) =>
        codegen.Expression.get(block.self(), field(block.owner(), f.typ, f.name))

      // sets a field
      case SetField(f, v) =>
        block.put(block.self(), field(block.owner(), f.typ, f.name), compileExpression(v, block))
        codegen.Expression.EMPTY

      // loads a given constant
      case Constant(value) => constant(value)

      // new ArrayValue[]{p1, p2,...}
      case ArrayLiteral(typ, values) => newInitializedArray(typ, values.map(v => compileExpression(v, block)): _*)

      // array[offset] = value
      case ArraySet(array, offset, value) =>
        block.expression(codegen.Expression.arraySet(
          compileExpression(array, block),
          compileExpression(offset, block),
          compileExpression(value, block)
        ))
        codegen.Expression.EMPTY

      // array.length
      case ArrayLength(array) =>
        codegen.Expression.arrayLength(compileExpression(array, block))

      // array[offset]
      case ArrayLoad(array, offset) =>
        codegen.Expression.arrayLoad(compileExpression(array, block), compileExpression(offset, block))

      // Foo.BAR
      case GetStatic(owner, typ, name) =>
        getStatic(staticField(owner.getOrElse(block.classGenerator().handle()), typ, name))

      // condition ? onTrue : onFalse
      case Ternary(condition, onTrue, onFalse) =>
        codegen.Expression.ternary(
          compileExpression(condition, block),
          compileExpression(onTrue, block),
          compileExpression(onFalse, block)
        )

      // lhs + rhs
      case Add(lhs, rhs) =>
        codegen.Expression.add(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs - rhs
      case Subtract(lhs, rhs) =>
        codegen.Expression.subtract(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs * rhs
      case Multiply(lhs, rhs) =>
        codegen.Expression.multiply(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs < rhs
      case Lt(lhs, rhs) =>
        codegen.Expression.lt(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs <= rhs
      case Lte(lhs, rhs) =>
        codegen.Expression.lte(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs > rhs
      case Gt(lhs, rhs) =>
        codegen.Expression.gt(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs > rhs
      case Gte(lhs, rhs) =>
        codegen.Expression.gte(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs == rhs
      case Eq(lhs, rhs) =>
        codegen.Expression.equal(compileExpression(lhs, block), compileExpression(rhs, block))

      // lhs != rhs
      case NotEq(lhs, rhs) =>
        codegen.Expression.notEqual(compileExpression(lhs, block), compileExpression(rhs, block))

      // test == null
      case IsNull(test) => codegen.Expression.isNull(compileExpression(test, block))

      // run multiple ops in a block, the value of the block is the last expression
      case Block(ops) =>
        if (ops.isEmpty) codegen.Expression.EMPTY else ops.map(compileExpression(_, block)).last

      // if (test) {onTrue}
      case Condition(test, onTrue, None) =>
        beginBlock(block.ifStatement(compileExpression(test, block)))(compileExpression(onTrue, _))

      case Condition(test, onTrue, Some(onFalse)) =>
        block.ifElseStatement(
          compileExpression(test, block),
          (t: codegen.CodeBlock) => compileExpression(onTrue, t),
          (f: codegen.CodeBlock) => compileExpression(onFalse, f)
        )
        codegen.Expression.EMPTY

      // typ name;
      case DeclareLocalVariable(typ, name) =>
        block.declare(typ, name)

      // name = value;
      case AssignToLocalVariable(name, value) =>
        block.assign(block.local(name), compileExpression(value, block))
        codegen.Expression.EMPTY

      // try {ops} catch(exception name)(onError)
      case TryCatch(ops, onError, exception, name) =>
        beginBlock(block.tryCatch(
          (errorBlock: codegen.CodeBlock) => compileExpression(onError, errorBlock),
          param(exception, name)
        ))(
          compileExpression(ops, _)
        )
        codegen.Expression.EMPTY

      // throw error
      case Throw(error) =>
        block.throwException(compileExpression(error, block))
        codegen.Expression.EMPTY

      // lhs && rhs
      case BooleanAnd(Seq(lhs, rhs)) =>
        codegen.Expression.and(compileExpression(lhs, block), compileExpression(rhs, block))
      case BooleanAnd(as) =>
        codegen.Expression.ands(as.map(a => compileExpression(a, block)).toArray)

      // lhs && rhs
      case BooleanOr(Seq(lhs, rhs)) =>
        codegen.Expression.or(compileExpression(lhs, block), compileExpression(rhs, block))
      case BooleanOr(as) =>
        codegen.Expression.ors(as.map(a => compileExpression(a, block)).toArray)

      // new Foo(args[0], args[1], ...)
      case NewInstance(constructor, args) =>
        codegen.Expression.invoke(
          codegen.Expression.newInstance(constructor.owner),
          constructor.asReference,
          args.map(compileExpression(_, block)): _*
        )

      // new Foo[5]
      case NewArray(baseType, size) =>
        codegen.Expression.newArray(baseType, size)

      // new Foo[size]
      case NewArrayDynamicSize(baseType, size) =>
        codegen.Expression.newArray(baseType, compileExpression(size, block))

      case Returns(value: IntermediateRepresentation) =>
        block.returns(compileExpression(value, block))
        codegen.Expression.EMPTY

      // while(test) { body }
      case Loop(test, body, labelName) =>
        beginBlock(block.whileLoop(compileExpression(test, block), labelName))(compileExpression(body, _))

      // break label
      case Break(labelName) =>
        block.breaks(labelName)
        codegen.Expression.EMPTY

      // (to) expressions
      case Cast(to, expression) => codegen.Expression.cast(to, compileExpression(expression, block))

      //  expressions instance of t
      case InstanceOf(typ, expression) => codegen.Expression.instanceOf(typ, compileExpression(expression, block))

      case Not(test) => codegen.Expression.not(compileExpression(test, block))

      case e @ OneTime(inner) =>
        if (!e.isUsed) {
          e.use()
          compileExpression(inner, block)
        } else {
          codegen.Expression.EMPTY
        }
      case Noop =>
        codegen.Expression.EMPTY

      case Box(expression) =>
        codegen.Expression.box(compileExpression(expression, block))

      case Unbox(expression) =>
        codegen.Expression.unbox(compileExpression(expression, block))

      case Self(_) => block.self()

      case NewInstanceInnerClass(ExtendClass(className, overrides, params, methods, fields), args) =>
        val parentClass: ClassHandle = block.classGenerator().handle()
        val generator = parentClass.generator
        val classHandle = beginBlock(generator.generateClass(overrides, parentClass.packageName(), className)) {
          (clazz: codegen.ClassGenerator) =>
            beginBlock(clazz.generateConstructor(params.map(_.asCodeGen): _*)) { constructor =>
              constructor.expression(Expression.invokeSuper(overrides, params.map(p => constructor.load(p.name)): _*))
              fields.distinct.foreach {
                case field @ InstanceField(typ, name) =>
                  val reference = clazz.field(typ, name)
                  field.initializer.map(ir => compileExpression(ir(), constructor)).foreach { value =>
                    constructor.put(constructor.self(), reference, value)
                  }
                case StaticField(typ, name, _) =>
                  val field = clazz.publicStaticField(typ, name)
                  constructor.putStatic(
                    field,
                    Expression.getStatic(FieldReference.staticField(parentClass, field.`type`(), field.name()))
                  )
              }
            }
            // methods
            methods.foreach { m =>
              compileMethodDeclaration(clazz, m)
            }
            clazz.handle()
        }

        val constructor =
          if (args.isEmpty) {
            codegen.MethodReference.constructorReference(classHandle)
          } else {
            codegen.MethodReference.constructorReference(classHandle, params.map(_.typ): _*)
          }
        codegen.Expression.invoke(
          codegen.Expression.newInstance(classHandle),
          constructor,
          args.map(compileExpression(_, block)): _*
        )

      case unknownIr =>
        throw new CodeGenerationNotSupportedException(null, s"Unknown ir `$unknownIr`") {}
    }

  private def compileClassDeclaration(c: ClassDeclaration[_], generator: CodeGenerator): codegen.ClassHandle = {
    val handle = beginBlock(generator.generateClass(
      c.extendsClass.getOrElse(codegen.TypeReference.OBJECT),
      c.packageName,
      c.className,
      c.implementsInterfaces.toSeq: _*
    )) { (clazz: codegen.ClassGenerator) =>
      generateConstructor(
        clazz,
        c.fields,
        c.constructorParameters,
        block => compileExpression(c.initializationCode, block),
        c.extendsClass
      )
      c.methods.foreach { m =>
        compileMethodDeclaration(clazz, m)
      }
      clazz.handle()
    }
    handle
  }

  private def compileMethodDeclaration(clazz: codegen.ClassGenerator, m: MethodDeclaration): Unit = {

    val estimatedSize = estimateByteCodeSize(m)
    if (estimatedSize > methodLimit) {
      throw new CantCompileQueryException(
        s"Method '${m.methodName}' is too big, estimated size $estimatedSize is bigger than $methodLimit"
      )
    }

    val method = codegen.MethodDeclaration.method(
      m.returnType,
      m.methodName,
      m.parameters.map(_.asCodeGen): _*
    ).modifiers(m.modifiers)
    m.parameterizedWith.foreach {
      case (name, bound) => method.parameterizedWith(name, bound)
    }
    m.throws.foreach(method.throwsException)

    beginBlock(clazz.generate(method)) { block =>
      m.localVariables.distinct.foreach { v =>
        block.assign(v.typ, v.name, compileExpression(v.value, block))
      }
      if (m.returnType == codegen.TypeReference.VOID) {
        block.expression(compileExpression(m.body, block))
      } else {
        block.returns(compileExpression(m.body, block))
      }
    }
  }
}
