/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.codegen.api

import org.neo4j.codegen
import org.neo4j.codegen.CodeGenerator.generateCode
import org.neo4j.codegen.Expression._
import org.neo4j.codegen.FieldReference.{field, staticField}
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen.TypeReference.OBJECT
import org.neo4j.codegen._
import org.neo4j.codegen.bytecode.ByteCode.BYTECODE
import org.neo4j.codegen.bytecode.ByteCode.PRINT_BYTECODE
import org.neo4j.codegen.source.SourceCode.SOURCECODE
import org.neo4j.codegen.source.SourceCode.PRINT_SOURCE
import org.neo4j.codegen.source.SourceVisitor
import org.neo4j.codegen.{CodeGenerator, CodeGeneratorOption, DisassemblyVisitor, TypeReference}

import scala.collection.mutable.ArrayBuffer
import org.neo4j.codegen.source.SourceCode.{PRINT_SOURCE, SOURCECODE}

/**
  * Produces runnable code from an IntermediateRepresentation
  */
object CodeGeneration {

  // Use these options for Debugging. They will print generated code to stdout
  private val DEBUG_PRINT_SOURCE = false
  private val DEBUG_PRINT_BYTECODE = false

  sealed trait CodeGenerationMode {
    def saver: CodeSaver
  }
  case class ByteCodeGeneration(saver: CodeSaver) extends CodeGenerationMode
  case class SourceCodeGeneration(saver: CodeSaver) extends CodeGenerationMode

  object CodeGenerationMode {
    def fromDebugOptions(debugOptions: Set[String]): CodeGenerationMode = {
      if(debugOptions.contains("generate_java_source")) {
        val saver = new CodeSaver(debugOptions.contains("show_java_source"), debugOptions.contains("show_bytecode"))
        SourceCodeGeneration(saver)
      } else {
        val saver = new CodeSaver(false, debugOptions.contains("show_bytecode"))
        ByteCodeGeneration(saver)
      }
    }
  }

  class CodeSaver(saveSource: Boolean, saveByteCode: Boolean) {
    private val _source: ArrayBuffer[(String, String)] = new ArrayBuffer()
    private val _bytecode: ArrayBuffer[(String, String)] = new ArrayBuffer()

    private def sourceVisitor: SourceVisitor =
      (reference: TypeReference, sourceCode: CharSequence) => _source += (reference.name() -> sourceCode.toString)

    private def byteCodeVisitor: DisassemblyVisitor =
      (className: String, disassembly: CharSequence) => _bytecode += (className -> disassembly.toString)

    def options: List[CodeGeneratorOption] = {
      var l: List[CodeGeneratorOption] = Nil
      if (saveSource) l ::= sourceVisitor
      if (saveByteCode) l::= byteCodeVisitor
      l
    }

    def sourceCode: Seq[(String, String)] = _source

    def bytecode: Seq[(String, String)] = _bytecode
  }

  def compileClass[T](c: ClassDeclaration[T], generator: CodeGenerator): Class[T] = {
    val handle = compileClassDeclaration(c, generator)
    val clazz = handle.loadClass()
    setConstants(clazz, c.fields)
    clazz.asInstanceOf[Class[T]]
  }

  def compileAnonymousClass[T](c: ClassDeclaration[T], generator: CodeGenerator): Class[T] = {
    val handle = compileClassDeclaration(c, generator)
    val clazz = handle.loadAnonymousClass()
    setConstants(clazz, c.fields)
    clazz.asInstanceOf[Class[T]]
  }

  private def setConstants(clazz: Class[_], fields: Seq[Field]): Unit = {
    fields.distinct.foreach {
      case StaticField(_, name, Some(value)) =>
        clazz.getDeclaredField(name).set(null, value)
      case _ =>
    }
  }

  private def beginBlock[Block <: AutoCloseable, T](block: Block)(exhaustBlock: Block => T): T = {
    /*
     * In the java API we are using try-with-resources for this. This is slightly problematic since we
     * are then always calling close which potentially will hide errors thrown in code generation.
     */
    val result = exhaustBlock(block)
    block.close()
    result
  }

  private def generateConstructor(clazz: codegen.ClassGenerator, fields: Seq[Field], params: Seq[Parameter] = Seq.empty,
                          initializationCode: codegen.CodeBlock => codegen.Expression = _ => codegen.Expression.EMPTY,
                          parent: Option[TypeReference] = None): Unit = {
    beginBlock(clazz.generateConstructor(params.map(_.asCodeGen): _*)) { block =>
      block.expression(invokeSuper(parent.getOrElse(OBJECT)))
      fields.distinct.foreach {
        case InstanceField(typ, name, initializer) =>
          val reference = clazz.field(typ, name)
          initializer.map(ir => compileExpression(ir, block)).foreach { value =>
            block.put(block.self(), reference, value)
          }
        case StaticField(typ, name, _) =>
          clazz.publicStaticField(typ, name)
      }
      initializationCode(block)
    }
  }

  def createGenerator(codeGenerationMode: CodeGenerationMode): CodeGenerator = {
    var (strategy, options) = (codeGenerationMode, DEBUG_PRINT_SOURCE) match {
      case (SourceCodeGeneration(saver), _) => (SOURCECODE, saver.options)
      case (ByteCodeGeneration(saver), true) => (SOURCECODE, saver.options)
      case (ByteCodeGeneration(saver), false) => (BYTECODE, saver.options)
    }
    if (DEBUG_PRINT_SOURCE) options ::= PRINT_SOURCE
    if (DEBUG_PRINT_BYTECODE) options ::= PRINT_BYTECODE

    generateCode(classOf[IntermediateRepresentation].getClassLoader, strategy, options: _*)
  }

  private def compileExpression(ir: IntermediateRepresentation, block: codegen.CodeBlock): codegen.Expression = ir match {
    //Foo.method(p1, p2,...)
    case InvokeStatic(method, params) =>
      invoke(method.asReference, params.map(p => compileExpression(p, block)): _*)
    //target.method(p1,p2,...)
    case Invoke(target, method, params) =>
      invoke(compileExpression(target, block), method.asReference, params.map(p => compileExpression(p, block)): _*)
    //target.method(p1,p2,...)
    case InvokeSideEffect(target, method, params) =>
      val invocation = invoke(compileExpression(target, block), method.asReference,
                              params.map(p => compileExpression(p, block)): _*)

      if (method.returnType.isVoid) block.expression(invocation)
      else block.expression(codegen.Expression.pop(invocation))
      codegen.Expression.EMPTY

    //loads local variable by name
    case Load(variable) => block.load(variable)

    //loads field
    case LoadField(f) =>
      codegen.Expression.get(block.self(), field(block.owner(), f.typ, f.name))

    //sets a field
    case SetField(f, v) =>
      block.put(block.self(), field(block.owner(), f.typ, f.name), compileExpression(v, block))
      codegen.Expression.EMPTY

     //loads a given constant
    case Constant(value) => constant(value)

    //new ArrayValue[]{p1, p2,...}
    case ArrayLiteral(typ, values) => newInitializedArray(typ, values.map(v => compileExpression(v, block)): _*)

    // array[offset] = value
    case ArraySet(array, offset, value) =>
      block.expression(codegen.Expression.arraySet(compileExpression(array, block), compileExpression(offset, block), compileExpression(value, block)))
      codegen.Expression.EMPTY

    //array.length
    case ArrayLength(array) =>
     codegen.Expression.arrayLength(compileExpression(array, block))

    // array[offset]
    case ArrayLoad(array, offset) => codegen.Expression.arrayLoad(compileExpression(array, block), constant(offset))

    //Foo.BAR
    case GetStatic(owner, typ, name) => getStatic(staticField(owner.getOrElse(block.classGenerator().handle()), typ, name))

    //condition ? onTrue : onFalse
    case Ternary(condition, onTrue, onFalse) =>
      codegen.Expression.ternary(compileExpression(condition, block),
                         compileExpression(onTrue, block),
                         compileExpression(onFalse, block))

    //lhs + rhs
    case Add(lhs, rhs) =>
      codegen.Expression.add(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs - rhs
    case Subtract(lhs, rhs) =>
      codegen.Expression.subtract(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs * rhs
    case Multiply(lhs, rhs) =>
      codegen.Expression.multiply(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs < rhs
    case Lt(lhs, rhs) =>
      codegen.Expression.lt(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs <= rhs
    case Lte(lhs, rhs) =>
      codegen.Expression.lte(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs > rhs
    case Gt(lhs, rhs) =>
      codegen.Expression.gt(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs > rhs
    case Gte(lhs, rhs) =>
      codegen.Expression.gte(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs == rhs
    case Eq(lhs, rhs) =>
      codegen.Expression.equal(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs != rhs
    case NotEq(lhs, rhs) =>
      codegen.Expression.notEqual(compileExpression(lhs, block), compileExpression(rhs, block))

    //test == null
    case IsNull(test) => codegen.Expression.isNull(compileExpression(test, block))

    //run multiple ops in a block, the value of the block is the last expression
    case Block(ops) =>
      if (ops.isEmpty) codegen.Expression.EMPTY else ops.map(compileExpression(_, block)).last

    //if (test) {onTrue}
    case Condition(test, onTrue, None) =>
      beginBlock(block.ifStatement(compileExpression(test, block)))(compileExpression(onTrue, _))

    case Condition(test, onTrue, Some(onFalse)) =>
      block.ifElseStatement(compileExpression(test, block), (t: codegen.CodeBlock) => compileExpression(onTrue, t),
                            (f: codegen.CodeBlock) => compileExpression(onFalse, f))
      codegen.Expression.EMPTY

    //typ name;
    case DeclareLocalVariable(typ, name) =>
      block.declare(typ, name)

    //name = value;
    case AssignToLocalVariable(name, value) =>
      block.assign(block.local(name), compileExpression(value, block))
      codegen.Expression.EMPTY

    //try {ops} catch(exception name)(onError)
    case TryCatch(ops, onError, exception, name) =>
      block.tryCatch((mainBlock: codegen.CodeBlock) => compileExpression(ops, mainBlock),
                     (errorBlock: codegen.CodeBlock) => compileExpression(onError, errorBlock),
                     param(exception, name))
      codegen.Expression.EMPTY

    //throw error
    case Throw(error) =>
      block.throwException(compileExpression(error, block))
      codegen.Expression.EMPTY

    //lhs && rhs
    case BooleanAnd(lhs, rhs) =>
      codegen.Expression.and(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs && rhs
    case BooleanOr(lhs, rhs) =>
      codegen.Expression.or(compileExpression(lhs, block), compileExpression(rhs, block))

    //new Foo(args[0], args[1], ...)
    case NewInstance(constructor, args) =>
      codegen.Expression.invoke(codegen.Expression.newInstance(constructor.owner), constructor.asReference, args.map(compileExpression(_, block)):_*)

    //new Foo[5]
    case NewArray(baseType, size) =>
      codegen.Expression.newArray(baseType, size)

    case Returns(value: IntermediateRepresentation) =>
      block.returns(compileExpression(value, block))
      codegen.Expression.EMPTY

    //while(test) { body }
    case Loop(test, body, labelName) =>
      beginBlock(block.whileLoop(compileExpression(test, block), labelName))(compileExpression(body, _))

    //break label
    case Break(labelName) =>
      block.breaks(labelName)
      codegen.Expression.EMPTY

    // (to) expressions
    case Cast(to, expression) => codegen.Expression.cast(to, compileExpression(expression, block))

    //  expressions instance of t
    case InstanceOf(typ, expression) => codegen.Expression.instanceOf(typ, compileExpression(expression, block))

    case Not(test) => codegen.Expression.not(compileExpression(test, block))

    case e@OneTime(inner) =>
      if (!e.isUsed) {
        e.use()
        compileExpression(inner, block)
      } else codegen.Expression.EMPTY

    case Noop =>
      codegen.Expression.EMPTY

    case Box(expression) =>
      codegen.Expression.box(compileExpression(expression, block))

    case Unbox(expression) =>
      codegen.Expression.unbox(compileExpression(expression, block))

    case Self => block.self()

    case NewInstanceInnerClass(ExtendClass(className, overrides, params, methods, fields), args) =>
      val parentClass: ClassHandle = block.classGenerator().handle()
      val generator = parentClass.generator
      val classHandle = beginBlock(generator.generateClass(overrides, parentClass.packageName(), className)) { clazz: codegen.ClassGenerator =>

        beginBlock(clazz.generateConstructor(params.map(_.asCodeGen): _*)) { constructor =>
          constructor.expression(Expression.invokeSuper(overrides, params.map(p => constructor.load(p.name)): _*))
          fields.distinct.foreach {
            case InstanceField(typ, name, initializer) =>
              val reference = clazz.field(typ, name)
              initializer.map(ir => compileExpression(ir, constructor)).foreach { value =>
                constructor.put(constructor.self(), reference, value)
              }
            case StaticField(typ, name, _) =>
              val field = clazz.publicStaticField(typ, name)
              constructor.putStatic(field, Expression.getStatic(FieldReference.staticField(parentClass, field.`type`(), field.name())))
          }
        }
        //methods
        methods.foreach { m =>
          compileMethodDeclaration(clazz, m)
        }
        clazz.handle()
      }

      val constructor = if (args.isEmpty) codegen.MethodReference.constructorReference(classHandle)
      else codegen.MethodReference.constructorReference(classHandle, params.map(_.typ):_*)

      codegen.Expression.invoke(codegen.Expression.newInstance(classHandle), constructor, args.map(compileExpression(_, block)):_*)

  }

  private def compileClassDeclaration(c: ClassDeclaration[_], generator: CodeGenerator): codegen.ClassHandle = {
    val handle = beginBlock(generator.generateClass(c.extendsClass.getOrElse(codegen.TypeReference.OBJECT), c.packageName, c.className, c.implementsInterfaces: _*)) { clazz: codegen.ClassGenerator =>
      generateConstructor(clazz,
                          c.fields,
                          c.constructorParameters,
                          block => compileExpression(c.initializationCode, block),
                          c.extendsClass)
      c.methods.foreach { m =>
        compileMethodDeclaration(clazz, m)
      }
      clazz.handle()
    }
    handle
  }

  private def compileMethodDeclaration(clazz: codegen.ClassGenerator, m: MethodDeclaration): Unit = {
    val method = codegen.MethodDeclaration.method(m.returnType, m.methodName,
                                     m.parameters.map(_.asCodeGen): _*)
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
      }
      else {
        block.returns(compileExpression(m.body, block))
      }
    }
  }
}
