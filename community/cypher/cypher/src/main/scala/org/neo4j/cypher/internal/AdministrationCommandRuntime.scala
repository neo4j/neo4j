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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.security.SecureHasher
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.string.UTF8
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.ByteArray
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

trait AdministrationCommandRuntime extends CypherRuntime[RuntimeContext] {
  protected val followerError = "Administration commands must be executed on the LEADER server."
  protected val secureHasher = new SecureHasher

  def isApplicableAdministrationCommand(logicalPlanState: LogicalPlanState): Boolean

  protected def validatePassword(password: Array[Byte]): Array[Byte] = {
    if (password == null || password.length == 0) throw new InvalidArgumentsException("A password cannot be empty.")
    password
  }

  protected def hashPassword(initialPassword: Array[Byte]): TextValue = {
    try {
      Values.utf8Value(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize())
    } finally {
      //TODO: Make this work again (we have places we need this un-zero'd later for checking if password is duplicated)
      //if (initialPassword != null) java.util.Arrays.fill(initialPassword, 0.toByte)
    }
  }

  private val internalPrefix: String = "__internal_"

  protected def internalKey(name: String): String = internalPrefix + name

  trait PasswordExpression {
    def key: String
    def value: Value
    def bytesKey: String
    def bytesValue: Value
    def mapValueConverter: MapValue => MapValue
  }

  case class LiteralPasswordExpression(key: String, value: Value, bytesKey: String, bytesValue: Value) extends PasswordExpression {
    val mapValueConverter = IdentityConverter
  }

  case class ParameterPasswordExpression(key: String,
                                         value: Value,
                                         bytesKey: String,
                                         bytesValue: Value,
                                         mapValueConverter: MapValue => MapValue) extends PasswordExpression

  protected def getPasswordExpression(password: Either[Array[Byte], AnyRef]): PasswordExpression =
    password match {
      case Left(encodedPassword) =>
        validatePassword(encodedPassword)
        LiteralPasswordExpression(internalKey("credentials"), hashPassword(encodedPassword), internalKey("credentials_bytes"), Values.byteArray(encodedPassword))
      case Right(pw) if pw.isInstanceOf[ParameterFromSlot] =>
        // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
        val parameterPassword = pw.asInstanceOf[ParameterFromSlot]
        validateStringParameterType(parameterPassword)
        def convertPasswordParameters(params: MapValue): MapValue = {
          val passwordParameter = parameterPassword.name
          val encodedPassword = getValidPasswordParameter(params, passwordParameter)
          validatePassword(encodedPassword)
          val hashedPassword = hashPassword(encodedPassword)
          params.updatedWith(passwordParameter, hashedPassword).updatedWith(passwordParameter + "_bytes", Values.byteArray(encodedPassword))
        }
        ParameterPasswordExpression(parameterPassword.name, Values.NO_VALUE, s"${parameterPassword.name}_bytes", Values.NO_VALUE, convertPasswordParameters)
    }

  protected def getPasswordFieldsCurrent(password: Either[Array[Byte], AnyRef]): (String, Value, MapValue => MapValue) = {
    password match {
      case Left(encodedPassword) =>
        validatePassword(encodedPassword)
        (internalKey("current_credentials_bytes"), Values.byteArray(encodedPassword), IdentityConverter)
      case Right(pw) if pw.isInstanceOf[ParameterFromSlot] =>
        // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
        val parameterPassword = pw.asInstanceOf[ParameterFromSlot]
        validateStringParameterType(parameterPassword)
        val passwordParameter = parameterPassword.name
        val renamedParameter = s"__current_${passwordParameter}_bytes"
        def convertPasswordParameters(params: MapValue): MapValue = {
          val encodedPassword = getValidPasswordParameter(params, passwordParameter)
          validatePassword(encodedPassword)
          params.updatedWith(renamedParameter, Values.byteArray(encodedPassword))
        }
        (renamedParameter, Values.NO_VALUE, convertPasswordParameters)
    }
  }

  private def getValidPasswordParameter(params: MapValue, passwordParameter: String): Array[Byte] = {
    params.get(passwordParameter) match {
      case bytes: ByteArray =>
        bytes.asObjectCopy()
      case s: StringValue =>
        UTF8.encode(s.stringValue())
      case Values.NO_VALUE =>
        throw new ParameterNotFoundException(s"Expected parameter(s): $passwordParameter")
      case other =>
        throw new ParameterWrongTypeException("Only string values are accepted as password, got: " + other.getTypeName)
    }
  }

  private def validateStringParameterType(param: ParameterFromSlot): Unit = {
    param.parameterType match {
      case _:StringType =>
      case _ => throw new ParameterWrongTypeException(s"Only ${StringType.instance} values are accepted as password, got: " + param.parameterType)
    }
  }

  private def runtimeValue(parameter: String, params: MapValue): String = {
    val value: AnyValue = if (params.containsKey(parameter))
      params.get(parameter)
    else
      params.get(internalKey(parameter))
    value.asInstanceOf[Value].asObject().toString
  }

  protected def runtimeValue(field: Either[String, AnyRef], params: MapValue): String = field match {
    case Left(u) => u
    case Right(p) if p.isInstanceOf[ParameterFromSlot] =>
      runtimeValue(p.asInstanceOf[ParameterFromSlot].name, params)
    case Right(p) if p.isInstanceOf[Parameter] =>
      runtimeValue(p.asInstanceOf[Parameter].name, params)
  }

  protected def getNameFields(key: String,
                              name: Either[String, AnyRef],
                              valueMapper: String => String = s => s): (String, Value, MapValue => MapValue) = name match {
    case Left(u) =>
      (s"$internalPrefix$key", Values.utf8Value(valueMapper(u)), IdentityConverter)
    case Right(p) if p.isInstanceOf[ParameterFromSlot] =>
      // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
      val parameter = p.asInstanceOf[ParameterFromSlot]
      validateStringParameterType(parameter)
      def rename: String => String = paramName => internalKey(paramName)
      (rename(parameter.name), Values.NO_VALUE, RenamingParameterConverter(parameter.name, rename, valueMapper))
  }

  case object IdentityConverter extends Function[MapValue, MapValue] {
    def apply(map: MapValue): MapValue = map
  }

  case class RenamingParameterConverter(parameter: String, rename: String => String = s => s, valueMapper: String => String = s => s) extends Function[MapValue, MapValue] {
    def apply(params: MapValue): MapValue = {
      val newValue = valueMapper(params.get(parameter).asInstanceOf[TextValue].stringValue())
      params.updatedWith(rename(parameter), Values.utf8Value(newValue))
    }
  }

  protected def makeCreateUserExecutionPlan(userName: Either[String, Parameter],
                                  password: Either[Array[Byte], Parameter],
                                  requirePasswordChange: Boolean,
                                  suspended: Boolean)(
                                   sourcePlan: Option[ExecutionPlan],
                                   normalExecutionEngine: ExecutionEngine): ExecutionPlan = {
    val passwordChangeRequiredKey = internalKey("passwordChangeRequired")
    val suspendedKey = internalKey("suspended")
    val (userNameKey, userNameValue, userNameConverter) = getNameFields("username", userName)
    val credentials = getPasswordExpression(password)
    val mapValueConverter: MapValue => MapValue = p => credentials.mapValueConverter(userNameConverter(p))
    UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:User {name: $$$userNameKey, credentials: $$${credentials.key},
         |passwordChangeRequired: $$$passwordChangeRequiredKey, suspended: $$$suspendedKey})
         |RETURN u.name""".stripMargin,
      VirtualValues.map(
        Array(userNameKey, credentials.key, passwordChangeRequiredKey, suspendedKey),
        Array(
          userNameValue,
          credentials.value,
          Values.booleanValue(requirePasswordChange),
          Values.booleanValue(suspended))),
      QueryHandler
        .handleNoResult(params => Some(new IllegalStateException(s"Failed to create the specified user '${runtimeValue(userName, params)}'.")))
        .handleError((error, params) => (error, error.getCause) match {
          case (_, _: UniquePropertyValueValidationException) =>
            new InvalidArgumentsException(s"Failed to create the specified user '${runtimeValue(userName, params)}': User already exists.", error)
          case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to create the specified user '${runtimeValue(userName, params)}': $followerError", error)
          case _ => new IllegalStateException(s"Failed to create the specified user '${runtimeValue(userName, params)}'.", error)
        }),
      sourcePlan,
      initFunction = (params, _) => NameValidator.assertValidUsername(runtimeValue(userName, params)),
      parameterConverter = mapValueConverter
    )
  }

}
