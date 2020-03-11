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

  def validatePassword(password: Array[Byte]): Array[Byte] = {
    if (password == null || password.length == 0) throw new InvalidArgumentsException("A password cannot be empty.")
    password
  }

  def hashPassword(initialPassword: Array[Byte]): TextValue = {
    try {
      Values.utf8Value(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize())
    } finally {
      //TODO: Make this work again (we have places we need this un-zero'd later for checking if password is duplicated)
      //if (initialPassword != null) java.util.Arrays.fill(initialPassword, 0.toByte)
    }
  }

  def getPasswordFieldsCurrent(password: Either[Array[Byte], AnyRef]): (String, Value, MapValueConverter) =
    getPasswordFields(password, prefix = "__current_internal_", rename = s => s"__current_${s}_bytes", hashPw = false)

  def getPasswordFields(password: Either[Array[Byte], AnyRef],
                        prefix: String = "__internal_",
                        rename: String => String = s => s,
                        hashPw: Boolean = true): (String, Value, MapValueConverter) = password match {
    case Left(encodedPassword) =>
      validatePassword(encodedPassword)
      if (hashPw) {
        (rename(s"${prefix}credentials"), hashPassword(encodedPassword), IdentityConverter)
      } else {
        (rename(s"${prefix}credentials"), Values.byteArray(encodedPassword), IdentityConverter)
      }
    case Right(pw) if pw.isInstanceOf[ParameterFromSlot] =>
      // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
      val parameterPassword = pw.asInstanceOf[ParameterFromSlot]
      validateStringParameterType(parameterPassword)
      (rename(parameterPassword.name), Values.NO_VALUE, PasswordParameterConverter(parameterPassword.name, rename, hashPw = hashPw))
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

  protected def runtimeValuePW(field: Either[Array[Byte], AnyRef], params: MapValue): AnyRef = field match {
    case Left(u) => u
    case Right(p) if p.isInstanceOf[ParameterFromSlot] =>
      // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
      getValidPasswordParameter(params, p.asInstanceOf[ParameterFromSlot].name)
    case Right(p) if p.isInstanceOf[Parameter] =>
      // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
      getValidPasswordParameter(params, p.asInstanceOf[Parameter].name)
  }

  protected def runtimeValue(field: Either[String, AnyRef], params: MapValue): String = field match {
    case Left(u) => u
    case Right(p) if p.isInstanceOf[ParameterFromSlot] =>
      params.get(p.asInstanceOf[ParameterFromSlot].name).asInstanceOf[Value].asObject().toString
    case Right(p) if p.isInstanceOf[Parameter] =>
      params.get(p.asInstanceOf[Parameter].name).asInstanceOf[Value].asObject().toString
  }

  def getNameFields(key: String, name: Either[String, AnyRef],
                    prefix: String = "__internal_",
                    rename: String => String = s => s): (String, Value, MapValueConverter) = name match {
    case Left(u) =>
      (rename(s"$prefix$key"), Values.utf8Value(u), IdentityConverter)
    case Right(p) if p.isInstanceOf[ParameterFromSlot] =>
      // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
      val parameter = p.asInstanceOf[ParameterFromSlot]
      validateStringParameterType(parameter)
      (rename(parameter.name), Values.NO_VALUE, RenamingParameterConverter(parameter.name, rename))
  }

  trait MapValueConverter extends Function[MapValue, MapValue] {
    def overlaps(other: MapValueConverter) = false

    def apply(params: MapValue): MapValue = params
  }

  case object IdentityConverter extends MapValueConverter

  case class RenamingParameterConverter(parameter: String, rename: String => String = s => s) extends MapValueConverter {
    override def overlaps(other: MapValueConverter): Boolean = other match {
      case RenamingParameterConverter(name, _) => name == parameter
      case _ => false
    }

    override def apply(params: MapValue): MapValue = {
      params.updatedWith(rename(parameter), params.get(parameter))
    }
  }

  case class PasswordParameterConverter(passwordParameter: String, rename: String => String = s => s, hashPw: Boolean = true) extends MapValueConverter {
    override def overlaps(other: MapValueConverter): Boolean = other match {
      case PasswordParameterConverter(name, _, _) => name == passwordParameter
      case _ => false
    }

    override def apply(params: MapValue): MapValue = {
      val encodedPassword = getValidPasswordParameter(params, passwordParameter)
      validatePassword(encodedPassword)
      if (hashPw) {
        val hashedPassword = hashPassword(encodedPassword)
        params.updatedWith(rename(passwordParameter), hashedPassword)
      } else {
        params.updatedWith(rename(passwordParameter), Values.byteArray(encodedPassword))
      }
    }
  }

  def makeParameterValue(userName: Either[String, Parameter]): Value = userName match {
    case Left(s) => Values.utf8Value(s)
    case Right(_) => Values.NO_VALUE
  }

  def makeCreateUserExecutionPlan(userName: Either[String, Parameter],
                                  password: Either[Array[Byte], Parameter],
                                  requirePasswordChange: Boolean,
                                  suspended: Boolean)(
                                   sourcePlan: Option[ExecutionPlan],
                                   normalExecutionEngine: ExecutionEngine): ExecutionPlan = {
    val (userNameKey, userNameValue, userNameConverter) = getNameFields("username", userName)
    val (credentialsKey, credentialsValue, credentialsConverter) = getPasswordFields(password)
    val mapValueConverter: MapValue => MapValue = p => credentialsConverter(userNameConverter(p))
    UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:User {name: $$$userNameKey, credentials: $$$credentialsKey, passwordChangeRequired: $$passwordChangeRequired, suspended: $$suspended})
         |RETURN u.name""".stripMargin,
      VirtualValues.map(
        Array(userNameKey, credentialsKey, "passwordChangeRequired", "suspended"),
        Array(
          userNameValue,
          credentialsValue,
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
