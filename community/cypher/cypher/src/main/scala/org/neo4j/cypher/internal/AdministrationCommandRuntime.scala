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
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Transaction
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

  protected case class PasswordExpression(key: String,
                                          value: Value,
                                          bytesKey: String,
                                          bytesValue: Value,
                                          mapValueConverter: (Transaction, MapValue) => MapValue)

  protected def getPasswordExpression(password: expressions.Expression): PasswordExpression =
    password match {
      case parameterPassword: ParameterFromSlot =>
        validateStringParameterType(parameterPassword)
        def convertPasswordParameters(transaction: Transaction, params: MapValue): MapValue = {
          val passwordParameter = parameterPassword.name
          val encodedPassword = getValidPasswordParameter(params, passwordParameter)
          validatePassword(encodedPassword)
          val hashedPassword = hashPassword(encodedPassword)
          params.updatedWith(passwordParameter, hashedPassword).updatedWith(passwordParameter + "_bytes", Values.byteArray(encodedPassword))
        }
        PasswordExpression(parameterPassword.name, Values.NO_VALUE, s"${parameterPassword.name}_bytes", Values.NO_VALUE, convertPasswordParameters)
    }

  protected def getPasswordFieldsCurrent(password: expressions.Expression): (String, Value, MapValue => MapValue) = {
    password match {
      case parameterPassword: ParameterFromSlot =>
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
        bytes.asObject()  // Have as few copies of the password in memory as possible
      case s: StringValue =>
        UTF8.encode(s.stringValue()) // User parameters have String type
      case Values.NO_VALUE =>
        throw new ParameterNotFoundException(s"Expected parameter(s): $passwordParameter")
      case other =>
        throw new ParameterWrongTypeException("Only string values are accepted as password, got: " + other.getTypeName)
    }
  }

  private def validateStringParameterType(param: ParameterFromSlot): Unit = {
    param.parameterType match {
      case _: StringType =>
      case _ => throw new ParameterWrongTypeException(s"Only $CTString values are accepted as password, got: " + param.parameterType)
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
                              valueMapper: String => String = s => s): (String, Value, (Transaction, MapValue) => MapValue) = name match {
    case Left(u) =>
      (s"$internalPrefix$key", Values.utf8Value(valueMapper(u)), IdentityConverter)
    case Right(p) if p.isInstanceOf[ParameterFromSlot] =>
      // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
      val parameter = p.asInstanceOf[ParameterFromSlot]
      validateStringParameterType(parameter)
      def rename: String => String = paramName => internalKey(paramName)
      (rename(parameter.name), Values.NO_VALUE, RenamingParameterConverter(parameter.name, rename, valueMapper))
  }

  case object IdentityConverter extends ((Transaction, MapValue) => MapValue) {
    def apply(transaction: Transaction, map: MapValue): MapValue = map
  }

  case class RenamingParameterConverter(parameter: String, rename: String => String = s => s, valueMapper: String => String = s => s) extends ((Transaction, MapValue) => MapValue) {
    def apply(transaction: Transaction, params: MapValue): MapValue = {
      val newValue = valueMapper(params.get(parameter).asInstanceOf[TextValue].stringValue())
      params.updatedWith(rename(parameter), Values.utf8Value(newValue))
    }
  }

  protected def makeCreateUserExecutionPlan(userName: Either[String, Parameter],
                                  password: expressions.Expression,
                                  requirePasswordChange: Boolean,
                                  suspended: Boolean)(
                                   sourcePlan: Option[ExecutionPlan],
                                   normalExecutionEngine: ExecutionEngine): ExecutionPlan = {
    val passwordChangeRequiredKey = internalKey("passwordChangeRequired")
    val suspendedKey = internalKey("suspended")
    val (userNameKey, userNameValue, userNameConverter) = getNameFields("username", userName)
    val credentials = getPasswordExpression(password)
    val mapValueConverter: (Transaction, MapValue) => MapValue = (tx, p) => credentials.mapValueConverter(tx, userNameConverter(tx, p))
    UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:User {name: $$`$userNameKey`, credentials: $$`${credentials.key}`,
         |passwordChangeRequired: $$`$passwordChangeRequiredKey`, suspended: $$`$suspendedKey`})
         |RETURN u.name""".stripMargin,
      VirtualValues.map(
        Array(userNameKey, credentials.key, credentials.bytesKey, passwordChangeRequiredKey, suspendedKey),
        Array(
          userNameValue,
          credentials.value,
          credentials.bytesValue,
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
      finallyFunction = p => p.get(credentials.bytesKey).asInstanceOf[ByteArray].zero(),
      initFunction = (params, _) => NameValidator.assertValidUsername(runtimeValue(userName, params)),
      parameterConverter = mapValueConverter
    )
  }

  protected def makeAlterUserExecutionPlan(userName: Either[String, Parameter],
                                           password: Option[expressions.Expression],
                                           requirePasswordChange: Option[Boolean],
                                           suspended: Option[Boolean])(
                                            sourcePlan: Option[ExecutionPlan],
                                            normalExecutionEngine: ExecutionEngine
                                          ): ExecutionPlan = {
          val (userNameKey, userNameValue, userNameConverter) = getNameFields("username", userName)
      val maybePw = password.map(getPasswordExpression(_))
      val params = Seq(
        maybePw -> "credentials",
        requirePasswordChange -> "passwordChangeRequired",
        suspended -> "suspended"
      ).flatMap { param =>
        param._1 match {
          case None => Seq.empty
          case Some(boolExpr: Boolean) => Seq((param._2, internalKey(param._2), Values.booleanValue(boolExpr)))
          case Some(passwordExpression: PasswordExpression) => Seq((param._2, passwordExpression.key, passwordExpression.value))
          case Some(p) => throw new InvalidArgumentsException(s"Invalid option type for ALTER USER, expected PasswordExpression or Boolean but got: ${p.getClass.getSimpleName}")
        }
      }
      val (query, keys, values) = params.foldLeft((s"MATCH (user:User {name: $$`$userNameKey`}) WITH user, user.credentials AS oldCredentials", Seq.empty[String], Seq.empty[Value])) { (acc, param) =>
        val propertyName: String = param._1
        val key: String = param._2
        val value: Value = param._3
        (acc._1 + s" SET user.$propertyName = $$`$key`", acc._2 :+ key, acc._3 :+ value)
      }
      val parameterKeys: Seq[String] = (keys ++ maybePw.map(_.bytesKey).toSeq) :+ userNameKey
      val parameterValues: Seq[Value] = (values ++ maybePw.map(_.bytesValue).toSeq) :+ userNameValue
      val mapper: (Transaction, MapValue) => MapValue = (tx, m) => maybePw.map(_.mapValueConverter).getOrElse(IdentityConverter)(tx, userNameConverter(tx, m))
      UpdatingSystemCommandExecutionPlan("AlterUser", normalExecutionEngine,
        s"$query RETURN oldCredentials",
        VirtualValues.map(parameterKeys.toArray, parameterValues.toArray),
        QueryHandler
          .handleNoResult(p => Some(new InvalidArgumentsException(s"Failed to alter the specified user '${runtimeValue(userName, p)}': User does not exist.")))
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to alter the specified user '${runtimeValue(userName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to alter the specified user '${runtimeValue(userName, p)}'.", error)
          }
          .handleResult((_, value, p) => maybePw.flatMap { newPw =>
            val oldCredentials = SystemGraphCredential.deserialize(value.asInstanceOf[TextValue].stringValue(), secureHasher)
            val newValue = p.get(newPw.bytesKey).asInstanceOf[ByteArray].asObject()
            if (oldCredentials.matchesPassword(newValue))
              Some(new InvalidArgumentsException(s"Failed to alter the specified user '${runtimeValue(userName, p)}': Old password and new password cannot be the same."))
            else
              None
          }),
        sourcePlan,
        finallyFunction = p => maybePw.foreach(newPw => p.get(newPw.bytesKey).asInstanceOf[ByteArray].zero()),
        parameterConverter = mapper
      )
  }
}
