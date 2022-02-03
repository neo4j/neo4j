/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.security.SecureHasher
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.string.UTF8
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.ByteArray
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util.UUID

trait AdministrationCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = None

  def isApplicableAdministrationCommand(logicalPlan: LogicalPlan): Boolean
}

object AdministrationCommandRuntime {
  private[internal] val followerError = "Administration commands must be executed on the LEADER server."
  private val secureHasher = new SecureHasher
  private val internalPrefix: String = "__internal_"

  private[internal] def internalKey(name: String): String = internalPrefix + name

  private[internal] def validatePassword(password: Array[Byte]): Array[Byte] = {
    if (password == null || password.length == 0) throw new InvalidArgumentException("A password cannot be empty.")
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

  protected def validateAndFormatEncryptedPassword(password: Array[Byte]): TextValue = try {
    Values.utf8Value(SystemGraphCredential.serialize(password))
  } catch {
    case e: IllegalArgumentException => throw new InvalidArgumentException(e.getMessage, e)
  }



  protected case class PasswordExpression(key: String,
                                          value: Value,
                                          bytesKey: String,
                                          bytesValue: Value,
                                          mapValueConverter: (Transaction, MapValue) => MapValue)

  private[internal] def getPasswordExpression(userNameParameter: Option[expressions.Expression],
                                              password: expressions.Expression,
                                              isEncryptedPassword: Boolean,
                                              otherParams:Array[String]): PasswordExpression =
    password match {
      case parameterPassword: Parameter =>
        validateStringParameterType(parameterPassword)

        //make sure we get a unique parameter name so we don't overwrite other parameters
        val hashedPwKey = ensureUniqueParamName(internalKey(parameterPassword.name) + "_hashed", otherParams)
        val passwordByteKey = ensureUniqueParamName(internalKey(parameterPassword.name) + "_bytes", otherParams)

        def convertPasswordParameters(transaction: Transaction, params: MapValue): MapValue = {
          val encodedPassword = getValidPasswordParameter(params, parameterPassword.name)
          val hashedPassword = if (isEncryptedPassword) validateAndFormatEncryptedPassword(encodedPassword) else hashPassword(validatePassword(encodedPassword))
          params.updatedWith(hashedPwKey, hashedPassword).updatedWith(passwordByteKey, Values.byteArray(encodedPassword))
        }
        PasswordExpression(hashedPwKey, Values.NO_VALUE, passwordByteKey, Values.NO_VALUE, convertPasswordParameters)
    }

  private[internal] def getValidPasswordParameter(params: MapValue, passwordParameter: String): Array[Byte] = {
    params.get(passwordParameter) match {
      case bytes: ByteArray =>
        bytes.asObject()  // Have as few copies of the password in memory as possible
      case s: StringValue =>
        UTF8.encode(s.stringValue()) // User parameters have String type
      case Values.NO_VALUE =>
        throw new ParameterNotFoundException(s"Expected parameter(s): $passwordParameter")
      case other =>
        throw new ParameterWrongTypeException(s"Expected password parameter $$$passwordParameter to have type String but was ${other.getTypeName}")
    }
  }

  private[internal] def ensureUniqueParamName(originalName: String, otherParams: Array[String]): String = {
    var uniqueName = originalName
    val params:Seq[String] = otherParams.sorted
    for (otherParamName <- params) {
      if (otherParamName.equals(uniqueName))
        uniqueName = uniqueName + "_"
    }
    uniqueName
  }

  private[internal] def validateStringParameterType(param: Parameter): Unit = {
    param.parameterType match {
      case _: StringType =>
      case _ => throw new ParameterWrongTypeException(s"Only $CTString values are accepted as password, got: " + param.parameterType)
    }
  }

  private[internal] def makeCreateUserExecutionPlan(userName: Either[String, Parameter],
                                            isEncryptedPassword: Boolean,
                                            password: expressions.Expression,
                                            requirePasswordChange: Boolean,
                                            suspended: Boolean,
                                            defaultDatabase: Option[HomeDatabaseAction] = None)
                                           (sourcePlan: Option[ExecutionPlan],
                                            normalExecutionEngine: ExecutionEngine,
                                            securityAuthorizationHandler: SecurityAuthorizationHandler): ExecutionPlan = {
    val passwordChangeRequiredKey = internalKey("passwordChangeRequired")
    val suspendedKey = internalKey("suspended")
    val uuidKey = internalKey("uuid")
    val homeDatabaseFields = defaultDatabase.map {
      case RemoveHomeDatabaseAction    => NameFields(s"${internalPrefix}homeDatabase", Values.NO_VALUE, IdentityConverter)
      case SetHomeDatabaseAction(name) => getNameFields("homeDatabase", name, s => new NormalizedDatabaseName(s).name())
    }
    val userNameFields = getNameFields("username", userName)
    val nonPasswordParameterNames = Array(userNameFields.nameKey, uuidKey, passwordChangeRequiredKey, suspendedKey) ++ homeDatabaseFields.map(_.nameKey)
    val credentials = getPasswordExpression(userName.toOption, password, isEncryptedPassword, nonPasswordParameterNames)
    val homeDatabaseCypher = homeDatabaseFields.map(ddf => s", homeDatabase: $$`${ddf.nameKey}`").getOrElse("")
    val mapValueConverter: (Transaction, MapValue) => MapValue = (tx, p) => {
      val newHomeDatabaseFields = homeDatabaseFields.map(_.nameConverter(tx, userNameFields.nameConverter(tx, p)))
      credentials.mapValueConverter(tx, newHomeDatabaseFields.getOrElse(userNameFields.nameConverter(tx, p)))
    }
    UpdatingSystemCommandExecutionPlan("CreateUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:User {name: $$`${userNameFields.nameKey}`, id: $$`$uuidKey`, credentials: $$`${credentials.key}`,
         |passwordChangeRequired: $$`$passwordChangeRequiredKey`, suspended: $$`$suspendedKey`
         |$homeDatabaseCypher })
         |RETURN u.name""".stripMargin,
      VirtualValues.map(
        Array(credentials.key, credentials.bytesKey) ++ nonPasswordParameterNames,
        Array[AnyValue](
          credentials.value,
          credentials.bytesValue,
          userNameFields.nameValue,
          Values.utf8Value(UUID.randomUUID().toString),
          Values.booleanValue(requirePasswordChange),
          Values.booleanValue(suspended)
          ) ++ homeDatabaseFields.map(_.nameValue)
      ),
      QueryHandler
        .handleNoResult(params => Some(new IllegalStateException(s"Failed to create the specified user '${runtimeStringValue(userName, params)}'.")))
        .handleError((error, params) => (error, error.getCause) match {
          case (_, _: UniquePropertyValueValidationException) =>
            new InvalidArgumentException(s"Failed to create the specified user '${runtimeStringValue(userName, params)}': User already exists.", error)
          case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to create the specified user '${runtimeStringValue(userName, params)}': $followerError", error)
          case _ => new IllegalStateException(s"Failed to create the specified user '${runtimeStringValue(userName, params)}'.", error)
        }),
      sourcePlan,
      finallyFunction = p => p.get(credentials.bytesKey).asInstanceOf[ByteArray].zero(),
      initFunction = params => NameValidator.assertValidUsername(runtimeStringValue(userName, params)),
      parameterConverter = mapValueConverter
    )
  }

  private[internal] def makeAlterUserExecutionPlan(userName: Either[String, Parameter],
                                           isEncryptedPassword: Option[Boolean],
                                           password: Option[expressions.Expression],
                                           requirePasswordChange: Option[Boolean],
                                           suspended: Option[Boolean],
                                           defaultDatabase: Option[HomeDatabaseAction] = None)
                                          (sourcePlan: Option[ExecutionPlan],
                                           normalExecutionEngine: ExecutionEngine,
                                           securityAuthorizationHandler: SecurityAuthorizationHandler): ExecutionPlan = {
    val userNameFields = getNameFields("username", userName)
    val homeDatabaseFields = defaultDatabase.map {
      case RemoveHomeDatabaseAction    => NameFields(s"${internalPrefix}homeDatabase", Values.NO_VALUE, IdentityConverter)
      case SetHomeDatabaseAction(name) => getNameFields("homeDatabase", name, s => new NormalizedDatabaseName(s).name())
    }
    val nonPasswordParameterNames = Array(userNameFields.nameKey) ++ homeDatabaseFields.map(_.nameKey)
    val maybePw = password.map(p => getPasswordExpression(userName.toOption, p, isEncryptedPassword.getOrElse(false), nonPasswordParameterNames))
    val params = Seq(
      maybePw -> "credentials",
      requirePasswordChange -> "passwordChangeRequired",
      suspended -> "suspended",
      homeDatabaseFields -> "homeDatabase"
    ).flatMap { param =>
      param._1 match {
        case None => Seq.empty
        case Some(boolExpr: Boolean) => Seq((param._2, internalKey(param._2), Values.booleanValue(boolExpr)))
        case Some(passwordExpression: PasswordExpression) => Seq((param._2, passwordExpression.key, passwordExpression.value))
        case Some(nameFields: NameFields) => Seq((param._2, nameFields.nameKey, nameFields.nameValue))
        case Some(p) => throw new InvalidArgumentException(
          s"Invalid option type for ALTER USER, expected PasswordExpression, Boolean, String or Parameter but got: ${p.getClass.getSimpleName}")
      }
    }
    val (query, keys, values) = params.foldLeft((s"MATCH (user:User {name: $$`${userNameFields.nameKey}`}) WITH user, user.credentials AS oldCredentials", Seq.empty[String], Seq.empty[Value])) { (acc, param) =>
      val propertyName: String = param._1
      val key: String = param._2
      val value: Value = param._3
      (acc._1 + s" SET user.$propertyName = $$`$key`", acc._2 :+ key, acc._3 :+ value)
    }
    val parameterKeys: Seq[String] = (keys ++ maybePw.map(_.bytesKey).toSeq) :+ userNameFields.nameKey
    val parameterValues: Seq[Value] = (values ++ maybePw.map(_.bytesValue).toSeq) :+ userNameFields.nameValue
    val mapper: (Transaction, MapValue) => MapValue = (tx, p) => {
      val newHomeDatabaseFields = homeDatabaseFields.map(_.nameConverter(tx, userNameFields.nameConverter(tx, p)))
      maybePw.map(_.mapValueConverter).getOrElse(IdentityConverter)(tx, newHomeDatabaseFields.getOrElse(userNameFields.nameConverter(tx, p)))
    }
    UpdatingSystemCommandExecutionPlan("AlterUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"$query RETURN oldCredentials",
      VirtualValues.map(parameterKeys.toArray, parameterValues.toArray),
      QueryHandler
        .handleNoResult(p => Some(new InvalidArgumentException(s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': User does not exist.")))
        .handleError {
          case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': $followerError", error)
          case (error, p) => new IllegalStateException(s"Failed to alter the specified user '${runtimeStringValue(userName, p)}'.", error)
        }
        .handleResult((_, value, p) => maybePw.flatMap { newPw =>
          val oldCredentials = SystemGraphCredential.deserialize(value.asInstanceOf[TextValue].stringValue(), secureHasher)
          val newValue = p.get(newPw.bytesKey).asInstanceOf[ByteArray].asObject()
          if (oldCredentials.matchesPassword(newValue))
            Some(new InvalidArgumentException(s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': Old password and new password cannot be the same."))
          else
            None
        }),
      sourcePlan,
      finallyFunction = p => maybePw.foreach(newPw => p.get(newPw.bytesKey).asInstanceOf[ByteArray].zero()),
      parameterConverter = mapper
    )
  }

  private[internal] def makeRenameExecutionPlan(entity: String,
                                        fromName: Either[String, Parameter],
                                        toName: Either[String, Parameter],
                                        initFunction: MapValue => Boolean)
                                       (sourcePlan: Option[ExecutionPlan],
                                        normalExecutionEngine: ExecutionEngine,
                                        securityAuthorizationHandler: SecurityAuthorizationHandler): ExecutionPlan = {
    val fromNameFields = getNameFields("fromName", fromName)
    val toNameFields = getNameFields("toName", toName)
    val mapValueConverter: (Transaction, MapValue) => MapValue = (tx, p) => toNameFields.nameConverter(tx, fromNameFields.nameConverter(tx, p))

    UpdatingSystemCommandExecutionPlan(s"Create$entity",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (old:$entity {name: $$`${fromNameFields.nameKey}`})
         |SET old.name = $$`${toNameFields.nameKey}`
         |RETURN old.name
        """.stripMargin,
      VirtualValues.map(Array(fromNameFields.nameKey, toNameFields.nameKey), Array(fromNameFields.nameValue, toNameFields.nameValue)),
      QueryHandler
        .handleNoResult(p => Some(new InvalidArgumentException(s"Failed to rename the specified ${entity.toLowerCase} '${runtimeStringValue(fromName, p)}' to " +
          s"'${runtimeStringValue(toName, p)}': The ${entity.toLowerCase} '${runtimeStringValue(fromName, p)}' does not exist.")))
        .handleError((error, p) => (error, error.getCause) match {
          case (_, _: UniquePropertyValueValidationException) =>
            new InvalidArgumentException(s"Failed to rename the specified ${entity.toLowerCase} '${runtimeStringValue(fromName, p)}' to " +
              s"'${runtimeStringValue(toName, p)}': " + s"$entity '${runtimeStringValue(toName, p)}' already exists.", error)
          case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to rename the specified ${entity.toLowerCase} '${runtimeStringValue(fromName, p)}': $followerError", error)
          case _ =>
            new IllegalStateException(s"Failed to rename the specified ${entity.toLowerCase} '${runtimeStringValue(fromName, p)}' to '${runtimeStringValue(toName, p)}'.", error)
        }),
      sourcePlan,
      initFunction = initFunction,
      parameterConverter = mapValueConverter
    )
  }

  /**
   *
   * @param key parameter key used in the "inner" cypher
   * @param name the literal or parameter
   * @param valueMapper function to apply to the value
   * @return
   */
  private[internal] def getNameFields(key: String,
                              name: Either[String, Parameter],
                              valueMapper: String => String = identity): NameFields = name match {
    case Left(u) =>
      NameFields(s"$internalPrefix$key", Values.utf8Value(valueMapper(u)), IdentityConverter)
    case Right(parameter) =>
      def rename: String => String = paramName => internalKey(paramName)
      NameFields(rename(parameter.name), Values.NO_VALUE, RenamingStringParameterConverter(parameter.name, rename, { v => Values.utf8Value(valueMapper(v.stringValue()))}))
  }

  private[internal] def runtimeStringValue(field: Either[String, Parameter], params: MapValue): String = field match {
    case Left(u) => u
    case Right(p)  => runtimeStringValue(p.name, params)
  }

  private def runtimeStringValue(parameter: String, params: MapValue): String = {
    val value: AnyValue = if (params.containsKey(parameter))
      params.get(parameter)
    else
      params.get(internalKey(parameter))
    value match {
      case tv: TextValue => tv.stringValue()
      case _ =>  throw new ParameterWrongTypeException(s"Expected parameter $$$parameter to have type String but was $value")
    }
  }

  case class RenamingStringParameterConverter(parameter: String, rename: String => String = identity _, valueMapper: TextValue => TextValue = identity _)
    extends ((Transaction, MapValue) => MapValue) {
    def apply(transaction: Transaction, params: MapValue): MapValue = {
      val paramValue = params.get(parameter)
      // Check the parameter is actually the expected type
      if (!paramValue.isInstanceOf[TextValue]) {
        throw new ParameterWrongTypeException(s"Expected parameter $$$parameter to have type String but was $paramValue")
      } else params.updatedWith(rename(parameter), valueMapper(params.get(parameter).asInstanceOf[TextValue]))
    }
  }

  case object IdentityConverter extends ((Transaction, MapValue) => MapValue) {
    def apply(transaction: Transaction, map: MapValue): MapValue = map
  }

  case class NameFields(nameKey: String, nameValue: Value, nameConverter: (Transaction, MapValue) => MapValue)
}
