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
package org.neo4j.cypher.internal

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.MapBasedParameterProvider
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NativeAuth
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.notification.HomeDatabaseNotPresent
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.Continue
import org.neo4j.cypher.internal.procs.InitAndFinallyFunctions
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.ParameterTransformer.ParameterGenerationFunction
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerResult
import org.neo4j.cypher.internal.procs.SystemGraphWriteExecutionPlan
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.WriteEffect
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_CONSTRAINT
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_ID_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_PROVIDER_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_RULE
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.HAS_AUTH
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.ROLE
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_HOME_DB_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_ID_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_SUSPENDED_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.COMPOSITE_DATABASE_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_DEFAULT_LANGUAGE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.gqlstatus.PrivilegeGqlCodeEntity
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.server.security.SecureHasher
import org.neo4j.server.security.SystemGraphCredential
import org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent
import org.neo4j.string.UTF8
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.ByteArray
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.UUID

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

trait AdministrationCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = None

  def isApplicableAdministrationCommand(logicalPlan: LogicalPlan): Boolean
}

object AdministrationCommandRuntime {
  private val secureHasher = new SecureHasher
  private val internalPrefix: String = "__internal_"
  val resolved_databaseName: String = "resolved_databaseName"
  val resolved_databaseUuid: String = "resolved_databaseUuid"

  def internalKey(name: String): String = internalPrefix + name

  private[internal] def validatePassword(password: Array[Byte])(config: Config): Array[Byte] = {
    if (password == null || password.length == 0) throw InvalidArgumentException.providedPasswordEmpty()

    val minimumPasswordLength = config.get(GraphDatabaseSettings.auth_minimum_password_length)
    val cb = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(password))
    try {
      if (cb.codePoints.count < minimumPasswordLength)
        throw InvalidArgumentException.shortPassword(minimumPasswordLength)
      password
    } finally {
      for (i <- 0 until cb.length) cb.put(i, '0')
    }
  }

  protected def hashPassword(initialPassword: Array[Byte]): TextValue = {
    Values.utf8Value(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize())
  }

  protected def validateAndFormatEncryptedPassword(password: Array[Byte]): TextValue =
    try {
      Values.utf8Value(SystemGraphCredential.serialize(password))
    } catch {
      case e: InvalidArgumentException => throw e
    }

  protected case class PasswordExpression(
    key: String,
    value: Value,
    bytesKey: String,
    bytesValue: Value,
    mapValueConverter: (Transaction, MapValue) => MapValue
  )

  private[internal] def getPasswordExpression(
    password: expressions.Expression,
    isEncryptedPassword: Boolean,
    otherParams: Array[String]
  )(config: Config): PasswordExpression =
    password match {
      case parameterPassword: Parameter =>
        validateStringParameterType(parameterPassword)

        // make sure we get a unique parameter name so we don't overwrite other parameters
        val hashedPwKey = ensureUniqueParamName(internalKey(parameterPassword.name) + "_hashed", otherParams)
        val passwordByteKey = ensureUniqueParamName(internalKey(parameterPassword.name) + "_bytes", otherParams)

        def convertPasswordParameters(params: MapValue): MapValue = {
          val encodedPassword = getValidPasswordParameter(params, parameterPassword.name)
          val hashedPassword =
            if (isEncryptedPassword) validateAndFormatEncryptedPassword(encodedPassword)
            else hashPassword(validatePassword(encodedPassword)(config))
          params.updatedWith(hashedPwKey, hashedPassword).updatedWith(
            passwordByteKey,
            Values.byteArray(encodedPassword)
          )
        }
        PasswordExpression(
          hashedPwKey,
          Values.NO_VALUE,
          passwordByteKey,
          Values.NO_VALUE,
          (_, params) => convertPasswordParameters(params)
        )

      case _ => throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"Internal error when processing password."
        )
    }

  private[internal] def getValidPasswordParameter(params: MapValue, passwordParameter: String): Array[Byte] = {
    params.get(passwordParameter) match {
      case bytes: ByteArray =>
        bytes.asObject() // Have as few copies of the password in memory as possible
      case s: StringValue =>
        UTF8.encode(s.stringValue()) // User parameters have String type
      case Values.NO_VALUE =>
        throw ParameterNotFoundException.expectedParam(passwordParameter, params.keySet())
      case other =>
        throw ParameterWrongTypeException.expectedPasswordToBeString(passwordParameter, other.getTypeName)
    }
  }

  private[internal] def ensureUniqueParamName(originalName: String, otherParams: Array[String]): String = {
    var uniqueName = originalName
    val params: Seq[String] = otherParams.sorted
    for (otherParamName <- params) {
      if (otherParamName.equals(uniqueName))
        uniqueName = uniqueName + "_"
    }
    uniqueName
  }

  private[internal] def validateStringParameterType(param: Parameter): Unit = {
    param.parameterType match {
      case _: StringType =>
      case _ =>
        throw ParameterWrongTypeException.onlyStringValuesAsPassword(
          param.name,
          String.valueOf(CTString),
          String.valueOf(param.parameterType)
        )
    }
  }

  def getParameterName(parameter: Either[String, Parameter]): Option[String] =
    // Either.toOption returns a Some containing the Right value if it exists or a None if this is a Left
    parameter.toOption.map(_.name)

  def makeCreateUserExecutionPlan(
    userName: Either[String, Parameter],
    suspended: Boolean,
    defaultDatabase: Option[HomeDatabaseAction],
    nativeAuth: Option[NativeAuth],
    externalAuths: Seq[ExternalAuth],
    validateAuth: (Seq[ExternalAuth], Option[NativeAuth]) => QueryHandlerResult = (_, _) => Continue,
    tagWriter: Option[(Transaction, String, MapValue) => Boolean] = None
  )(
    sourcePlan: Option[ExecutionPlan],
    normalExecutionEngine: ExecutionEngine,
    securityAuthorizationHandler: SecurityAuthorizationHandler,
    config: Config
  ): ExecutionPlan = {
    val changeRequiredOption = nativeAuth.map(auth => auth.changeRequired.getOrElse(true))
    val passwordChangeRequiredKey = internalKey("passwordChangeRequired")
    val suspendedKey = internalKey("suspended")
    val uuidKey = internalKey("uuid")
    val authKey = internalKey("auth")
    val homeDatabaseFields = defaultDatabase.map {
      case RemoveHomeDatabaseAction => DatabaseNameFields(
          s"${internalPrefix}homeDatabase",
          Values.NO_VALUE,
          s"${internalPrefix}homeDatabase_namespace",
          Values.NO_VALUE,
          s"${internalPrefix}homeDatabase_displayName",
          Values.NO_VALUE,
          s"${internalPrefix}homeDatabase_quotedDisplayName",
          Values.NO_VALUE,
          wasParameter = false,
          IdentityConverter
        )
      case SetHomeDatabaseAction(name) =>
        getDatabaseNameFields("homeDatabase", name, emulateGetNameFields = true)
    }
    val userNameFields = getNameFields("username", userName)
    val nonPasswordParameterNames = Array(
      userNameFields.nameKey,
      uuidKey,
      suspendedKey,
      authKey
    ) ++ homeDatabaseFields.map(_.displayNameKey) ++ changeRequiredOption.map(_ =>
      passwordChangeRequiredKey
    )
    val credentialsOption = nativeAuth.map(_.password).collectFirst {
      case Some(Password(password, isEncrypted)) =>
        getPasswordExpression(password, isEncrypted, nonPasswordParameterNames)(config)
    }
    val homeDatabaseCypher = homeDatabaseFields.map(ddf =>
      s", $USER_HOME_DB_PROPERTY: $$`${ddf.displayNameKey}`"
    ).getOrElse("")
    val nativeAuthCypher = credentialsOption.map(credentials =>
      s", $USER_CREDENTIALS_PROPERTY: $$`${credentials.key}`, $USER_CREDENTIALS_EXPIRED_PROPERTY: $$`$passwordChangeRequiredKey`"
    ).getOrElse("")

    def authMapGenerator: ParameterGenerationFunction = (_, _, params) => {
      val userId = Values.utf8Value(UUID.randomUUID().toString)
      val authList = externalAuths.map(auth => {
        val id = runtimeStringValue(auth.id, params, prettyPrint = true)
        validateAuthId(id)
        VirtualValues.map(Array("provider", "id"), Array(Values.utf8Value(auth.provider), Values.utf8Value(id)))
      }) ++ nativeAuth.map(_ =>
        VirtualValues.map(Array("provider", "id"), Array(Values.utf8Value(NATIVE_AUTH), userId))
      )
      VirtualValues.map(Array(authKey, uuidKey), Array(VirtualValues.list(authList: _*), userId))
    }

    val parameterTransformer = ParameterTransformer(authMapGenerator)
      .convert(userNameFields.nameConverter)
      .optionallyConvert(homeDatabaseFields.map(_.nameConverter))
      .optionallyConvert(credentialsOption.map(_.mapValueConverter))
      .validate(isHomeDatabasePresent(homeDatabaseFields))
    val createUserPlan: ExecutionPlan = UpdatingSystemCommandExecutionPlan(
      "CreateUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:$USER {$USER_NAME_PROPERTY: $$`${userNameFields.nameKey}`, $USER_ID_PROPERTY: $$`$uuidKey`, $USER_SUSPENDED_PROPERTY: $$`$suspendedKey`
         |$nativeAuthCypher
         |$homeDatabaseCypher })
         |WITH u
         |CALL {
         |  WITH u
         |  UNWIND $$`$authKey` AS auth
         |  CREATE (u)-[:$HAS_AUTH]->(:$AUTH {$AUTH_PROVIDER_PROPERTY: auth.provider, $AUTH_ID_PROPERTY: auth.id})
         |}
         |RETURN u.$USER_NAME_PROPERTY""".stripMargin,
      VirtualValues.map(
        credentialsOption.map(credentials => Array(credentials.key, credentials.bytesKey)).getOrElse(
          Array.empty[String]
        ) ++ nonPasswordParameterNames,
        credentialsOption.map(credentials => Array[AnyValue](credentials.value, credentials.bytesValue)).getOrElse(
          Array.empty[AnyValue]
        )
          ++ Array[AnyValue](
            userNameFields.nameValue,
            Values.NO_VALUE,
            Values.booleanValue(suspended),
            Values.NO_VALUE // generated
          ) ++ homeDatabaseFields.map(_.displayNameValue) ++ changeRequiredOption.map(Values.booleanValue)
      ),
      QueryHandler
        .handleError((error, params) =>
          (error, error.getCause) match {
            case (_, e: UniquePropertyValueValidationException) =>
              if (e.constraint().getName.equals(AUTH_CONSTRAINT)) {
                InvalidArgumentException.providerIdCombinationAlreadyInUseCreate(runtimeStringValue(userName, params));
              } else {
                InvalidArgumentException.createEntityAlreadyExists(
                  PrivilegeGqlCodeEntity.USER,
                  runtimeStringValue(userName, params)
                )
              }
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              DatabaseAdministrationOnFollowerException.notALeader(
                "CREATE USER",
                s"Failed to create the specified user '${runtimeStringValue(userName, params)}'",
                error
              )
            case _ => CypherExecutionException.createEntityCause("user", runtimeStringValue(userName, params), error)
          }
        )
        .handleResult { (_, _, _) => validateAuth(externalAuths, nativeAuth) },
      sourcePlan,
      initAndFinally = InitAndFinallyFunctions(
        initFunction = params => NameValidator.assertValidUsername(runtimeStringValue(userName, params)),
        finallyFunction =
          p => credentialsOption.foreach(credentials => p.get(credentials.bytesKey).asInstanceOf[ByteArray].zero())
      ),
      parameterTransformer = parameterTransformer
    )
    tagWriter.fold(createUserPlan)(fn =>
      SystemGraphWriteExecutionPlan(
        "CreateUserSetTags",
        securityAuthorizationHandler,
        Some(createUserPlan),
        WriteEffect.Subsumed((tx, _, params) => fn(tx, runtimeStringValue(userName, params), params))
      )
    )
  }

  def makeAlterUserExecutionPlan(
    userName: Either[String, Parameter],
    suspended: Option[Boolean],
    homeDatabase: Option[HomeDatabaseAction],
    nativeAuth: Option[NativeAuth],
    externalAuths: Seq[ExternalAuth],
    removeAuths: RemoveAuth,
    validateAuth: (Seq[ExternalAuth], Option[NativeAuth]) => QueryHandlerResult = (_, _) => Continue,
    tagWriter: Option[(Transaction, String, MapValue) => Boolean] = None
  )(
    sourcePlan: Option[ExecutionPlan],
    normalExecutionEngine: ExecutionEngine,
    securityAuthorizationHandler: SecurityAuthorizationHandler,
    userSecurityGraphComponent: UserSecurityGraphComponent,
    config: Config
  ): ExecutionPlan = {
    val userNameFields = getNameFields("username", userName)
    val setAuthKey = internalKey("setAuth")
    val removeAuthKey = internalKey("removeAuth")
    val removeNativeKey = internalKey("removeNative")
    val enforceAuthKey = internalKey("enforceAuth")
    val homeDatabaseFields = homeDatabase.map {
      case RemoveHomeDatabaseAction => DatabaseNameFields(
          s"${internalPrefix}homeDatabase",
          Values.NO_VALUE,
          s"${internalPrefix}homeDatabase_namespace",
          Values.NO_VALUE,
          s"${internalPrefix}homeDatabase_displayName",
          Values.NO_VALUE,
          s"${internalPrefix}homeDatabase_quotedDisplayName",
          Values.NO_VALUE,
          wasParameter = false,
          IdentityConverter
        )
      case SetHomeDatabaseAction(name) =>
        getDatabaseNameFields("homeDatabase", name, emulateGetNameFields = true)
    }
    val nonPasswordParameterNames = Array(userNameFields.nameKey) ++ homeDatabaseFields.map(_.displayNameKey) ++
      Array(setAuthKey, removeAuthKey, removeNativeKey, enforceAuthKey)
    val maybePw = nativeAuth.map(_.password).collectFirst {
      case Some(Password(password, isEncrypted)) =>
        getPasswordExpression(password, isEncrypted, nonPasswordParameterNames)(config)
    }
    val params = Seq(
      maybePw -> USER_CREDENTIALS_PROPERTY,
      nativeAuth.flatMap(_.changeRequired) -> USER_CREDENTIALS_EXPIRED_PROPERTY,
      suspended -> USER_SUSPENDED_PROPERTY,
      homeDatabaseFields -> USER_HOME_DB_PROPERTY
    ).flatMap { param =>
      param._1 match {
        case None                    => Seq.empty
        case Some(boolExpr: Boolean) => Seq((param._2, internalKey(param._2), Values.booleanValue(boolExpr)))
        case Some(passwordExpression: PasswordExpression) =>
          Seq((param._2, passwordExpression.key, passwordExpression.value))
        case Some(nameFields: NameFields) => Seq((param._2, nameFields.nameKey, nameFields.nameValue))
        case Some(nameFields: DatabaseNameFields) => Seq(
            (param._2, nameFields.displayNameKey, nameFields.displayNameValue)
          )
        case Some(p) =>
          // The $input (...getSimpleName) is a bit strange, but it's fine as this error should not happen as we only give the expected values in the loop
          throw InvalidArgumentException.invalidOptionTypeForAlterUser(
            String.valueOf(p),
            String.valueOf(p.getClass.getSimpleName)
          )
      }
    }
    val (setParts, keys, values) = params.foldLeft((
      "",
      Seq.empty[String],
      Seq.empty[Value]
    )) { (acc, param) =>
      val propertyName: String = param._1
      val key: String = param._2
      val value: Value = param._3
      (acc._1 + s" SET user.$propertyName = $$`$key`", acc._2 :+ key, acc._3 :+ value)
    }
    val parameterKeys: Array[String] =
      ((keys ++ maybePw.map(_.bytesKey).toSeq) :+ userNameFields.nameKey).toArray ++ Array(
        setAuthKey,
        removeAuthKey,
        removeNativeKey,
        enforceAuthKey
      )
    val parameterValues: Array[AnyValue] =
      ((values ++ maybePw.map(_.bytesValue).toSeq) :+ userNameFields.nameValue).toArray ++ Array[AnyValue](
        Values.NO_VALUE, // generated
        Values.NO_VALUE, // generated
        Values.NO_VALUE, // generated
        Values.NO_VALUE // generated
      )

    def enforceAuthGen: ParameterGenerationFunction = (transaction, _, _) => {
      val enforced = Values.booleanValue(userSecurityGraphComponent.requiresAuthObject(transaction))
      VirtualValues.map(Array(enforceAuthKey), Array(enforced))
    }

    def authMapGenerator: ParameterGenerationFunction = (_, _, params) => {
      val setAuthList = externalAuths.map(auth => {
        val id = runtimeStringValue(auth.id, params, prettyPrint = true)
        validateAuthId(id)
        VirtualValues.map(Array("provider", "id"), Array(Values.utf8Value(auth.provider), Values.utf8Value(id)))
      })
      val providers =
        removeAuths.auths.flatMap(expr => runtimeStringListValue(expr, params)).distinct
      val removeNative = providers.contains(NATIVE_AUTH)
      val removeAuthList = providers.map(Values.utf8Value)

      VirtualValues.map(
        Array(setAuthKey, removeAuthKey, removeNativeKey),
        Array(
          VirtualValues.list(setAuthList: _*),
          VirtualValues.list(removeAuthList: _*),
          Values.booleanValue(removeNative)
        )
      )
    }

    val removeAuthString = {
      val authMatch =
        if (removeAuths.all)
          s"OPTIONAL MATCH (user)-[:$HAS_AUTH]->(a:$AUTH)"
        else
          s"""UNWIND $$`$removeAuthKey` AS auth
             |  OPTIONAL MATCH (user)-[:$HAS_AUTH]->(a:$AUTH {$AUTH_PROVIDER_PROPERTY: auth})""".stripMargin

      s"""WITH user, oldCredentials
         |CALL {
         |  WITH user
         |  WITH user,
         |  CASE
         |    WHEN $$`$removeNativeKey` THEN {credentials: null, change: null}
         |    ELSE {credentials: user.$USER_CREDENTIALS_PROPERTY, change: user.$USER_CREDENTIALS_EXPIRED_PROPERTY}
         |  END AS cMap
         |  SET user.$USER_CREDENTIALS_PROPERTY = cMap.credentials, user.$USER_CREDENTIALS_EXPIRED_PROPERTY = cMap.change
         |}
         |WITH user, oldCredentials
         |CALL {
         |  WITH user
         |  $authMatch
         |  DETACH DELETE (a)
         |}""".stripMargin
    }

    val addNativeAuthString =
      if (nativeAuth.nonEmpty)
        s"""MERGE (user)-[:$HAS_AUTH]->(:$AUTH {$AUTH_PROVIDER_PROPERTY: '$NATIVE_AUTH', $AUTH_ID_PROPERTY: user.$USER_ID_PROPERTY})
           |SET user.$USER_CREDENTIALS_EXPIRED_PROPERTY = coalesce(user.$USER_CREDENTIALS_EXPIRED_PROPERTY, true)""".stripMargin
      else ""

    val nativeAuthValid =
      s"""
         |WITH user, oldCredentials
         |OPTIONAL MATCH (user)-[:$HAS_AUTH]->(nativeAuth:$AUTH {$AUTH_PROVIDER_PROPERTY: '$NATIVE_AUTH'})
         |WITH user, oldCredentials,
         | CASE EXISTS { (nativeAuth) }
         |  WHEN true THEN EXISTS { (user) WHERE user.$USER_CREDENTIALS_PROPERTY IS NOT NULL AND user.$USER_CREDENTIALS_EXPIRED_PROPERTY IS NOT NULL }
         |  ELSE true
         | END AS validNativeAuth
         |""".stripMargin

    val addAuthString =
      s"""WITH user, oldCredentials
         |CALL {
         |  WITH user
         |  UNWIND $$`$setAuthKey` AS auth
         |  MERGE (user)-[:$HAS_AUTH]->(a:$AUTH {$AUTH_PROVIDER_PROPERTY: auth.provider}) SET a.$AUTH_ID_PROPERTY = auth.id
         |}""".stripMargin

    val enforceAuthString =
      s"""CASE $$`$enforceAuthKey`
         | WHEN true THEN EXISTS { (user)-[:$HAS_AUTH]->(:$AUTH) }
         | ELSE true
         |END AS authOk
         |""".stripMargin

    val parameterTransformer = ParameterTransformer()
      .generate(enforceAuthGen)
      .generate(authMapGenerator)
      .convert(userNameFields.nameConverter)
      .optionallyConvert(homeDatabaseFields.map(_.nameConverter))
      .optionallyConvert(maybePw.map(_.mapValueConverter))
      .validate(isHomeDatabasePresent(homeDatabaseFields))
    val alterUserPlan: ExecutionPlan = UpdatingSystemCommandExecutionPlan(
      "AlterUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (user:$USER {$USER_NAME_PROPERTY: $$`${userNameFields.nameKey}`})
         |WITH user, user.$USER_CREDENTIALS_PROPERTY AS oldCredentials
         |$removeAuthString
         |$setParts
         |$addNativeAuthString
         |$addAuthString
         |$nativeAuthValid
         |RETURN EXISTS { (user:$USER {$USER_NAME_PROPERTY: $$`${userNameFields.nameKey}`}) } AS exists,
         |oldCredentials, $enforceAuthString, validNativeAuth """.stripMargin,
      VirtualValues.map(parameterKeys, parameterValues),
      QueryHandler
        .handleNoResult(p =>
          Some(ThrowException(InvalidArgumentException.alterMissingUser(
            runtimeStringValue(userName, p),
            getParameterName(userName).orNull
          )))
        )
        .handleError((error, p) =>
          (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              InvalidArgumentException.providerIdCombinationAlreadyInUseAlter(runtimeStringValue(userName, p))
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              DatabaseAdministrationOnFollowerException.notALeader(
                "ALTER USER",
                s"Failed to alter the specified user '${runtimeStringValue(userName, p)}'",
                error
              )
            case _ => CypherExecutionException.alterEntityCause("user", runtimeStringValue(userName, p), error)
          }
        )
        .handleResult {
          case (0, value: BooleanValue, p) if !value.booleanValue() =>
            ThrowException(InvalidArgumentException.alterMissingUser(
              runtimeStringValue(userName, p),
              getParameterName(userName).orNull
            ))
          case (1, value: TextValue, p) =>
            maybePw.map {
              newPw =>
                val oldCredentials =
                  SystemGraphCredential.deserialize(value.stringValue(), secureHasher)
                val newValue = p.get(newPw.bytesKey).asInstanceOf[ByteArray].asObject()
                if (oldCredentials.matchesPassword(newValue)) {
                  ThrowException(InvalidArgumentException.oldPasswordEqualsNew(runtimeStringValue(userName, p), false))
                } else validateAuth(externalAuths, nativeAuth)
            }.getOrElse(validateAuth(externalAuths, nativeAuth))
          case (2, value: BooleanValue, _) if !value.booleanValue() =>
            ThrowException(InvalidArgumentException.atLeastOneAuthProviderRequired())
          case (3, value: BooleanValue, _) if !value.booleanValue() =>
            ThrowException(InvalidArgumentException.missingMandatoryAuthClause(
              "SET PASSWORD",
              NATIVE_AUTH,
              s"Clause `SET PASSWORD` is mandatory for auth provider `$NATIVE_AUTH`."
            ))
          case _ => validateAuth(externalAuths, nativeAuth)
        },
      sourcePlan,
      initAndFinally = InitAndFinallyFunctions(finallyFunction =
        p => maybePw.foreach(newPw => p.get(newPw.bytesKey).asInstanceOf[ByteArray].zero())
      ),
      parameterTransformer =
        parameterTransformer
    )
    tagWriter.fold(alterUserPlan)(fn =>
      SystemGraphWriteExecutionPlan(
        "AlterUserSetTags",
        securityAuthorizationHandler,
        Some(alterUserPlan),
        WriteEffect.Subsumed((tx, _, params) => fn(tx, runtimeStringValue(userName, params), params))
      )
    )
  }

  private def isHomeDatabasePresent(homeDatabaseFields: Option[DatabaseNameFields])(
    tx: Transaction,
    params: MapValue
  ): (MapValue, Set[InternalNotification]) =
    homeDatabaseFields.map(ddf => {
      params.get(ddf.displayNameKey) match {
        case tv: TextValue =>
          val notifications: Set[InternalNotification] =
            if (Iterators.asList(tx.findNodes(DATABASE_NAME_LABEL, DISPLAY_NAME_PROPERTY, tv.stringValue())).isEmpty) {
              Set(HomeDatabaseNotPresent(tv.stringValue()))
            } else {
              Set.empty
            }
          (params, notifications)
        case _ => (params, Set.empty[InternalNotification])
      }
    }).getOrElse((params, Set.empty))

  def makeRenameExecutionPlan(
    entityType: PrivilegeGqlCodeEntity,
    namePropKey: String,
    fromName: Either[String, Parameter],
    toName: Either[String, Parameter],
    initFunction: MapValue => Boolean
  )(
    sourcePlan: Option[ExecutionPlan],
    normalExecutionEngine: ExecutionEngine,
    securityAuthorizationHandler: SecurityAuthorizationHandler
  ): ExecutionPlan = {
    val entity = entityType match {
      case PrivilegeGqlCodeEntity.USER           => USER
      case PrivilegeGqlCodeEntity.ROLE           => ROLE
      case PrivilegeGqlCodeEntity.AUTHRULE       => AUTH_RULE
      case PrivilegeGqlCodeEntity.DATABASE       => DATABASE_NAME
      case PrivilegeGqlCodeEntity.DATABASE_ALIAS => DATABASE_NAME
    }
    val fromNameFields = getNameFields("fromName", fromName)
    val toNameFields = getNameFields("toName", toName)

    val parameterTransformer = ParameterTransformer()
      .convert(fromNameFields.nameConverter)
      .convert(toNameFields.nameConverter)
    UpdatingSystemCommandExecutionPlan(
      s"Create$entity",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (old:$entity {$namePropKey: $$`${fromNameFields.nameKey}`})
         |SET old.$namePropKey = $$`${toNameFields.nameKey}`
         |RETURN old.$namePropKey
        """.stripMargin,
      VirtualValues.map(
        Array(fromNameFields.nameKey, toNameFields.nameKey),
        Array(fromNameFields.nameValue, toNameFields.nameValue)
      ),
      QueryHandler
        .handleNoResult(p => {
          Some(ThrowException(InvalidArgumentException.renameEntityNotFound(
            entityType,
            runtimeStringValue(fromName, p),
            runtimeStringValue(toName, p),
            getParameterName(fromName).orNull
          )))
        })
        .handleError((error, p) =>
          (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              InvalidArgumentException.renameEntityAlreadyExists(
                entityType,
                runtimeStringValue(fromName, p),
                runtimeStringValue(toName, p)
                // Not including the cause as that would be leaking information, we can consider logging it instead
              )
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              DatabaseAdministrationOnFollowerException.notALeader(
                s"RENAME ${entity.toUpperCase(Locale.ROOT)}",
                s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}'",
                error
              )
            case _ => CypherExecutionException.renameEntityCause(
                entity.toLowerCase(Locale.ROOT),
                runtimeStringValue(fromName, p),
                runtimeStringValue(toName, p),
                error
              )

          }
        ),
      sourcePlan,
      initAndFinally = InitAndFinallyFunctions(initFunction = initFunction),
      parameterTransformer = parameterTransformer
    )
  }

  /**
   *
   * @param key parameter key used in the "inner" cypher
   * @param name the literal or parameter
   * @param valueMapper function to apply to the value
   * @return
   */
  def getNameFields(
    key: String,
    name: Either[String, Parameter],
    valueMapper: String => String = identity
  ): NameFields = name match {
    case Left(u) =>
      NameFields(internalKey(key), Values.utf8Value(valueMapper(u)), IdentityConverter)
    case Right(parameter) =>
      NameFields(
        internalKey(key),
        Values.NO_VALUE,
        RenamingStringParameterConverter(
          parameter.name,
          internalKey(key),
          { v => Values.utf8Value(valueMapper(v.stringValue())) }
        )
      )
  }

  /**
   *
   * @param nameKey parameter key used in the "inner" cypher
   * @param name the namespaced name or parameter
   * @param emulateGetNameFields make this behave closer to getNameFields to keep existing behaviour for example for create database
   * @return
   */
  def getDatabaseNameFields(
    nameKey: String,
    name: DatabaseName,
    emulateGetNameFields: Boolean = false
  ): DatabaseNameFields = {
    // NOTE: valueMapper and backtick are used in different order, ensure that valueMapper doesn't affect backticks
    val valueMapper: String => String = NormalizedDatabaseName.normalize
    def backtick(s: String) = ExpressionStringifier().backtick(s)
    name match {
      case name @ NamespacedName(_, None) =>
        DatabaseNameFields(
          s"$internalPrefix$nameKey",
          Values.utf8Value(valueMapper(name.name)),
          s"$internalPrefix${nameKey}_namespace",
          Values.utf8Value(DEFAULT_NAMESPACE),
          s"$internalPrefix${nameKey}_displayName",
          Values.utf8Value(valueMapper(name.name)),
          s"$internalPrefix${nameKey}_quotedDisplayName",
          Values.utf8Value(backtick(valueMapper(name.name))),
          wasParameter = false,
          IdentityConverter
        )
      case name @ NamespacedName(_, Some(namespace)) =>
        DatabaseNameFields(
          s"$internalPrefix$nameKey",
          Values.utf8Value(valueMapper(name.name)),
          s"$internalPrefix${nameKey}_namespace",
          Values.utf8Value(valueMapper(namespace)),
          s"$internalPrefix${nameKey}_displayName",
          Values.utf8Value(
            if (namespace == DEFAULT_NAMESPACE) valueMapper(name.name)
            else valueMapper(namespace) + "." + valueMapper(name.name)
          ),
          s"$internalPrefix${nameKey}_quotedDisplayName",
          Values.utf8Value(
            if (namespace == DEFAULT_NAMESPACE) backtick(valueMapper(name.name))
            else backtick(valueMapper(namespace)) + "." + backtick(valueMapper(name.name))
          ),
          wasParameter = false,
          IdentityConverter
        )
      case pn: ParameterName =>
        // use 'real' parameter name to fetch values with `pn.getNameParts`
        // but then the internal name (nameKey) to not cause namespace collision with other internal parameters
        // similar as what `RenamingStringParameterConverter` does for `getNameFields`
        val displayNameKey = internalKey(nameKey + "_displayName")
        val quotedDisplayNameKey = internalKey(nameKey + "_quotedDisplayName")
        DatabaseNameFields(
          internalKey(nameKey),
          Values.NO_VALUE,
          internalKey(nameKey + "_namespace"),
          Values.utf8Value(DEFAULT_NAMESPACE),
          displayNameKey,
          Values.NO_VALUE,
          quotedDisplayNameKey,
          Values.NO_VALUE,
          wasParameter = true,
          (_, params) => {
            val (namespace, name, displayName, quotedDisplayName) =
              pn.getNameParts(MapBasedParameterProvider(params), DEFAULT_NAMESPACE, emulateGetNameFields)
            params.updatedWith(
              internalKey(nameKey + "_namespace"),
              Values.utf8Value(valueMapper(namespace.getOrElse(DEFAULT_NAMESPACE)))
            )
              .updatedWith(internalKey(nameKey), Values.utf8Value(valueMapper(name)))
              .updatedWith(displayNameKey, Values.utf8Value(valueMapper(displayName)))
              .updatedWith(quotedDisplayNameKey, Values.utf8Value(valueMapper(quotedDisplayName)))
          }
        )
    }
  }

  def runtimeStringValue(field: DatabaseName, params: MapValue): String = field match {
    case n: NamespacedName => n.toString
    case pn: ParameterName => runtimeStringValue(pn.parameter.name, params, prettyPrint = false)
  }

  def runtimeStringValue(
    field: Either[String, Parameter],
    params: MapValue,
    literalValueMapper: String => String = identity
  ): String = field match {
    case Left(s)  => literalValueMapper(s)
    case Right(p) => runtimeStringValue(p.name, params, prettyPrint = false)
  }

  def runtimeStringValue(field: Expression, params: MapValue, prettyPrint: Boolean): String = ({
    case StringLiteral(s) => s
    case p: Parameter     => runtimeStringValue(p.name, params, prettyPrint)
  }: PartialFunction[Expression, String]).apply(field)

  def runtimeStringValue(parameter: String, params: MapValue, prettyPrint: Boolean): String = {
    val value: AnyValue =
      if (params.containsKey(parameter))
        params.get(parameter)
      else
        params.get(internalKey(parameter))
    value match {
      case tv: TextValue => tv.stringValue()
      case _ =>
        throw ParameterWrongTypeException.expectedParameterToBeString42N51(
          prettyPrint,
          parameter,
          String.valueOf(value),
          value.prettify()
        )
    }
  }

  def runtimeStringListValue(
    field: Expression,
    params: MapValue,
    allowEmptyList: Boolean = false
  ): List[String] = field match {
    case StringLiteral(s) if s.nonEmpty => List(s)
    case l: ListLiteral
      if l.expressions.forall(e =>
        e.isInstanceOf[StringLiteral] && e.asInstanceOf[StringLiteral].value.nonEmpty
      ) && (allowEmptyList || l.expressions.nonEmpty) =>
      l.expressions.map(_.asInstanceOf[StringLiteral].value).toList
    case p: Parameter =>
      val value: AnyValue =
        if (params.containsKey(p.name))
          params.get(p.name)
        else
          params.get(internalKey(p.name))

      val pp = new PrettyPrinter()
      value match {
        case tv: TextValue if tv.stringValue().nonEmpty => List(tv.stringValue())
        case lv: ListValue if allowEmptyList || lv.nonEmpty =>
          lv.iterator().asScala.map {
            case tv: TextValue if tv.stringValue().nonEmpty => tv.stringValue()
            case v =>
              v.writeTo(pp)
              throw ParameterWrongTypeException.expectedListParameterToContainStrings(
                p.name,
                pp.value()
              )
          }.toList
        case _ =>
          value.writeTo(pp)
          throw ParameterWrongTypeException.expectedStringOrStringList2(p.name, pp.value());
      }
    case _ =>
      // this fails in parsing or semantic checking, but is needed for scala warnings
      val listDescription =
        if (allowEmptyList) "List of non-empty Strings"
        else "non-empty List of non-empty Strings"
      throw CypherExecutionException.internalError(
        this.getClass.getSimpleName,
        s"Expected non-empty String or $listDescription but was `${field.asCanonicalStringVal}`."
      )
  }

  private def validateAuthId(id: String): Unit =
    if (id.isEmpty) throw InvalidArgumentException.notAllowedToBeEmptyString("Auth id")

  private case class RenamingStringParameterConverter(
    parameter: String,
    name: String,
    valueMapper: TextValue => TextValue = identity
  ) extends ((Transaction, MapValue) => MapValue) {

    def apply(transaction: Transaction, params: MapValue): MapValue = {
      val paramValue = params.get(parameter)
      // Check the parameter is actually the expected type
      if (!paramValue.isInstanceOf[TextValue]) {
        throw ParameterWrongTypeException.expectedParameterToBeString42N51(
          false,
          parameter,
          String.valueOf(paramValue),
          paramValue.prettify()
        )
      } else params.updatedWith(name, valueMapper(params.get(parameter).asInstanceOf[TextValue]))
    }
  }

  case object IdentityConverter extends ((Transaction, MapValue) => MapValue) {
    def apply(transaction: Transaction, map: MapValue): MapValue = map
  }

  trait NameConverter {
    val nameConverter: (Transaction, MapValue) => MapValue
  }

  case class NameFields(
    nameKey: String,
    nameValue: Value,
    override val nameConverter: (Transaction, MapValue) => MapValue
  ) extends NameConverter

  case class DatabaseNameFields(
    nameKey: String,
    nameValue: Value,
    namespaceKey: String,
    namespaceValue: Value,
    displayNameKey: String,
    displayNameValue: Value,
    quotedDisplayNameKey: String,
    quotedDisplayNameValue: Value,
    wasParameter: Boolean,
    override val nameConverter: (Transaction, MapValue) => MapValue
  ) extends NameConverter {

    val keys: Array[String] = Array(nameKey, namespaceKey, displayNameKey, quotedDisplayNameKey)
    val values: Array[AnyValue] = Array(nameValue, namespaceValue, displayNameValue, quotedDisplayNameValue)

    def asNodeFilter(cypherVersion: CypherVersion): String = cypherVersion match {
      case CypherVersion.Cypher5 => s"{$NAME_PROPERTY: $$`$nameKey`, $NAMESPACE_PROPERTY: $$`$namespaceKey`}"
      case _                     => s"{$DISPLAY_NAME_PROPERTY: $$`$displayNameKey`}"
    }

  }

  type Show[T] = (T, MapValue) => String

  object Show {
    implicit val showDatabaseName: Show[DatabaseName] = (databaseName, p) => runtimeStringValue(databaseName, p)
    implicit val showString: Show[Either[String, Parameter]] = (s, p) => runtimeStringValue(s, p)
  }

  /*
   * This is a bit of a kludge to get around database names being ambiguous in 5.0 for backward
   * compatibility. We assume that 'db.name' means 'name' in composite 'db' but in case db does
   * not exist we need to rewrite the parameters to mean 'db.name' in the default namespace.
   */
  def checkNamespaceExists(
    aliasNameFields: DatabaseNameFields,
    context: AdministrationCommandRuntimeContext
  )(tx: Transaction, params: MapValue): (MapValue, Set[InternalNotification]) = {

    if (context.runtimeContext.cypherVersion != CypherVersion.Cypher5) {
      // Cypher 25+ ignores namespace/name split for lookup and has special handling for create
      // See updateNamespaceToMatchingComposite for Cypher25 behaviour
      (params, Set.empty)
    } else {
      val namespace = runtimeStringValue(aliasNameFields.namespaceKey, params, prettyPrint = false)
      // Check to see if there is a composite database node for this alias
      if (namespace == DEFAULT_NAMESPACE || compositeNamespaces(tx).contains(namespace)) {
        (params, Set.empty)
      } else {
        // Composite namespace doesn't exist
        interpretNamespaceAsPartOfName(aliasNameFields, params)
      }
    }
  }

  def updateNamespaceToMatchingComposite(
    cypherVersion: CypherVersion,
    aliasNameFields: DatabaseNameFields
  )(tx: Transaction, params: MapValue): MapValue = {
    if (cypherVersion == CypherVersion.Cypher5) {
      // See checkNamespaceExists for Cypher5 behaviour
      params
    } else {
      val fullName = runtimeStringValue(aliasNameFields.displayNameKey, params, prettyPrint = false)
      findNamespaceNameSplit(compositeNamespaces(tx), fullName) match {
        case Some((namespace, name)) =>
          params
            .updatedWith(aliasNameFields.namespaceKey, Values.utf8Value(namespace))
            .updatedWith(aliasNameFields.nameKey, Values.utf8Value(name))
        case None =>
          params
            .updatedWith(aliasNameFields.namespaceKey, Values.utf8Value(DEFAULT_NAMESPACE))
            .updatedWith(aliasNameFields.nameKey, Values.utf8Value(fullName))
      }
    }
  }

  private def compositeNamespaces(tx: Transaction): Seq[String] = {
    // List all composite database namespaces
    // MATCH (dbname:DatabaseName)-[:TARGETS]->(:CompositeDatabase) RETURN dbname.namespace
    Using.resource(tx.findNodes(
      DATABASE_NAME_LABEL
    )) { nodes =>
      nodes.asScala
        .filter(_
          .getProperty(NAMESPACE_PROPERTY)
          .equals(DEFAULT_NAMESPACE))
        .filter(nameNode =>
          Option(nameNode.getSingleRelationship(TARGETS_RELATIONSHIP, Direction.OUTGOING))
            .exists(_
              .getEndNode
              .hasLabel(COMPOSITE_DATABASE_LABEL))
        )
        .map(_.getProperty(DATABASE_NAME_PROPERTY))
        .flatMap {
          case s: String => Some(s)
          case _         => None // Covers both null and non-string values
        }.toList
    }
  }

  /**
   * Splits a name (e.g. 'a.b.c') into all possible namespace/name splits:
   * a | b.c
   * a.b | c
   * and returns the first that matches a namespace provided in compositeNamespaces
   *
   * @param compositeNamespaces composite namespaces that exist in the system db
   * @param fullName            the name to be split
   * @return An Option((namespace, name)) tuple if a matching split is found, None otherwise
   */
  private def findNamespaceNameSplit(compositeNamespaces: Seq[String], fullName: String): Option[(String, String)] = {
    // Find the indexes of all dots in the fullName (left to right)
    val dotIndexes = fullName.zipWithIndex.filter(_._1.equals('.')).map(_._2)
    // For each dot, split the fullName on that dot
    val namespaceNamePairs = dotIndexes
      .map(index => {
        val namespace = fullName.take(index)
        val name = fullName.drop(index + 1)
        (namespace, name)
      })
    // Find the first (namespace, name) pair for which a composite exists
    namespaceNamePairs
      .find { case (namespace, _) => compositeNamespaces.contains(namespace) }
  }

  private def interpretNamespaceAsPartOfName(
    aliasNameFields: DatabaseNameFields,
    params: MapValue
  ): (MapValue, Set[InternalNotification]) = {
    val namespace = runtimeStringValue(aliasNameFields.namespaceKey, params, prettyPrint = false)
    val name = runtimeStringValue(aliasNameFields.nameKey, params, prettyPrint = false)
    val aliasName = s"$namespace.$name"
    // This is just a regular local alias with . in the name, so use the default namespace
    (
      params.updatedWith(
        aliasNameFields.nameKey,
        Values.utf8Value(aliasName)
      )
        .updatedWith(aliasNameFields.namespaceKey, Values.utf8Value(DEFAULT_NAMESPACE)),
      Set.empty
    )
  }

  /** Translate from the persisted default language to the version description outputted in the show database and show alias commands.
    *
    * For aliases the property can be null, and should then return null.
    * For databases we should always get a value, and the default null will act as a warning about needing to update this when we add new Cypher versions
    *
    * @param node the node variable for which to check the property on
    */
  def translateDefaultLanguagePropertyToShowOutput(node: String): String =
    s"""CASE $node.$DATABASE_DEFAULT_LANGUAGE_PROPERTY
       |WHEN '${CypherVersion.Cypher5.persistedValue}' THEN '${CypherVersion.Cypher5.description}'
       |WHEN '${CypherVersion.Cypher25.persistedValue}' THEN '${CypherVersion.Cypher25.description}'
       |ELSE NULL
       |END""".stripMargin

}
