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
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NativeAuth
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.Continue
import org.neo4j.cypher.internal.procs.InitAndFinallyFunctions
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.ParameterTransformer.ParameterGenerationFunction
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerResult
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.util.DeprecatedDatabaseNameNotification
import org.neo4j.cypher.internal.util.HomeDatabaseNotPresent
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.COMPOSITE_DATABASE_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
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
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper.NATIVE_AUTH
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_CONSTRAINT
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

  private[internal] val followerError = "Administration commands must be executed on the LEADER server."
  private val secureHasher = new SecureHasher
  private val internalPrefix: String = "__internal_"

  private[internal] def internalKey(name: String): String = internalPrefix + name

  private[internal] def validatePassword(password: Array[Byte])(config: Config): Array[Byte] = {
    if (password == null || password.length == 0) throw new InvalidArgumentException("A password cannot be empty.")

    val minimumPasswordLength = config.get(GraphDatabaseSettings.auth_minimum_password_length)
    val cb = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(password))
    try {
      if (cb.codePoints.count < minimumPasswordLength)
        throw new InvalidArgumentException(s"A password must be at least $minimumPasswordLength characters.")
      password
    } finally {
      for (i <- 0 until cb.length) cb.put(i, '0')
    }
  }

  protected def hashPassword(initialPassword: Array[Byte]): TextValue = {
    try {
      Values.utf8Value(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize())
    } finally {
      // TODO: Make this work again (we have places we need this un-zero'd later for checking if password is duplicated)
      // if (initialPassword != null) java.util.Arrays.fill(initialPassword, 0.toByte)
    }
  }

  protected def validateAndFormatEncryptedPassword(password: Array[Byte]): TextValue =
    try {
      Values.utf8Value(SystemGraphCredential.serialize(password))
    } catch {
      case e: IllegalArgumentException => throw new InvalidArgumentException(e.getMessage, e)
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

      case _ => throw new IllegalStateException(s"Internal error when processing password.")
    }

  private[internal] def getValidPasswordParameter(params: MapValue, passwordParameter: String): Array[Byte] = {
    params.get(passwordParameter) match {
      case bytes: ByteArray =>
        bytes.asObject() // Have as few copies of the password in memory as possible
      case s: StringValue =>
        UTF8.encode(s.stringValue()) // User parameters have String type
      case Values.NO_VALUE =>
        throw new ParameterNotFoundException(s"Expected parameter(s): $passwordParameter")
      case other =>
        throw new ParameterWrongTypeException(
          s"Expected password parameter $$$passwordParameter to have type String but was ${other.getTypeName}"
        )
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
      case _ => throw new ParameterWrongTypeException(
          s"Only $CTString values are accepted as password, got: " + param.parameterType
        )
    }
  }

  private[internal] def makeCreateUserExecutionPlan(
    userName: Either[String, Parameter],
    suspended: Boolean,
    defaultDatabase: Option[HomeDatabaseAction],
    nativeAuth: Option[NativeAuth],
    externalAuths: Seq[ExternalAuth],
    validateAuth: (Seq[ExternalAuth], Option[NativeAuth]) => QueryHandlerResult = (_, _) => Continue
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
    val userId = Values.utf8Value(UUID.randomUUID().toString)
    val authKey = internalKey("auth")
    val homeDatabaseFields = defaultDatabase.map {
      case RemoveHomeDatabaseAction => NameFields(s"${internalPrefix}homeDatabase", Values.NO_VALUE, IdentityConverter)
      case SetHomeDatabaseAction(name) =>
        getNameFields("homeDatabase", name.asLegacyName, s => new NormalizedDatabaseName(s).name())
    }
    val userNameFields = getNameFields("username", userName)
    val nonPasswordParameterNames = Array(
      userNameFields.nameKey,
      uuidKey,
      suspendedKey,
      authKey
    ) ++ homeDatabaseFields.map(_.nameKey) ++ changeRequiredOption.map(_ =>
      passwordChangeRequiredKey
    )
    val credentialsOption = nativeAuth.map(_.password).collectFirst {
      case Some(Password(password, isEncrypted)) =>
        getPasswordExpression(password, isEncrypted, nonPasswordParameterNames)(config)
    }
    val homeDatabaseCypher = homeDatabaseFields.map(ddf => s", homeDatabase: $$`${ddf.nameKey}`").getOrElse("")
    val nativeAuthCypher = credentialsOption.map(credentials =>
      s", credentials: $$`${credentials.key}`, passwordChangeRequired: $$`$passwordChangeRequiredKey`"
    ).getOrElse("")

    def authMapGenerator: ParameterGenerationFunction = (_, _, params) => {
      val authList = externalAuths.map(auth => {
        val id = runtimeStringValue(auth.id, params, prettyPrint = true)
        validateAuthId(id)
        VirtualValues.map(Array("provider", "id"), Array(Values.utf8Value(auth.provider), Values.utf8Value(id)))
      }) ++ nativeAuth.map(_ =>
        VirtualValues.map(Array("provider", "id"), Array(Values.utf8Value(NATIVE_AUTH), userId))
      )
      VirtualValues.map(Array(authKey), Array(VirtualValues.list(authList: _*)))
    }

    val parameterTransformer = ParameterTransformer(authMapGenerator)
      .convert(userNameFields.nameConverter)
      .optionallyConvert(homeDatabaseFields.map(_.nameConverter))
      .optionallyConvert(credentialsOption.map(_.mapValueConverter))
      .validate(isHomeDatabasePresent(homeDatabaseFields))
    UpdatingSystemCommandExecutionPlan(
      "CreateUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:User {name: $$`${userNameFields.nameKey}`, id: $$`$uuidKey`, suspended: $$`$suspendedKey`
         |$nativeAuthCypher
         |$homeDatabaseCypher })
         |WITH u
         |CALL {
         |  WITH u
         |  UNWIND $$`$authKey` AS auth
         |  CREATE (u)-[:HAS_AUTH]->(:Auth {provider: auth.provider, id: auth.id})
         |}
         |RETURN u.name""".stripMargin,
      VirtualValues.map(
        credentialsOption.map(credentials => Array(credentials.key, credentials.bytesKey)).getOrElse(
          Array.empty
        ) ++ nonPasswordParameterNames,
        credentialsOption.map(credentials => Array[AnyValue](credentials.value, credentials.bytesValue)).getOrElse(
          Array.empty
        )
          ++ Array[AnyValue](
            userNameFields.nameValue,
            userId,
            Values.booleanValue(suspended),
            Values.NO_VALUE // generated
          ) ++ homeDatabaseFields.map(_.nameValue) ++ changeRequiredOption.map(Values.booleanValue)
      ),
      QueryHandler
        .handleNoResult(params =>
          Some(ThrowException(
            new CypherExecutionException(
              s"Failed to create the specified user '${runtimeStringValue(userName, params)}'."
            )
          ))
        )
        .handleError((error, params) =>
          (error, error.getCause) match {
            case (_, e: UniquePropertyValueValidationException) =>
              if (e.constraint().getName.equals(AUTH_CONSTRAINT)) {
                new InvalidArgumentException(
                  s"Failed to create the specified user '${runtimeStringValue(userName, params)}': The combination of provider and id is already in use.",
                  error
                )
              } else {
                new InvalidArgumentException(
                  s"Failed to create the specified user '${runtimeStringValue(userName, params)}': User already exists.",
                  error
                )
              }
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(
                s"Failed to create the specified user '${runtimeStringValue(userName, params)}': $followerError",
                error
              )
            case _ => new CypherExecutionException(
                s"Failed to create the specified user '${runtimeStringValue(userName, params)}'.",
                error
              )
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
  }

  private[internal] def makeAlterUserExecutionPlan(
    userName: Either[String, Parameter],
    suspended: Option[Boolean],
    homeDatabase: Option[HomeDatabaseAction],
    nativeAuth: Option[NativeAuth],
    externalAuths: Seq[ExternalAuth],
    removeAuths: RemoveAuth,
    validateAuth: (Seq[ExternalAuth], Option[NativeAuth]) => QueryHandlerResult = (_, _) => Continue
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
      case RemoveHomeDatabaseAction => NameFields(s"${internalPrefix}homeDatabase", Values.NO_VALUE, IdentityConverter)
      case SetHomeDatabaseAction(name) =>
        getNameFields("homeDatabase", name.asLegacyName, s => new NormalizedDatabaseName(s).name())
    }
    val nonPasswordParameterNames = Array(userNameFields.nameKey) ++ homeDatabaseFields.map(_.nameKey) ++
      Array(setAuthKey, removeAuthKey, removeNativeKey, enforceAuthKey)
    val maybePw = nativeAuth.map(_.password).collectFirst {
      case Some(Password(password, isEncrypted)) =>
        getPasswordExpression(password, isEncrypted, nonPasswordParameterNames)(config)
    }
    val params = Seq(
      maybePw -> "credentials",
      nativeAuth.flatMap(_.changeRequired) -> "passwordChangeRequired",
      suspended -> "suspended",
      homeDatabaseFields -> "homeDatabase"
    ).flatMap { param =>
      param._1 match {
        case None                    => Seq.empty
        case Some(boolExpr: Boolean) => Seq((param._2, internalKey(param._2), Values.booleanValue(boolExpr)))
        case Some(passwordExpression: PasswordExpression) =>
          Seq((param._2, passwordExpression.key, passwordExpression.value))
        case Some(nameFields: NameFields) => Seq((param._2, nameFields.nameKey, nameFields.nameValue))
        case Some(p) => throw new InvalidArgumentException(
            s"Invalid option type for ALTER USER, expected PasswordExpression, Boolean, String or Parameter but got: ${p.getClass.getSimpleName}"
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
          "OPTIONAL MATCH (user)-[:HAS_AUTH]->(a:Auth)"
        else
          s"""UNWIND $$`$removeAuthKey` AS auth
             |  OPTIONAL MATCH (user)-[:HAS_AUTH]->(a:Auth {provider: auth})""".stripMargin

      s"""WITH user, oldCredentials
         |CALL {
         |  WITH user
         |  WITH user,
         |  CASE
         |    WHEN $$`$removeNativeKey` THEN {credentials: null, change: null}
         |    ELSE {credentials: user.credentials, change: user.passwordChangeRequired}
         |  END AS cMap
         |  SET user.credentials = cMap.credentials, user.passwordChangeRequired = cMap.change
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
        s"""MERGE (user)-[:HAS_AUTH]->(:Auth {provider: '$NATIVE_AUTH', id: user.id})
           |SET user.passwordChangeRequired = coalesce(user.passwordChangeRequired, true)""".stripMargin
      else ""

    val nativeAuthValid =
      s"""
         |WITH user, oldCredentials
         |OPTIONAL MATCH (user)-[:HAS_AUTH]->(nativeAuth:Auth {provider: '$NATIVE_AUTH'})
         |WITH user, oldCredentials,
         | CASE EXISTS { (nativeAuth) }
         |  WHEN true THEN EXISTS { (user) WHERE user.credentials IS NOT NULL AND user.passwordChangeRequired IS NOT NULL }
         |  ELSE true
         | END AS validNativeAuth
         |""".stripMargin

    val addAuthString =
      s"""WITH user, oldCredentials
         |CALL {
         |  WITH user
         |  UNWIND $$`$setAuthKey` AS auth
         |  MERGE (user)-[:HAS_AUTH]->(a:Auth {provider: auth.provider}) SET a.id = auth.id
         |}""".stripMargin

    val enforceAuthString =
      s"""CASE $$`$enforceAuthKey`
         | WHEN true THEN EXISTS { (user)-[:HAS_AUTH]->(:Auth) }
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
    UpdatingSystemCommandExecutionPlan(
      "AlterUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (user:User {name: $$`${userNameFields.nameKey}`})
         |WITH user, user.credentials AS oldCredentials
         |$removeAuthString
         |$setParts
         |$addNativeAuthString
         |$addAuthString
         |$nativeAuthValid
         |RETURN EXISTS { (user:User {name: $$`${userNameFields.nameKey}`}) } AS exists,
         |oldCredentials, $enforceAuthString, validNativeAuth """.stripMargin,
      VirtualValues.map(parameterKeys, parameterValues),
      QueryHandler
        .handleNoResult(p =>
          Some(ThrowException(new InvalidArgumentException(
            s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': User does not exist."
          )))
        )
        .handleError((error, p) =>
          (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new InvalidArgumentException(
                s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': The combination of provider and id is already in use.",
                error
              )
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(
                s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': $followerError",
                error
              )
            case _ => new CypherExecutionException(
                s"Failed to alter the specified user '${runtimeStringValue(userName, p)}'.",
                error
              )
          }
        )
        .handleResult {
          case (0, value: BooleanValue, p) if !value.booleanValue() =>
            ThrowException(new InvalidArgumentException(
              s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': User does not exist."
            ))
          case (1, value: TextValue, p) =>
            maybePw.map {
              newPw =>
                val oldCredentials =
                  SystemGraphCredential.deserialize(value.stringValue(), secureHasher)
                val newValue = p.get(newPw.bytesKey).asInstanceOf[ByteArray].asObject()
                if (oldCredentials.matchesPassword(newValue)) {
                  ThrowException(new InvalidArgumentException(
                    s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': Old password and new password cannot be the same."
                  ))
                } else validateAuth(externalAuths, nativeAuth)
            }.getOrElse(validateAuth(externalAuths, nativeAuth))
          case (2, value: BooleanValue, _) if !value.booleanValue() =>
            ThrowException(new InvalidArgumentException(
              "User has no auth provider. Add at least one auth provider for the user or consider suspending them."
            ))
          case (3, value: BooleanValue, _) if !value.booleanValue() =>
            ThrowException(new InvalidArgumentException(
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
  }

  private def isHomeDatabasePresent(homeDatabaseFields: Option[NameFields])(
    tx: Transaction,
    params: MapValue
  ): (MapValue, Set[InternalNotification]) =
    homeDatabaseFields.map(ddf => {
      params.get(ddf.nameKey) match {
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

  private[internal] def makeRenameExecutionPlan(
    entity: String,
    fromName: Either[String, Parameter],
    toName: Either[String, Parameter],
    initFunction: MapValue => Boolean
  )(
    sourcePlan: Option[ExecutionPlan],
    normalExecutionEngine: ExecutionEngine,
    securityAuthorizationHandler: SecurityAuthorizationHandler
  ): ExecutionPlan = {
    val fromNameFields = getNameFields("fromName", fromName)
    val toNameFields = getNameFields("toName", toName)

    val parameterTransformer = ParameterTransformer()
      .convert(fromNameFields.nameConverter)
      .convert(toNameFields.nameConverter)
    UpdatingSystemCommandExecutionPlan(
      s"Create$entity",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (old:$entity {name: $$`${fromNameFields.nameKey}`})
         |SET old.name = $$`${toNameFields.nameKey}`
         |RETURN old.name
        """.stripMargin,
      VirtualValues.map(
        Array(fromNameFields.nameKey, toNameFields.nameKey),
        Array(fromNameFields.nameValue, toNameFields.nameValue)
      ),
      QueryHandler
        .handleNoResult(p =>
          Some(ThrowException(new InvalidArgumentException(
            s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' to " +
              s"'${runtimeStringValue(toName, p)}': The ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' does not exist."
          )))
        )
        .handleError((error, p) =>
          (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new InvalidArgumentException(
                s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' to " +
                  s"'${runtimeStringValue(toName, p)}': " + s"$entity '${runtimeStringValue(toName, p)}' already exists.",
                error
              )
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(
                s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}': $followerError",
                error
              )
            case _ =>
              new CypherExecutionException(
                s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' to '${runtimeStringValue(toName, p)}'.",
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
  private[internal] def getNameFields(
    key: String,
    name: Either[String, Parameter],
    valueMapper: String => String = identity
  ): NameFields = name match {
    case Left(u) =>
      NameFields(internalKey(key), Values.utf8Value(valueMapper(u)), IdentityConverter)
    case Right(parameter) =>
      def rename: String => String = paramName => internalKey(paramName)
      NameFields(
        rename(parameter.name),
        Values.NO_VALUE,
        RenamingStringParameterConverter(
          parameter.name,
          rename,
          { v => Values.utf8Value(valueMapper(v.stringValue())) }
        )
      )
  }

  /**
   *
   * @param nameKey parameter key used in the "inner" cypher
   * @param name the namespaced name or parameter
   * @return
   */
  private[internal] def getDatabaseNameFields(
    nameKey: String,
    name: DatabaseName
  ): DatabaseNameFields = {
    val valueMapper: String => String = new NormalizedDatabaseName(_).name()
    name match {
      case name @ NamespacedName(_, None) =>
        DatabaseNameFields(
          s"$internalPrefix$nameKey",
          Values.utf8Value(valueMapper(name.name)),
          s"$internalPrefix${nameKey}_namespace",
          Values.utf8Value(DEFAULT_NAMESPACE),
          s"$internalPrefix${nameKey}_displayName",
          Values.utf8Value(valueMapper(name.name)),
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
          wasParameter = false,
          IdentityConverter
        )
      case pn @ ParameterName(parameter) =>
        def rename: String => String = paramName => internalKey(paramName)
        val displayNameKey = internalKey(parameter.name + "_displayName")
        DatabaseNameFields(
          rename(parameter.name),
          Values.NO_VALUE,
          internalKey(parameter.name + "_namespace"),
          Values.utf8Value(DEFAULT_NAMESPACE),
          displayNameKey,
          Values.NO_VALUE,
          wasParameter = true,
          (_, params) => {
            val (namespace, name, displayName) = pn.getNameParts(params, DEFAULT_NAMESPACE)
            params.updatedWith(
              internalKey(parameter.name + "_namespace"),
              Values.utf8Value(valueMapper(namespace.getOrElse(DEFAULT_NAMESPACE)))
            )
              .updatedWith(internalKey(parameter.name), Values.utf8Value(valueMapper(name)))
              .updatedWith(displayNameKey, Values.utf8Value(valueMapper(displayName)))
          }
        )
    }
  }

  private[internal] def runtimeStringValue(field: DatabaseName, params: MapValue): String = field match {
    case n: NamespacedName => n.toString
    case ParameterName(p)  => runtimeStringValue(p.name, params, prettyPrint = false)
  }

  private[internal] def runtimeStringValue(field: Either[String, Parameter], params: MapValue): String = field match {
    case Left(s)  => s
    case Right(p) => runtimeStringValue(p.name, params, prettyPrint = false)
  }

  private[internal] def runtimeStringValue(field: Expression, params: MapValue, prettyPrint: Boolean): String = ({
    case StringLiteral(s) => s
    case p: Parameter     => runtimeStringValue(p.name, params, prettyPrint)
  }: PartialFunction[Expression, String]).apply(field)

  private[internal] def runtimeStringValue(parameter: String, params: MapValue, prettyPrint: Boolean): String = {
    val value: AnyValue =
      if (params.containsKey(parameter))
        params.get(parameter)
      else
        params.get(internalKey(parameter))
    value match {
      case tv: TextValue => tv.stringValue()
      case _ =>
        val (p, v) = if (prettyPrint) {
          val pp = new PrettyPrinter()
          value.writeTo(pp)
          (s"`$$$parameter`", s"`${pp.value()}`.")
        } else (s"$$$parameter", value.toString)
        throw new ParameterWrongTypeException(s"Expected parameter $p to have type String but was $v")
    }
  }

  private[internal] def runtimeStringListValue(field: Expression, params: MapValue): List[String] = field match {
    case StringLiteral(s) if s.nonEmpty => List(s)
    case l: ListLiteral
      if l.expressions.forall(e =>
        e.isInstanceOf[StringLiteral] && e.asInstanceOf[StringLiteral].value.nonEmpty
      ) && l.expressions.nonEmpty =>
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
        case lv: ListValue if lv.nonEmpty =>
          lv.iterator().asScala.map {
            case tv: TextValue if tv.stringValue().nonEmpty => tv.stringValue()
            case v =>
              v.writeTo(pp)
              throw new ParameterWrongTypeException(
                s"Expected parameter `$$${p.name}` to only contain non-empty Strings but contained `${pp.value()}`."
              )
          }.toList
        case _ =>
          value.writeTo(pp)
          throw new ParameterWrongTypeException(
            s"Expected parameter `$$${p.name}` to be a non-empty String or a non-empty List of non-empty Strings but was `${pp.value()}`."
          )
      }
    case _ =>
      // this fails in parsing or semantic checking, but is needed for scala warnings
      throw new InvalidArgumentException(
        s"Expected non-empty String or non-empty List of non-empty Strings but was `${field.asCanonicalStringVal}`."
      )
  }

  private def validateAuthId(id: String): Unit =
    if (id.isEmpty) throw new InvalidArgumentException("Invalid input. Auth id is not allowed to be an empty string.")

  case class RenamingStringParameterConverter(
    parameter: String,
    rename: String => String = identity,
    valueMapper: TextValue => TextValue = identity
  ) extends ((Transaction, MapValue) => MapValue) {

    def apply(transaction: Transaction, params: MapValue): MapValue = {
      val paramValue = params.get(parameter)
      // Check the parameter is actually the expected type
      if (!paramValue.isInstanceOf[TextValue]) {
        throw new ParameterWrongTypeException(
          s"Expected parameter $$$parameter to have type String but was $paramValue"
        )
      } else params.updatedWith(rename(parameter), valueMapper(params.get(parameter).asInstanceOf[TextValue]))
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
    wasParameter: Boolean,
    override val nameConverter: (Transaction, MapValue) => MapValue
  ) extends NameConverter {

    val keys: Array[String] = Array(nameKey, namespaceKey, displayNameKey)
    val values: Array[AnyValue] = Array(nameValue, namespaceValue, displayNameValue)

    def asNodeFilter: String = s"{$NAME_PROPERTY: $$`$nameKey`, $NAMESPACE_PROPERTY: $$`$namespaceKey`}"

  }

  type Show[T] = (T, MapValue) => String

  object Show {
    implicit val showDatabaseName: Show[DatabaseName] = (databaseName, p) => runtimeStringValue(databaseName, p)
    implicit val showString: Show[Either[String, Parameter]] = (s, p) => runtimeStringValue(s, p)
  }

  /*
   * This is a bit of a kludge to get around database names being ambiguous in 5.0 for backward
   * compatibility. We assume that 'db.name' means 'name' in composite 'db' but in case db does
   * not exist we need to rewrite the parameters to mean 'db.name' in the default namespace. Also flag
   * this usage as deprecated.
   */
  def checkNamespaceExists(aliasNameFields: DatabaseNameFields)(
    tx: Transaction,
    params: MapValue
  ): (MapValue, Set[InternalNotification]) = {

    def paramString(key: String) = params.get(key).asInstanceOf[StringValue].stringValue()

    if (paramString(aliasNameFields.namespaceKey) != DEFAULT_NAMESPACE) {
      // Check to see if there is a composite database node for this alias
      // MATCH (dbname:DatabaseName{name: name})-[:TARGETS]->(:CompositeDatabase) WHERE dbname.namespace = namespace
      val compositeDatabaseExists = Using.resource(tx.findNodes(
        DATABASE_NAME_LABEL,
        DATABASE_NAME_PROPERTY,
        paramString(aliasNameFields.namespaceKey)
      )) { nodes =>
        nodes.asScala.exists(n =>
          n.getProperty(NAMESPACE_PROPERTY).equals(DEFAULT_NAMESPACE) && n.getSingleRelationship(
            TARGETS_RELATIONSHIP,
            Direction.OUTGOING
          ).getEndNode.hasLabel(COMPOSITE_DATABASE_LABEL)
        )
      }
      if (!compositeDatabaseExists) {
        val aliasName = paramString(aliasNameFields.namespaceKey) + "." + paramString(aliasNameFields.nameKey)
        // This is just a regular local alias with . in the name, so use the default namespace
        (
          params.updatedWith(
            aliasNameFields.nameKey,
            Values.utf8Value(aliasName)
          )
            .updatedWith(aliasNameFields.namespaceKey, Values.utf8Value(DEFAULT_NAMESPACE)),
          if (aliasNameFields.wasParameter) Set.empty else Set(DeprecatedDatabaseNameNotification(aliasName, None))
        )
      } else {
        (params, Set.empty)
      }
    } else {
      (params, Set.empty)
    }
  }
}
