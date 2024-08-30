/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
parser grammar Cypher5Parser;


options { tokenVocab = Cypher5Lexer; }
statements
   : statement (SEMICOLON statement)* SEMICOLON? EOF
   ;

statement
   : periodicCommitQueryHintFailure? (command | regularQuery)
   ;

periodicCommitQueryHintFailure
   : USING PERIODIC COMMIT UNSIGNED_DECIMAL_INTEGER?
   ;

regularQuery
   : singleQuery (UNION (ALL | DISTINCT)? singleQuery)*
   ;

singleQuery
   : clause+
   ;

clause
   : useClause
   | finishClause
   | returnClause
   | createClause
   | insertClause
   | deleteClause
   | setClause
   | removeClause
   | matchClause
   | mergeClause
   | withClause
   | unwindClause
   | callClause
   | subqueryClause
   | loadCSVClause
   | foreachClause
   | orderBySkipLimitClause
   ;

useClause
   : USE GRAPH? graphReference
   ;

graphReference
   : LPAREN graphReference RPAREN
   | functionInvocation
   | symbolicAliasName
   ;

finishClause
   : FINISH
   ;

returnClause
   : RETURN returnBody
   ;

returnBody
   : DISTINCT? returnItems orderBy? skip? limit?
   ;

returnItem
   : expression (AS variable)?
   ;

returnItems
   : (TIMES | returnItem) (COMMA returnItem)*
   ;

orderItem
   : expression (ascToken | descToken)?
   ;

ascToken
   : ASC | ASCENDING
   ;

descToken
   : DESC | DESCENDING
   ;

orderBy
   : ORDER BY orderItem (COMMA orderItem)*
   ;

skip
   : (OFFSET | SKIPROWS) expression
   ;

limit
   : LIMITROWS expression
   ;

whereClause
   : WHERE expression
   ;

withClause
   : WITH returnBody whereClause?
   ;

createClause
   : CREATE patternList
   ;

insertClause
   : INSERT insertPatternList
   ;

setClause
   : SET setItem (COMMA setItem)*
   ;

setItem
   : propertyExpression EQ expression        # SetProp
   | dynamicPropertyExpression EQ expression # SetDynamicProp
   | variable EQ expression                  # SetProps
   | variable PLUSEQUAL expression           # AddProp
   | variable nodeLabels                     # SetLabels
   | variable nodeLabelsIs                   # SetLabelsIs
   ;

removeClause
   : REMOVE removeItem (COMMA removeItem)*
   ;

removeItem
   : propertyExpression         # RemoveProp
   | dynamicPropertyExpression  # RemoveDynamicProp
   | variable nodeLabels        # RemoveLabels
   | variable nodeLabelsIs      # RemoveLabelsIs
   ;

deleteClause
   : (DETACH | NODETACH)? DELETE expression (COMMA expression)*
   ;

matchClause
   : OPTIONAL? MATCH matchMode? patternList hint* whereClause?
   ;

matchMode
   : REPEATABLE (ELEMENT BINDINGS? | ELEMENTS)
   | DIFFERENT (RELATIONSHIP BINDINGS? | RELATIONSHIPS)
   ;

hint
   : USING (((
      INDEX
      | BTREE INDEX
      | TEXT INDEX
      | RANGE INDEX
      | POINT INDEX
   ) SEEK? variable labelOrRelType LPAREN nonEmptyNameList RPAREN)
   | JOIN ON nonEmptyNameList
   | SCAN variable labelOrRelType
   )
   ;

mergeClause
   : MERGE pattern mergeAction*
   ;

mergeAction
   : ON (MATCH | CREATE) setClause
   ;

unwindClause
   : UNWIND expression AS variable
   ;

callClause
   : CALL procedureName (LPAREN (procedureArgument (COMMA procedureArgument)*)? RPAREN)? (YIELD (TIMES | procedureResultItem (COMMA procedureResultItem)* whereClause?))?
   ;

procedureName
   : namespace symbolicNameString
   ;

procedureArgument
   : expression
   ;

procedureResultItem
   : symbolicNameString (AS variable)?
   ;

loadCSVClause
   : LOAD CSV (WITH HEADERS)? FROM expression AS variable (FIELDTERMINATOR stringLiteral)?
   ;

foreachClause
   : FOREACH LPAREN variable IN expression BAR clause+ RPAREN
   ;

subqueryClause
   : CALL subqueryScope? LCURLY regularQuery RCURLY subqueryInTransactionsParameters?
   ;

subqueryScope
   : LPAREN (TIMES | variable (COMMA variable)*)? RPAREN
   ;

subqueryInTransactionsParameters
   : IN (expression? CONCURRENT)? TRANSACTIONS (subqueryInTransactionsBatchParameters | subqueryInTransactionsErrorParameters | subqueryInTransactionsReportParameters)*
   ;

subqueryInTransactionsBatchParameters
   : OF expression (ROW | ROWS)
   ;

subqueryInTransactionsErrorParameters
   : ON ERROR (CONTINUE | BREAK | FAIL)
   ;

subqueryInTransactionsReportParameters
   : REPORT STATUS AS variable
   ;

orderBySkipLimitClause
   : orderBy skip? limit?
   | skip limit?
   | limit
   ;

patternList
   : pattern (COMMA pattern)*
   ;

insertPatternList
   : insertPattern (COMMA insertPattern)*
   ;

pattern
   : (variable EQ)? selector? anonymousPattern
   ;

insertPattern
   : (symbolicNameString EQ)? insertNodePattern (insertRelationshipPattern insertNodePattern)*
   ;

quantifier
   : LCURLY UNSIGNED_DECIMAL_INTEGER RCURLY
   | LCURLY from = UNSIGNED_DECIMAL_INTEGER? COMMA to = UNSIGNED_DECIMAL_INTEGER? RCURLY
   | PLUS
   | TIMES
   ;

anonymousPattern
   : shortestPathPattern
   | patternElement
   ;

shortestPathPattern
   : (SHORTEST_PATH | ALL_SHORTEST_PATHS) LPAREN patternElement RPAREN
   ;

patternElement
   : (nodePattern (relationshipPattern quantifier? nodePattern)* | parenthesizedPath)+
   ;

selector
   : ANY SHORTEST pathToken?                                  # AnyShortestPath
   | ALL SHORTEST pathToken?                                  # AllShortestPath
   | ANY UNSIGNED_DECIMAL_INTEGER? pathToken?                 # AnyPath
   | ALL pathToken?                                           # AllPath
   | SHORTEST UNSIGNED_DECIMAL_INTEGER? pathToken? groupToken # ShortestGroup
   | SHORTEST UNSIGNED_DECIMAL_INTEGER pathToken?             # AnyShortestPath
   ;

groupToken
   : GROUP | GROUPS
   ;

pathToken
   : PATH | PATHS
   ;

pathPatternNonEmpty
   : nodePattern (relationshipPattern nodePattern)+
   ;

nodePattern
   : LPAREN variable? labelExpression? properties? (WHERE expression)? RPAREN
   ;

insertNodePattern
   : LPAREN variable? insertNodeLabelExpression? map? RPAREN
   ;

parenthesizedPath
   : LPAREN pattern (WHERE expression)? RPAREN quantifier?
   ;

nodeLabels
   : (labelType | dynamicLabelType)+
   ;

nodeLabelsIs
   : IS (symbolicNameString | dynamicExpression) (labelType | dynamicLabelType)*
   ;

dynamicExpression
   : DOLLAR LPAREN expression RPAREN
   ;

dynamicLabelType
   : COLON dynamicExpression
   ;

labelType
   : COLON symbolicNameString
   ;

relType
   : COLON symbolicNameString
   ;

labelOrRelType
   : COLON symbolicNameString
   ;

properties
   : map
   | parameter["ANY"]
   ;

relationshipPattern
   : leftArrow? arrowLine (LBRACKET variable? labelExpression? pathLength? properties? (WHERE expression)? RBRACKET)? arrowLine rightArrow?
   ;

insertRelationshipPattern
   : leftArrow? arrowLine LBRACKET variable? insertRelationshipLabelExpression map? RBRACKET arrowLine rightArrow?
   ;

leftArrow
   : LT
   | ARROW_LEFT_HEAD
   ;

arrowLine
   : ARROW_LINE
   | MINUS
   ;

rightArrow
   : GT
   | ARROW_RIGHT_HEAD
   ;

pathLength
   : TIMES (from = UNSIGNED_DECIMAL_INTEGER? DOTDOT to = UNSIGNED_DECIMAL_INTEGER? | single = UNSIGNED_DECIMAL_INTEGER)?
   ;

labelExpression
   : COLON labelExpression4
   | IS labelExpression4Is
   ;

labelExpression4
   : labelExpression3 (BAR COLON? labelExpression3)*
   ;

labelExpression4Is
   : labelExpression3Is (BAR COLON? labelExpression3Is)*
   ;

labelExpression3
   : labelExpression2 ((AMPERSAND | COLON) labelExpression2)*
   ;

labelExpression3Is
   : labelExpression2Is ((AMPERSAND | COLON) labelExpression2Is)*
   ;

labelExpression2
   : EXCLAMATION_MARK* labelExpression1
   ;

labelExpression2Is
   : EXCLAMATION_MARK* labelExpression1Is
   ;

labelExpression1
   : LPAREN labelExpression4 RPAREN #ParenthesizedLabelExpression
   | PERCENT                        #AnyLabel
   | symbolicNameString             #LabelName
   ;

labelExpression1Is
   : LPAREN labelExpression4Is RPAREN #ParenthesizedLabelExpressionIs
   | PERCENT                          #AnyLabelIs
   | symbolicLabelNameString          #LabelNameIs
   ;

insertNodeLabelExpression
   : (COLON | IS) symbolicNameString ((AMPERSAND | COLON) symbolicNameString)*
   ;

insertRelationshipLabelExpression
   : (COLON | IS) symbolicNameString
   ;

expression
   : expression11 (OR expression11)*
   ;

expression11
   : expression10 (XOR expression10)*
   ;

expression10
   : expression9 (AND expression9)*
   ;

expression9
   : NOT* expression8
   ;

// Making changes here? Consider looking at extendedWhen too.
expression8
   : expression7 ((
      EQ
      | INVALID_NEQ
      | NEQ
      | LE
      | GE
      | LT
      | GT
   ) expression7)*
   ;

expression7
   : expression6 comparisonExpression6?
   ;

// Making changes here? Consider looking at extendedWhen too.
comparisonExpression6
   : (
      REGEQ
      | STARTS WITH
      | ENDS WITH
      | CONTAINS
      | IN
   ) expression6                                      # StringAndListComparison
   | IS NOT? NULL                                     # NullComparison
   | (IS NOT? (TYPED | COLONCOLON) | COLONCOLON) type # TypeComparison
   | IS NOT? normalForm? NORMALIZED                   # NormalFormComparison
   ;

normalForm
   : NFC
   | NFD
   | NFKC
   | NFKD
   ;

expression6
   : expression5 ((PLUS | MINUS | DOUBLEBAR) expression5)*
   ;

expression5
   : expression4 ((TIMES | DIVIDE | PERCENT) expression4)*
   ;

expression4
   : expression3 (POW expression3)*
   ;

expression3
   : expression2
   | (PLUS | MINUS) expression2
   ;

expression2
   : expression1 postFix*
   ;

postFix
   : property                                                           # PropertyPostfix
   | labelExpression                                                    # LabelPostfix
   | LBRACKET expression RBRACKET                                       # IndexPostfix
   | LBRACKET fromExp = expression? DOTDOT toExp = expression? RBRACKET # RangePostfix
   ;

property
   : DOT propertyKeyName
   ;

dynamicProperty
   : LBRACKET expression RBRACKET
   ;

propertyExpression
   : expression1 property+
   ;

dynamicPropertyExpression
   : expression1 dynamicProperty
   ;

expression1
   : literal
   | parameter["ANY"]
   | caseExpression
   | extendedCaseExpression
   | countStar
   | existsExpression
   | countExpression
   | collectExpression
   | mapProjection
   | listComprehension
   | listLiteral
   | patternComprehension
   | reduceExpression
   | listItemsPredicate
   | normalizeFunction
   | trimFunction
   | patternExpression
   | shortestPathExpression
   | parenthesizedExpression
   | functionInvocation
   | variable
   ;

literal
   : numberLiteral # NummericLiteral
   | stringLiteral # StringsLiteral
   | map           # OtherLiteral
   | TRUE          # BooleanLiteral
   | FALSE         # BooleanLiteral
   | INF           # KeywordLiteral
   | INFINITY      # KeywordLiteral
   | NAN           # KeywordLiteral
   | NULL          # KeywordLiteral
   ;

caseExpression
   : CASE caseAlternative+ (ELSE expression)? END
   ;

caseAlternative
   : WHEN expression THEN expression
   ;

extendedCaseExpression
   : CASE expression extendedCaseAlternative+ (ELSE elseExp = expression)? END
   ;

extendedCaseAlternative
   : WHEN extendedWhen (COMMA extendedWhen)* THEN expression
   ;

// Making changes here? Consider looking at comparisonExpression6 and expression8 too.
extendedWhen
   : (REGEQ | STARTS WITH | ENDS WITH) expression6 # WhenStringOrList
   | IS NOT? NULL                                  # WhenNull
   | (IS NOT? TYPED | COLONCOLON) type             # WhenType
   | IS NOT? normalForm? NORMALIZED                # WhenForm
   | (
      EQ
      | NEQ
      | INVALID_NEQ
      | LE
      | GE
      | LT
      | GT
   ) expression7                                   # WhenComparator
   | expression                                    # WhenEquals
   ;

// Observe that this is not possible to write as:
// (WHERE whereExp = expression)? (BAR barExp = expression)? RBRACKET
// Due to an ambigouity with cases such as [node IN nodes WHERE node:A|B]
// where |B will be interpreted as part of the whereExp, rather than as the expected barExp.
listComprehension
   : LBRACKET variable IN expression ((WHERE whereExp = expression)? BAR barExp = expression | (WHERE whereExp = expression)?) RBRACKET
   ;

patternComprehension
   : LBRACKET (variable EQ)? pathPatternNonEmpty (WHERE whereExp = expression)? BAR barExp = expression RBRACKET
   ;

reduceExpression
   : REDUCE LPAREN variable EQ expression COMMA variable IN expression BAR expression RPAREN
   ;

listItemsPredicate
   : (
      ALL
      | ANY
      | NONE
      | SINGLE
   ) LPAREN variable IN inExp = expression (WHERE whereExp = expression)? RPAREN
   ;

normalizeFunction
   : NORMALIZE LPAREN expression (COMMA normalForm)? RPAREN
   ;

trimFunction
   : TRIM LPAREN ((BOTH | LEADING | TRAILING)? (trimCharacterString = expression)? FROM)? trimSource = expression RPAREN
   ;

patternExpression
   : pathPatternNonEmpty
   ;

shortestPathExpression
   : shortestPathPattern
   ;

parenthesizedExpression
   : LPAREN expression RPAREN
   ;

mapProjection
   : variable LCURLY (mapProjectionElement (COMMA mapProjectionElement)* )? RCURLY
   ;

mapProjectionElement
   : propertyKeyName COLON expression
   | property
   | variable
   | DOT TIMES
   ;

countStar
   : COUNT LPAREN TIMES RPAREN
   ;

existsExpression
   : EXISTS LCURLY (regularQuery | matchMode? patternList whereClause?) RCURLY
   ;

countExpression
   : COUNT LCURLY (regularQuery | matchMode? patternList whereClause?) RCURLY
   ;

collectExpression
   : COLLECT LCURLY regularQuery RCURLY
   ;

numberLiteral
   : MINUS? (
      DECIMAL_DOUBLE
      | UNSIGNED_DECIMAL_INTEGER
      | UNSIGNED_HEX_INTEGER
      | UNSIGNED_OCTAL_INTEGER
   )
   ;

signedIntegerLiteral
   : MINUS? UNSIGNED_DECIMAL_INTEGER
   ;

listLiteral
   : LBRACKET (expression (COMMA expression)* )? RBRACKET
   ;

propertyKeyName
   : symbolicNameString
   ;

parameter[String paramType]
   : DOLLAR parameterName[paramType]
   ;

parameterName[String paramType]
   : (symbolicNameString | UNSIGNED_DECIMAL_INTEGER)
   ;

functionInvocation
   : functionName LPAREN (DISTINCT | ALL)? (functionArgument (COMMA functionArgument)* )? RPAREN
   ;

functionArgument
   : expression
   ;

functionName
   : namespace symbolicNameString
   ;

namespace
   : (symbolicNameString DOT)*
   ;

variable
   : symbolicNameString
   ;

// Returns non-list of propertyKeyNames
nonEmptyNameList
   : symbolicNameString (COMMA symbolicNameString)*
   ;

type
   : typePart (BAR typePart)*
   ;

typePart
   : typeName typeNullability? typeListSuffix*
   ;

typeName
   // Note! These are matched based on the first token. Take precaution in ExpressionBuilder.scala when modifying
   : NOTHING
   | NULL
   | BOOL
   | BOOLEAN
   | VARCHAR
   | STRING
   | INT
   | SIGNED? INTEGER
   | FLOAT
   | DATE
   | LOCAL (TIME | DATETIME)
   | ZONED (TIME | DATETIME)
   | TIME (WITHOUT | WITH) (TIMEZONE | TIME ZONE)
   | TIMESTAMP (WITHOUT | WITH) (TIMEZONE | TIME ZONE)
   | DURATION
   | POINT
   | NODE
   | VERTEX
   | RELATIONSHIP
   | EDGE
   | MAP
   | (LIST | ARRAY) LT type GT
   | PATH
   | PATHS
   | PROPERTY VALUE
   | ANY (
      NODE
      | VERTEX
      | RELATIONSHIP
      | EDGE
      | MAP
      | PROPERTY VALUE
      | VALUE? LT type GT
      | VALUE
   )?
   ;

typeNullability
   : NOT NULL
   | EXCLAMATION_MARK
   ;

typeListSuffix
   : (LIST | ARRAY) typeNullability?
   ;

// Show, terminate, schema and admin commands

command
   : useClause? (
      createCommand
      | dropCommand
      | alterCommand
      | renameCommand
      | denyCommand
      | revokeCommand
      | grantCommand
      | startDatabase
      | stopDatabase
      | enableServerCommand
      | allocationCommand
      | showCommand
      | terminateCommand
   )
   ;

createCommand
   : CREATE (OR REPLACE)? (
      createAlias
      | createCompositeDatabase
      | createConstraint
      | createDatabase
      | createIndex
      | createRole
      | createUser
   )
   ;

dropCommand
   : DROP (
      dropAlias
      | dropConstraint
      | dropDatabase
      | dropIndex
      | dropRole
      | dropServer
      | dropUser
   )
   ;

showCommand
   : SHOW (
      showAliases
      | showConstraintCommand
      | showCurrentUser
      | showDatabase
      | showFunctions
      | showIndexCommand
      | showPrivileges
      | showProcedures
      | showRolePrivileges
      | showRoles
      | showServers
      | showSettings
      | showSupportedPrivileges
      | showTransactions
      | showUserPrivileges
      | showUsers
   )
   ;

showCommandYield
   : yieldClause returnClause?
   | whereClause
   ;

yieldItem
   : variable (AS variable)?
   ;

yieldSkip
   : (OFFSET | SKIPROWS) signedIntegerLiteral
   ;

yieldLimit
   : LIMITROWS signedIntegerLiteral
   ;

yieldClause
   : YIELD (TIMES | yieldItem (COMMA yieldItem)*) orderBy? yieldSkip? yieldLimit? whereClause?
   ;

commandOptions
   : OPTIONS mapOrParameter
   ;

// Non-admin show and terminate commands

terminateCommand
   : TERMINATE terminateTransactions
   ;

composableCommandClauses
   : terminateCommand
   | composableShowCommandClauses
   ;

composableShowCommandClauses
   : SHOW (
      showIndexCommand
      | showConstraintCommand
      | showFunctions
      | showProcedures
      | showSettings
      | showTransactions
   )
   ;

showBriefAndYield
   : (BRIEF | VERBOSE) OUTPUT?
   | yieldClause returnClause?
   | whereClause
   ;

showIndexCommand
   : (
      FULLTEXT
      | LOOKUP
      | POINT
      | RANGE
      | TEXT
      | VECTOR
   ) showIndexesNoBrief
   | (ALL | BTREE)? showIndexesAllowBrief
   ;

showIndexesAllowBrief
   : indexToken showBriefAndYield? composableCommandClauses?
   ;

showIndexesNoBrief
   : indexToken showCommandYield? composableCommandClauses?
   ;

showConstraintCommand
   : (NODE | RELATIONSHIP | REL)? constraintAllowYieldType showConstraintsAllowYield # ShowConstraintMulti
   | (NODE | RELATIONSHIP | REL) UNIQUE showConstraintsAllowYield                    # ShowConstraintUnique
   | (RELATIONSHIP | REL)? KEY showConstraintsAllowYield                             # ShowConstraintKey
   | REL EXIST showConstraintsAllowYield                                             # ShowConstraintRelExist
   | (NODE | RELATIONSHIP)? EXISTS showConstraintsAllowBrief                         # ShowConstraintOldExists
   | constraintBriefAndYieldType? showConstraintsAllowBriefAndYield                  # ShowConstraintBriefAndYield
   ;

constraintAllowYieldType
   : UNIQUENESS
   | constraintExistType
   | PROPERTY TYPE
   ;

constraintExistType
   : EXISTENCE
   | PROPERTY EXISTENCE
   | PROPERTY EXIST
   ;

constraintBriefAndYieldType
   : ALL
   | UNIQUE
   | EXIST
   | NODE KEY
   | NODE EXIST
   | RELATIONSHIP EXIST
   ;

showConstraintsAllowBriefAndYield
   : constraintToken showBriefAndYield? composableCommandClauses?
   ;

showConstraintsAllowBrief
   : constraintToken ((BRIEF | VERBOSE) OUTPUT?)? composableCommandClauses?
   ;

showConstraintsAllowYield
   : constraintToken showCommandYield? composableCommandClauses?
   ;

showProcedures
   : (PROCEDURE | PROCEDURES) executableBy? showCommandYield? composableCommandClauses?
   ;

showFunctions
   : showFunctionsType? functionToken executableBy? showCommandYield? composableCommandClauses?
   ;

functionToken
   : FUNCTION | FUNCTIONS
   ;

executableBy
   : EXECUTABLE (BY (CURRENT USER | symbolicNameString))?
   ;

showFunctionsType
   : ALL
   | BUILT IN
   | USER DEFINED
   ;

showTransactions
   : transactionToken namesAndClauses
   ;

terminateTransactions
   : transactionToken namesAndClauses
   ;

showSettings
   : settingToken namesAndClauses
   ;

settingToken
   : SETTING | SETTINGS
   ;

namesAndClauses
   : (showCommandYield? | stringsOrExpression showCommandYield?) composableCommandClauses?
   ;

stringsOrExpression
   : stringList
   | expression
   ;

// Schema commands

commandNodePattern
   : LPAREN variable labelType RPAREN
   ;

commandRelPattern
   : LPAREN RPAREN leftArrow? arrowLine LBRACKET variable relType RBRACKET arrowLine rightArrow? LPAREN RPAREN
   ;

createConstraint
   : CONSTRAINT symbolicNameOrStringParameter? (IF NOT EXISTS)? (ON | FOR) (commandNodePattern | commandRelPattern) constraintType commandOptions?
   ;

constraintType
   : ASSERT EXISTS propertyList                                                  # ConstraintExists
   | (REQUIRE | ASSERT) propertyList (COLONCOLON | IS (TYPED | COLONCOLON)) type # ConstraintTyped
   | (REQUIRE | ASSERT) propertyList IS (NODE | RELATIONSHIP | REL)? UNIQUE      # ConstraintIsUnique
   | (REQUIRE | ASSERT) propertyList IS (NODE | RELATIONSHIP | REL)? KEY         # ConstraintKey
   | (REQUIRE | ASSERT) propertyList IS NOT NULL                                 # ConstraintIsNotNull
   ;

dropConstraint
   : CONSTRAINT (ON (commandNodePattern | commandRelPattern) ASSERT (EXISTS propertyList | propertyList IS (UNIQUE | NODE KEY | NOT NULL)) | symbolicNameOrStringParameter (IF EXISTS)?)
   ;

createIndex
   : BTREE INDEX createIndex_
   | RANGE INDEX createIndex_
   | TEXT INDEX createIndex_
   | POINT INDEX createIndex_
   | VECTOR INDEX createIndex_
   | LOOKUP INDEX createLookupIndex
   | FULLTEXT INDEX createFulltextIndex
   | INDEX (ON oldCreateIndex | createIndex_)
   ;

oldCreateIndex
   : labelType LPAREN nonEmptyNameList RPAREN
   ;

createIndex_
   : symbolicNameOrStringParameter? (IF NOT EXISTS)? FOR (commandNodePattern | commandRelPattern) ON propertyList commandOptions?
   ;

createFulltextIndex
   : symbolicNameOrStringParameter? (IF NOT EXISTS)? FOR (fulltextNodePattern | fulltextRelPattern) ON EACH LBRACKET enclosedPropertyList RBRACKET commandOptions?
   ;

fulltextNodePattern
   : LPAREN variable COLON symbolicNameString (BAR symbolicNameString)* RPAREN
   ;

fulltextRelPattern
   : LPAREN RPAREN leftArrow? arrowLine LBRACKET variable COLON symbolicNameString (BAR symbolicNameString)* RBRACKET arrowLine rightArrow? LPAREN RPAREN
   ;

createLookupIndex
   : symbolicNameOrStringParameter? (IF NOT EXISTS)? FOR (lookupIndexNodePattern | lookupIndexRelPattern) symbolicNameString LPAREN variable RPAREN commandOptions?
   ;

lookupIndexNodePattern
   : LPAREN variable RPAREN ON EACH
   ;

lookupIndexRelPattern
   : LPAREN RPAREN leftArrow? arrowLine LBRACKET variable RBRACKET arrowLine rightArrow? LPAREN RPAREN ON EACH?
   ;

dropIndex
   : INDEX (ON labelType LPAREN nonEmptyNameList RPAREN | symbolicNameOrStringParameter (IF EXISTS)?)
   ;

propertyList
   : variable property | LPAREN enclosedPropertyList RPAREN
   ;

enclosedPropertyList
   : variable property (COMMA variable property)*
   ;

// Admin commands

alterCommand
   : ALTER (
      alterAlias
      | alterCurrentUser
      | alterDatabase
      | alterUser
      | alterServer
   )
   ;

renameCommand
   : RENAME (renameRole | renameServer | renameUser)
   ;

grantCommand
   : GRANT (
      IMMUTABLE? privilege TO roleNames
      | roleToken grantRole
   )
   ;

denyCommand
   : DENY IMMUTABLE? privilege TO roleNames
   ;

revokeCommand
   : REVOKE (
      (DENY | GRANT)? IMMUTABLE? privilege FROM roleNames
      | roleToken revokeRole
   )
   ;

userNames
   : symbolicNameOrStringParameterList
   ;

roleNames
   : symbolicNameOrStringParameterList
   ;

roleToken
   : ROLES
   | ROLE
   ;

// Server commands

enableServerCommand
   : ENABLE SERVER stringOrParameter commandOptions?
   ;

alterServer
   : SERVER stringOrParameter SET commandOptions
   ;

renameServer
   : SERVER stringOrParameter TO stringOrParameter
   ;

dropServer
   : SERVER stringOrParameter
   ;

showServers
   : (SERVER | SERVERS) showCommandYield?
   ;

allocationCommand
   : DRYRUN? (deallocateDatabaseFromServers | reallocateDatabases)
   ;

deallocateDatabaseFromServers
   : DEALLOCATE (DATABASE | DATABASES) FROM (SERVER | SERVERS) stringOrParameter (COMMA stringOrParameter)*
   ;

reallocateDatabases
   : REALLOCATE (DATABASE | DATABASES)
   ;

// Role commands

createRole
   : ROLE commandNameExpression (IF NOT EXISTS)? (AS COPY OF commandNameExpression)?
   ;

dropRole
   : ROLE commandNameExpression (IF EXISTS)?
   ;

renameRole
   : ROLE commandNameExpression (IF EXISTS)? TO commandNameExpression
   ;

showRoles
   : (ALL | POPULATED)? roleToken (WITH (USER | USERS))? showCommandYield?
   ;

grantRole
   : roleNames TO userNames
   ;

revokeRole
   : roleNames FROM userNames
   ;

// User commands

createUser
   : USER commandNameExpression (IF NOT EXISTS)? (SET (
      password
      | PASSWORD passwordChangeRequired
      | userStatus
      | homeDatabase
      | setAuthClause
   ))+;

dropUser
   : USER commandNameExpression (IF EXISTS)?
   ;

renameUser
   : USER commandNameExpression (IF EXISTS)? TO commandNameExpression
   ;

alterCurrentUser
   : CURRENT USER SET PASSWORD FROM passwordExpression TO passwordExpression
   ;

alterUser
   : USER commandNameExpression (IF EXISTS)? (REMOVE (
      HOME DATABASE
      | ALL AUTH (PROVIDER | PROVIDERS)?
      | removeNamedProvider
   ))* (SET (
      password
      | PASSWORD passwordChangeRequired
      | userStatus
      | homeDatabase
      | setAuthClause
   ))*
   ;

removeNamedProvider
   : AUTH (PROVIDER | PROVIDERS)? (stringLiteral | stringListLiteral | parameter["ANY"])
   ;

password
   : (PLAINTEXT | ENCRYPTED)? PASSWORD passwordExpression passwordChangeRequired?
   ;

passwordOnly
   : (PLAINTEXT | ENCRYPTED)? PASSWORD passwordExpression
   ;

passwordExpression
   : stringLiteral
   | parameter["STRING"]
   ;

passwordChangeRequired
   : CHANGE NOT? REQUIRED
   ;

userStatus
   : STATUS (SUSPENDED | ACTIVE)
   ;

homeDatabase
   : HOME DATABASE symbolicAliasNameOrParameter
   ;

setAuthClause
   : AUTH PROVIDER? stringLiteral LCURLY (SET (
      userAuthAttribute
   ))+ RCURLY
   ;

userAuthAttribute
   : ID stringOrParameterExpression
   | passwordOnly
   | PASSWORD passwordChangeRequired
   ;

showUsers
   : (USER | USERS) (WITH AUTH)? showCommandYield?
   ;

showCurrentUser
   : CURRENT USER showCommandYield?
   ;

// Privilege commands

showSupportedPrivileges
   : SUPPORTED privilegeToken showCommandYield?
   ;

showPrivileges
   : ALL? privilegeToken privilegeAsCommand? showCommandYield?
   ;

showRolePrivileges
   : (ROLE | ROLES) roleNames privilegeToken privilegeAsCommand? showCommandYield?
   ;

showUserPrivileges
   : (USER | USERS) userNames? privilegeToken privilegeAsCommand? showCommandYield?
   ;

privilegeAsCommand
   : AS REVOKE? (COMMAND | COMMANDS)
   ;

privilegeToken
   : PRIVILEGE
   | PRIVILEGES
   ;

privilege
   : allPrivilege
   | createPrivilege
   | databasePrivilege
   | dbmsPrivilege
   | dropPrivilege
   | loadPrivilege
   | qualifiedGraphPrivileges
   | qualifiedGraphPrivilegesWithProperty
   | removePrivilege
   | setPrivilege
   | showPrivilege
   | writePrivilege
   ;

allPrivilege
   : ALL allPrivilegeType? ON allPrivilegeTarget
   ;

allPrivilegeType
   : (DATABASE | GRAPH | DBMS)? PRIVILEGES
   ;

allPrivilegeTarget
   : (DEFAULT | HOME) (DATABASE | GRAPH)                    # DefaultTarget
   | (DATABASE | DATABASES) (TIMES | symbolicAliasNameList) # DatabaseVariableTarget
   | (GRAPH | GRAPHS) (TIMES | symbolicAliasNameList)       # GraphVariableTarget
   | DBMS                                                   # DBMSTarget
   ;

createPrivilege
   : CREATE (
      createPrivilegeForDatabase ON databaseScope
      | actionForDBMS ON DBMS
      | ON graphScope graphQualifier
   )
   ;

createPrivilegeForDatabase
   : indexToken
   | constraintToken
   | createNodePrivilegeToken
   | createRelPrivilegeToken
   | createPropertyPrivilegeToken
   ;

createNodePrivilegeToken
   : NEW NODE? (LABEL | LABELS)
   ;

createRelPrivilegeToken
   : NEW RELATIONSHIP? (TYPE | TYPES)
   ;

createPropertyPrivilegeToken
   : NEW PROPERTY? (NAME | NAMES)
   ;

actionForDBMS
   : ALIAS
   | COMPOSITE? DATABASE
   | ROLE
   | USER
   ;

dropPrivilege
   : DROP (
      (indexToken | constraintToken) ON databaseScope
      | actionForDBMS ON DBMS
   )
   ;

loadPrivilege
   : LOAD ON (
      (URL | CIDR) stringOrParameter
      | ALL DATA
   )
   ;

showPrivilege
   : SHOW (
      (indexToken | constraintToken | transactionToken userQualifier?) ON databaseScope
      | (ALIAS | PRIVILEGE | ROLE | SERVER | SERVERS | settingToken settingQualifier | USER) ON DBMS
   )
   ;

setPrivilege
   : SET (
      (passwordToken | USER (STATUS | HOME DATABASE) | DATABASE ACCESS) ON DBMS
      | LABEL labelsResource ON graphScope
      | PROPERTY propertiesResource ON graphScope graphQualifier
      | AUTH ON DBMS
   )
   ;

passwordToken
   : PASSWORD
   | PASSWORDS
   ;

removePrivilege
   : REMOVE (
      (PRIVILEGE | ROLE) ON DBMS
      | LABEL labelsResource ON graphScope
   )
   ;

writePrivilege
   : WRITE ON graphScope
   ;

databasePrivilege
   : (
      ACCESS
      | START
      | STOP
      | (indexToken | constraintToken | NAME) MANAGEMENT?
      | (TRANSACTION MANAGEMENT? | TERMINATE transactionToken) userQualifier?
   )
   ON databaseScope
   ;

dbmsPrivilege
   : (
      ALTER (ALIAS | DATABASE | USER)
      | ASSIGN (PRIVILEGE | ROLE)
      | (ALIAS | COMPOSITE? DATABASE | PRIVILEGE | ROLE | SERVER | USER) MANAGEMENT
      | dbmsPrivilegeExecute
      | RENAME (ROLE | USER)
      | IMPERSONATE userQualifier?
   )
   ON DBMS
   ;

dbmsPrivilegeExecute
   : EXECUTE (
      adminToken PROCEDURES
      | BOOSTED? (
         procedureToken executeProcedureQualifier
         | (USER DEFINED?)? functionToken executeFunctionQualifier
      )
   )
   ;

adminToken
   : ADMIN
   | ADMINISTRATOR
   ;

procedureToken
   : PROCEDURE
   | PROCEDURES
   ;

indexToken
   : INDEX
   | INDEXES
   ;

constraintToken
   : CONSTRAINT
   | CONSTRAINTS
   ;

transactionToken
   : TRANSACTION
   | TRANSACTIONS
   ;

userQualifier
   : LPAREN (TIMES | userNames) RPAREN
   ;

executeFunctionQualifier
   : globs
   ;

executeProcedureQualifier
   : globs
   ;

settingQualifier
   : globs
   ;

globs
   : glob (COMMA glob)*
   ;

glob
   : escapedSymbolicNameString globRecursive?
   | globRecursive
   ;

globRecursive
   : globPart globRecursive?
   ;

globPart
   : DOT escapedSymbolicNameString?
   | QUESTION
   | TIMES
   | unescapedSymbolicNameString
   ;

qualifiedGraphPrivilegesWithProperty
   : (TRAVERSE | (READ | MATCH) propertiesResource) ON graphScope graphQualifier (LPAREN TIMES RPAREN)?
   ;

qualifiedGraphPrivileges
   : (DELETE | MERGE propertiesResource) ON graphScope graphQualifier
   ;

labelsResource
   : TIMES
   | nonEmptyStringList
   ;

propertiesResource
   : LCURLY (TIMES | nonEmptyStringList) RCURLY
   ;

// Returns non-empty list of strings
nonEmptyStringList
   : symbolicNameString (COMMA symbolicNameString)*
   ;

graphQualifier
   : (
      graphQualifierToken (TIMES | nonEmptyStringList)
      | FOR LPAREN variable? (COLON symbolicNameString (BAR symbolicNameString)*)? (RPAREN WHERE expression | (WHERE expression | map) RPAREN)
   )?
   ;

graphQualifierToken
   : relToken
   | nodeToken
   | elementToken
   ;

relToken
   : RELATIONSHIP
   | RELATIONSHIPS
   ;

elementToken
   : ELEMENT
   | ELEMENTS
   ;

nodeToken
   : NODE
   | NODES
   ;

databaseScope
   : (DEFAULT | HOME) DATABASE
   | (DATABASE | DATABASES) (TIMES | symbolicAliasNameList)
   ;

graphScope
   : (DEFAULT | HOME) GRAPH
   | (GRAPH | GRAPHS) (TIMES | symbolicAliasNameList)
   ;

// Database commands

createCompositeDatabase
   : COMPOSITE DATABASE symbolicAliasNameOrParameter (IF NOT EXISTS)? commandOptions? waitClause?
   ;

createDatabase
   : DATABASE symbolicAliasNameOrParameter (IF NOT EXISTS)? (TOPOLOGY (primaryTopology | secondaryTopology)+)? commandOptions? waitClause?
   ;

primaryTopology
   : UNSIGNED_DECIMAL_INTEGER primaryToken
   ;

primaryToken
   : PRIMARY | PRIMARIES
   ;

secondaryTopology
   : UNSIGNED_DECIMAL_INTEGER secondaryToken
   ;

secondaryToken
   : SECONDARY | SECONDARIES
   ;

dropDatabase
   : COMPOSITE? DATABASE symbolicAliasNameOrParameter (IF EXISTS)? ((DUMP | DESTROY) DATA)? waitClause?
   ;

alterDatabase
   : DATABASE symbolicAliasNameOrParameter (IF EXISTS)? (
      (SET (alterDatabaseAccess | alterDatabaseTopology | alterDatabaseOption))+
      | (REMOVE OPTION symbolicNameString)+
   ) waitClause?
   ;

alterDatabaseAccess
   : ACCESS READ (ONLY | WRITE)
   ;

alterDatabaseTopology
   : TOPOLOGY (primaryTopology | secondaryTopology)+
   ;

alterDatabaseOption
   : OPTION symbolicNameString expression
   ;

startDatabase
   : START DATABASE symbolicAliasNameOrParameter waitClause?
   ;

stopDatabase
   : STOP DATABASE symbolicAliasNameOrParameter waitClause?
   ;

waitClause
   : WAIT (UNSIGNED_DECIMAL_INTEGER secondsToken?)?
   | NOWAIT
   ;

secondsToken
   : SEC | SECOND | SECONDS;

showDatabase
   : (DEFAULT | HOME) DATABASE showCommandYield?
   | (DATABASE | DATABASES) symbolicAliasNameOrParameter? showCommandYield?
   ;

// Alias commands

createAlias
   : ALIAS symbolicAliasNameOrParameter (IF NOT EXISTS)? FOR DATABASE symbolicAliasNameOrParameter (AT stringOrParameter USER commandNameExpression PASSWORD passwordExpression (DRIVER mapOrParameter)?)? (PROPERTIES mapOrParameter)?
   ;

dropAlias
   : ALIAS symbolicAliasNameOrParameter (IF EXISTS)? FOR DATABASE
   ;

alterAlias
   : ALIAS symbolicAliasNameOrParameter (IF EXISTS)? SET DATABASE (
      alterAliasTarget
      | alterAliasUser
      | alterAliasPassword
      | alterAliasDriver
      | alterAliasProperties
   )+
   ;

alterAliasTarget
   : TARGET symbolicAliasNameOrParameter (AT stringOrParameter)?
   ;

alterAliasUser
   : USER commandNameExpression
   ;

alterAliasPassword
   : PASSWORD passwordExpression
   ;

alterAliasDriver
   : DRIVER mapOrParameter
   ;

alterAliasProperties
   : PROPERTIES mapOrParameter
   ;

showAliases
   : (ALIAS | ALIASES) symbolicAliasNameOrParameter? FOR (DATABASE | DATABASES) showCommandYield?
   ;

// Various strings, symbolic names, lists and maps

// Should return an Either[String, Parameter]
symbolicNameOrStringParameter
   : symbolicNameString
   | parameter["STRING"]
   ;

// Should return an Expression
commandNameExpression
   : symbolicNameString
   | parameter["STRING"]
   ;

symbolicNameOrStringParameterList
   : commandNameExpression (COMMA commandNameExpression)*
   ;

symbolicAliasNameList
   : symbolicAliasNameOrParameter (COMMA symbolicAliasNameOrParameter)*
   ;

symbolicAliasNameOrParameter
   : symbolicAliasName
   | parameter["STRING"]
   ;

symbolicAliasName
   : symbolicNameString (DOT symbolicNameString)*
   ;

stringListLiteral
   : LBRACKET (stringLiteral (COMMA stringLiteral)*)? RBRACKET
   ;

stringList
   : stringLiteral (COMMA stringLiteral)+
   ;

stringLiteral
   : STRING_LITERAL1
   | STRING_LITERAL2
   ;

// Should return an Expression
stringOrParameterExpression
   : stringLiteral
   | parameter["STRING"]
   ;

// Should return an Either[String, Parameter]
stringOrParameter
   : stringLiteral
   | parameter["STRING"]
   ;

mapOrParameter
   : map
   | parameter["MAP"]
   ;

map
   : LCURLY (propertyKeyName COLON expression (COMMA propertyKeyName COLON expression)*)? RCURLY
   ;

symbolicNameString
   : escapedSymbolicNameString
   | unescapedSymbolicNameString
   ;

escapedSymbolicNameString
   : ESCAPED_SYMBOLIC_NAME
   ;

unescapedSymbolicNameString
   : unescapedLabelSymbolicNameString
   | NOT
   | NULL
   | TYPED
   | NORMALIZED
   | NFC
   | NFD
   | NFKC
   | NFKD
   ;

symbolicLabelNameString
   : escapedSymbolicNameString
   | unescapedLabelSymbolicNameString
   ;

unescapedLabelSymbolicNameString
   : IDENTIFIER
   | ACCESS
   | ACTIVE
   | ADMIN
   | ADMINISTRATOR
   | ALIAS
   | ALIASES
   | ALL_SHORTEST_PATHS
   | ALL
   | ALTER
   | AND
   | ANY
   | ARRAY
   | AS
   | ASC
   | ASCENDING
   | ASSERT
   | ASSIGN
   | AT
   | AUTH
   | BINDINGS
   | BOOL
   | BOOLEAN
   | BOOSTED
   | BOTH
   | BREAK
   | BRIEF
   | BTREE
   | BUILT
   | BY
   | CALL
   | CASE
   | CHANGE
   | CIDR
   | COLLECT
   | COMMAND
   | COMMANDS
   | COMMIT
   | COMPOSITE
   | CONCURRENT
   | CONSTRAINT
   | CONSTRAINTS
   | CONTAINS
   | CONTINUE
   | COPY
   | COUNT
   | CREATE
   | CSV
   | CURRENT
   | DATA
   | DATABASE
   | DATABASES
   | DATE
   | DATETIME
   | DBMS
   | DEALLOCATE
   | DEFAULT
   | DEFINED
   | DELETE
   | DENY
   | DESC
   | DESCENDING
   | DESTROY
   | DETACH
   | DIFFERENT
   | DISTINCT
   | DRIVER
   | DROP
   | DRYRUN
   | DUMP
   | DURATION
   | EACH
   | EDGE
   | ELEMENT
   | ELEMENTS
   | ELSE
   | ENABLE
   | ENCRYPTED
   | END
   | ENDS
   | ERROR
   | EXECUTABLE
   | EXECUTE
   | EXIST
   | EXISTENCE
   | EXISTS
   | FAIL
   | FALSE
   | FIELDTERMINATOR
   | FINISH
   | FLOAT
   | FOREACH
   | FOR
   | FROM
   | FULLTEXT
   | FUNCTION
   | FUNCTIONS
   | GRANT
   | GRAPH
   | GRAPHS
   | GROUP
   | GROUPS
   | HEADERS
   | HOME
   | ID
   | IF
   | IMMUTABLE
   | IMPERSONATE
   | IN
   | INDEX
   | INDEXES
   | INF
   | INFINITY
   | INSERT
   | INT
   | INTEGER
   | IS
   | JOIN
   | KEY
   | LABEL
   | LABELS
   | LEADING
   | LIMITROWS
   | LIST
   | LOAD
   | LOCAL
   | LOOKUP
   | MATCH
   | MANAGEMENT
   | MAP
   | MERGE
   | NAME
   | NAMES
   | NAN
   | NEW
   | NODE
   | NODETACH
   | NODES
   | NONE
   | NORMALIZE
   | NOTHING
   | NOWAIT
   | OF
   | OFFSET
   | ON
   | ONLY
   | OPTIONAL
   | OPTIONS
   | OPTION
   | OR
   | ORDER
   | OUTPUT
   | PASSWORD
   | PASSWORDS
   | PATH
   | PATHS
   | PERIODIC
   | PLAINTEXT
   | POINT
   | POPULATED
   | PRIMARY
   | PRIMARIES
   | PRIVILEGE
   | PRIVILEGES
   | PROCEDURE
   | PROCEDURES
   | PROPERTIES
   | PROPERTY
   | PROVIDER
   | PROVIDERS
   | RANGE
   | READ
   | REALLOCATE
   | REDUCE
   | REL
   | RELATIONSHIP
   | RELATIONSHIPS
   | REMOVE
   | RENAME
   | REPEATABLE
   | REPLACE
   | REPORT
   | REQUIRE
   | REQUIRED
   | RETURN
   | REVOKE
   | ROLE
   | ROLES
   | ROW
   | ROWS
   | SCAN
   | SECONDARY
   | SECONDARIES
   | SEC
   | SECOND
   | SECONDS
   | SEEK
   | SERVER
   | SERVERS
   | SET
   | SETTING
   | SETTINGS
   | SHORTEST
   | SHORTEST_PATH
   | SHOW
   | SIGNED
   | SINGLE
   | SKIPROWS
   | START
   | STARTS
   | STATUS
   | STOP
   | VARCHAR
   | STRING
   | SUPPORTED
   | SUSPENDED
   | TARGET
   | TERMINATE
   | TEXT
   | THEN
   | TIME
   | TIMESTAMP
   | TIMEZONE
   | TO
   | TOPOLOGY
   | TRAILING
   | TRANSACTION
   | TRANSACTIONS
   | TRAVERSE
   | TRIM
   | TRUE
   | TYPE
   | TYPES
   | UNION
   | UNIQUE
   | UNIQUENESS
   | UNWIND
   | URL
   | USE
   | USER
   | USERS
   | USING
   | VALUE
   | VECTOR
   | VERBOSE
   | VERTEX
   | WAIT
   | WHEN
   | WHERE
   | WITH
   | WITHOUT
   | WRITE
   | XOR
   | YIELD
   | ZONE
   | ZONED
   ;

endOfFile
   : EOF
   ;
