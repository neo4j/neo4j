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
parser grammar Cypher25Parser;


options { tokenVocab = Cypher25Lexer; }
statements
   : statement (SEMICOLON statement)* SEMICOLON? EOF
   ;

statement
   : command | queryWithLocalDefinitions
   ;

queryWithLocalDefinitions
   : (DEFINE localDefinition)* nextStatement
   ;

localDefinition
   : PROCEDURE localProcedureDefinition
   | FUNCTION localFunctionDefinition
   ;

localProcedureDefinition
   : procedureName localInputFieldsSignature (typed outputType = localOutputFieldsSignature)? LCURLY queryWithLocalDefinitions RCURLY
   ;

localFunctionDefinition
   : functionName localInputFieldsSignature (typed outputType = type)? localFunctionBody
   ;

localInputFieldsSignature
    : LPAREN (localOptionalFieldSignature ( COMMA localOptionalFieldSignature )*)? RPAREN
    ;

localOutputFieldsSignature
    : LPAREN (localMandatoryFieldSignature ( COMMA localMandatoryFieldSignature )*)? RPAREN
    ;

localMandatoryFieldSignature
    : symbolicNameString (typed? type)?
    ;

localOptionalFieldSignature
    : symbolicNameString (typed? type)? (EQ expression)?
    ;

localFunctionBody
   : EQ expression                           # ExpressionBody
   | LCURLY queryWithLocalDefinitions RCURLY # QueryBody
   ;

nextStatement
   : regularQuery (NEXT regularQuery)*
   ;

regularQuery
   : union | when
   ;

union
   : singleQuery (UNION (ALL | DISTINCT)? singleQuery)*
   ;

when
   : whenBranch+ elseBranch?
   ;

whenBranch
   : WHEN expression THEN singleQuery
   ;

elseBranch
   : ELSE singleQuery
   ;

singleQuery
   : clause+
   | useClause? LCURLY queryWithLocalDefinitions RCURLY
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
   | filterClause
   | unwindClause
   | forListClause
   | letClause
   | callClause
   | subqueryClause
   | loadCSVClause
   | foreachClause
   | orderBySkipLimitClause
   | composableCommandClauses
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
   : (DISTINCT | ALL)? returnItems groupBy? orderBy? skip? limit?
   ;

returnItem
   : expression (AS variable)?
   ;

returnItems
   : (TIMES | returnItem) (COMMA returnItem)*
   ;

groupBy
   : GROUP BY (LPAREN RPAREN | ALL | expression (COMMA expression)*)
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

searchClause
   : SEARCH variable IN LPAREN (FULLTEXT | VECTOR) indexSpecificationClause forClause analyzerClause? whereClause? skip? limit RPAREN scoreClause?
   ;

indexSpecificationClause
   : INDEX commandNameExpression
   ;

forClause
   : FOR expression
   ;

analyzerClause
   : WITH ANALYZER expression
   ;

scoreClause
  : SCORE AS variable
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
   : variable EQ expression         # SetProps
   | variable PLUSEQUAL expression  # AddProp
   | variable nodeLabels            # SetLabels
   | variable nodeLabelsIs          # SetLabelsIs
   | expression2 EQ expression      # SetProp
   ;

removeClause
   : REMOVE removeItem (COMMA removeItem)*
   ;

removeItem
   : variable nodeLabels    # RemoveLabels
   | variable nodeLabelsIs  # RemoveLabelsIs
   | expression2            # RemoveProp
   ;

deleteClause
   : (DETACH | NODETACH)? DELETE expression (COMMA expression)*
   ;

matchClause
   : OPTIONAL? MATCH matchMode? patternList hint* (whereClause? searchClause? | searchClause whereClause)
   ;

matchMode
   : REPEATABLE (ELEMENT BINDINGS? | ELEMENTS)
   | DIFFERENT (RELATIONSHIP BINDINGS? | RELATIONSHIPS)
   ;

hint
   : USING (
     ((
        INDEX
        | TEXT INDEX
        | RANGE INDEX
        | POINT INDEX
     ) SEEK? variable labelOrRelType LPAREN nonEmptyNameList RPAREN)
     | JOIN ON nonEmptyNameList
     | SCAN variable labelOrRelType
     | EXPAND expandHintStep (COMMA expandHintStep)*
   )
   ;

expandHintStep
   : (ALL | INTO)? FROM from=variable TO to=variable (VIA via=variable)?
   | (ALL | INTO)? VIA via=variable
   ;

mergeClause
   : MERGE pattern mergeAction*
   ;

mergeAction
   : ON (MATCH | CREATE) setClause
   ;

filterClause
   : FILTER WHERE? expression
   ;

unwindClause
   : UNWIND expression AS variable
   ;

forListClause
   : FOR variable IN expression
   ;

letClause
   : LET letItem (COMMA letItem)*
   ;

letItem
   : variable EQ expression
   ;

callClause
   : OPTIONAL? CALL procedureName (LPAREN (procedureArgument (COMMA procedureArgument)*)? RPAREN)? (YIELD (TIMES | procedureResultItem (COMMA procedureResultItem)* whereClause?))?
   ;

procedureName
   : namespace symbolicNameString
   ;

procedureArgument
   : expression
   ;

procedureResultItem
   : yieldItemName = variable (AS yieldItemAlias = variable)?
   ;

loadCSVClause
   : LOAD CSV (WITH HEADERS)? FROM expression AS variable (FIELDTERMINATOR stringLiteral)?
   ;

foreachClause
   : FOREACH LPAREN variable IN expression BAR clause+ RPAREN
   ;

subqueryClause
   : OPTIONAL? CALL subqueryScope? LCURLY queryWithLocalDefinitions RCURLY subqueryInTransactionsParameters?
   ;

subqueryScope
   : LPAREN (TIMES | variable (COMMA variable)*)? RPAREN
   ;

subqueryInTransactionsParameters
   : IN (expression? CONCURRENT)? TRANSACTIONS (subqueryInTransactionsBatchParameters | subqueryInTransactionsDisjointByParameters | subqueryInTransactionsErrorParameters | subqueryInTransactionsReportParameters)*
   ;

subqueryInTransactionsBatchParameters
   : OF expression (ROW | ROWS)
   ;

subqueryInTransactionsDisjointByParameters
   : DISJOINT BY AUTO
   | DISJOINT BY NONE
   | DISJOINT BY LPAREN subqueryInTransactionsDisjointByExpressions RPAREN
   ;

subqueryInTransactionsDisjointByExpressions
   : expression (COMMA expression)*
   ;

subqueryInTransactionsErrorParameters
   : ON ERROR RETRY (subqueryInTransactionsRetryParameters)? (THEN (CONTINUE | BREAK | FAIL))?
   | ON ERROR (CONTINUE | BREAK | FAIL)
   ;

subqueryInTransactionsRetryParameters
   : FOR? expression secondsToken
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
   : (variable EQ)? pathPatternPrefix? anonymousPattern
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

pathPatternPrefix
   : pathMode pathToken?                                                       # AllPath
   | ANY SHORTEST pathMode? pathToken?                                         # AnyShortestPath
   | ALL SHORTEST pathMode? pathToken?                                         # AllShortestPath
   | ANY nonNegativeIntegerSpecification? pathMode? pathToken?                 # AnyPath
   | ALL pathMode? pathToken?                                                  # AllPath
   | SHORTEST nonNegativeIntegerSpecification? pathMode? pathToken? groupToken # ShortestGroup
   | SHORTEST nonNegativeIntegerSpecification pathMode? pathToken?             # AnyShortestPath
   ;

nonNegativeIntegerSpecification
   : UNSIGNED_DECIMAL_INTEGER | parameter["INTEGER"]
   ;

pathMode
    : WALK | TRAIL | ACYCLIC
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
   : LPAREN WHERE expression RPAREN //prioritize the WHERE keyword
   | LPAREN variable? labelExpression? properties? (WHERE expression)? RPAREN
   ;

insertNodePattern
   : LPAREN WHERE expression RPAREN //prioritize the WHERE keyword
   | LPAREN variable? insertNodeLabelExpression? map? RPAREN
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

dynamicAnyAllExpression
   : DOLLAR (ALL | ANY)? LPAREN expression RPAREN
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
   : leftArrow? arrowLine
     ( LBRACKET WHERE expression RBRACKET //prioritize the WHERE keyword
     | LBRACKET variable? labelExpression? pathLength? properties? (WHERE expression)? RBRACKET
     )? arrowLine rightArrow?
   ;

insertRelationshipPattern
   : leftArrow? arrowLine
     ( LBRACKET WHERE expression RBRACKET //prioritize the WHERE keyword
     | LBRACKET variable? insertRelationshipLabelExpression map? RBRACKET
     ) arrowLine rightArrow?
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
   : (COLON | IS) labelExpression4
   ;

labelExpression4
   : labelExpression3 (BAR COLON? labelExpression3)*
   ;

labelExpression3
   : labelExpression2 ((AMPERSAND | COLON) labelExpression2)*
   ;

labelExpression2
   : EXCLAMATION_MARK* labelExpression1
   ;

labelExpression1
   : LPAREN labelExpression4 RPAREN #ParenthesizedLabelExpression
   | PERCENT                        #AnyLabel
   | dynamicAnyAllExpression        #DynamicLabel
   | symbolicNameString             #LabelName
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
   | (IS NOT? LABELED? | COLON) labelExpression4      # LabelComparison
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
   | LBRACKET expression RBRACKET                                       # IndexPostfix
   | LBRACKET fromExp = expression? DOTDOT toExp = expression? RBRACKET # RangePostfix
   ;

property
   : DOT propertyKeyName
   ;

dynamicProperty
   : LBRACKET expression RBRACKET
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
   | allReduceExpression
   | listItemsPredicate
   | normalizeFunction
   | vectorFunction
   | vectorDistanceFunction
   | vectorNormFunction
   | trimFunction
   | propertyExistsPredicate
   | patternExpression
   | shortestPathExpression
   | parenthesizedExpression
   | functionInvocation
   | variable
   | obfuscatedLiteral
   ;

literal
   : numberLiteral # NumericLiteral
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
   : (
     EQ
     | INVALID_NEQ
     | NEQ
     | LE
     | GE
     | LT
     | GT ) expression7    # WhenSimpleComparison
   | comparisonExpression6 # WhenAdvancedComparison
   | expression            # WhenEquals
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

allReduceExpression
   :  allReduceExpressionValidArguments | allReduceExpressionInvalidArguments
   ;

allReduceExpressionValidArguments
   : ALLREDUCE LPAREN variable EQ expression COMMA variable IN expression BAR expression COMMA expression RPAREN
   ;

// Captures the case where the arguments are not valid, e.g. missing the variable or the bar expression.
// This will be mapped to a FunctionInvocation which will throw a useful error during Semantic Analysis.
allReduceExpressionInvalidArguments
   : ALLREDUCE LPAREN expression ((COMMA | BAR) expression)* RPAREN
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


vectorFunction
   : VECTOR LPAREN vectorValue = expression COMMA dimension = expression COMMA vectorCoordinateType RPAREN
   ;

vectorDistanceFunction
   : VECTOR_DISTANCE LPAREN vector1 = expression COMMA vector2 = expression COMMA vectorDistanceMetric RPAREN
   ;

vectorNormFunction
   : VECTOR_NORM LPAREN vectorValue = expression COMMA vectorNormDistanceMetric RPAREN
   ;

vectorDistanceMetric
   : EUCLIDEAN
   | EUCLIDEAN_SQUARED
   | MANHATTAN
   | COSINE
   | DOT_METRIC
   | HAMMING
   ;

vectorNormDistanceMetric
   : EUCLIDEAN
   | MANHATTAN
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
   : EXISTS LCURLY (queryWithLocalDefinitions | matchMode? patternList whereClause?) RCURLY
   ;

propertyExistsPredicate
   : PROPERTY_EXISTS LPAREN variable COMMA propertyKeyName RPAREN
   ;

countExpression
   : COUNT LCURLY (queryWithLocalDefinitions | matchMode? patternList whereClause?) RCURLY
   ;

collectExpression
   : COLLECT LCURLY queryWithLocalDefinitions RCURLY
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
   : (symbolicNameString | UNSIGNED_DECIMAL_INTEGER | UNSIGNED_OCTAL_INTEGER | EXTENDED_IDENTIFIER)
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
   : symbolicVariableNameString
   ;

obfuscatedLiteral
   : OBFUSCATION
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
   | UUID
   | INT
   | SIGNED? INTEGER
   | INTEGER64
   | INT64
   | FLOAT
   | FLOAT64
   | DATE
   | LOCAL (TIME | DATETIME)
   | ZONED (TIME | DATETIME)
   | TIME (WITHOUT | WITH) (TIMEZONE | TIME ZONE)
   | TIMESTAMP (WITHOUT | WITH) (TIMEZONE | TIME ZONE)
   | DURATION
   | POINT
   | NODE
   | VECTOR LPAREN signedIntegerLiteral COMMA vectorCoordinateType RPAREN
   | VECTOR (LT vectorCoordinateType GT)? (LPAREN signedIntegerLiteral RPAREN)?
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

vectorCoordinateType
    : (INT
    | SIGNED? INTEGER
    | INTEGER64
    | INTEGER32
    | INTEGER16
    | INTEGER8
    | INT64
    | INT32
    | INT16
    | INT8
    | FLOAT
    | FLOAT64
    | FLOAT32
    ) typeNullability?
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
      | showAdminCommand
   )
   ;

createCommand
   : CREATE (OR REPLACE)? (
      createAlias
      | createCompositeDatabase
      | createConstraint
      | createDatabase
      | createReplicaDatabase
      | createIndex
      | createRole
      | createUser
      | createAuthRule
   )
   ;

alterCommand
   : ALTER (
      alterAlias
      | alterCurrentUser
      | alterCurrentGraphType
      | alterDatabase
      | alterUser
      | alterUsers
      | alterServer
      | alterAuthRule
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
      | dropAuthRule
   )
   ;

showAdminCommand
   : SHOW (
      showAliases
      | showCurrentUser
      | showPrivileges
      | showRolePrivileges
      | showRoles
      | showServers
      | showSupportedPrivileges
      | showUserPrivileges
      | showUsers
      | showAuthRules
   )
   ;

showCommandYield
   : yieldClause returnClause?
   | whereClause
   ;

showCommandYieldWhere
   : yieldClause
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
      | showCurrentGraphTypeCommand
      | showFunctions
      | showProcedures
      | showSettings
      | showTransactions
      | showDatabase
   )
   ;

showIndexCommand
   : (showIndexType)? showIndexesEnd
   ;

showIndexType
    : ALL
    | FULLTEXT
    | LOOKUP
    | POINT
    | RANGE
    | TEXT
    | VECTOR
    ;

showIndexesEnd
   : indexToken showCommandYieldWhere?
   ;

showConstraintCommand
   : ALL? showConstraintsEnd                                                        # ShowConstraintAll
   | (showConstraintEntity)? constraintExistType showConstraintsEnd                 # ShowConstraintExist
   | (showConstraintEntity)? KEY showConstraintsEnd                                 # ShowConstraintKey
   | (showConstraintEntity)? PROPERTY TYPE showConstraintsEnd                       # ShowConstraintPropType
   | (showConstraintEntity)? (PROPERTY)? (UNIQUE | UNIQUENESS) showConstraintsEnd   # ShowConstraintUnique
   ;

showConstraintEntity
    : NODE                  # nodeEntity
    | (RELATIONSHIP | REL)  # relEntity
    ;

constraintExistType
   : EXISTENCE
   | EXIST
   | PROPERTY EXISTENCE
   | PROPERTY EXIST
   ;

showConstraintsEnd
   : constraintToken showCommandYieldWhere?
   ;

showCurrentGraphTypeCommand
   : CURRENT GRAPH TYPE (AS GRAPH)? showCommandYieldWhere?
   ;

showProcedures
   : (PROCEDURE | PROCEDURES) executableBy? showCommandYieldWhere?
   ;

showFunctions
   : showFunctionsType? functionToken executableBy? showCommandYieldWhere?
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
   : transactionToken stringsOrExpression showCommandYieldWhere?
   ;

showSettings
   : settingToken namesAndClauses
   ;

settingToken
   : SETTING | SETTINGS
   ;

// Keeping both sides here as optional instead of having one mandatory and then optionally call `namesAndClauses`,
// to not prioritize stringsOrExpression over regular clauses (for example, we don't want `SHOW SETTINGS WITH * MATCH (*)`
// to be parsed as variable `WITH` multiplied with the function call `MATCH (*)`)
namesAndClauses
   : showCommandYieldWhere?
   | stringsOrExpression showCommandYieldWhere?
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
   : CONSTRAINT commandNameExpression? (IF NOT EXISTS)? FOR (commandNodePattern | commandRelPattern) constraintType commandOptions?
   ;

constraintType
   : REQUIRE propertyList (COLONCOLON | IS (TYPED | COLONCOLON)) type # ConstraintTyped
   | REQUIRE propertyList IS (NODE | RELATIONSHIP | REL)? UNIQUE      # ConstraintIsUnique
   | REQUIRE propertyList IS (NODE | RELATIONSHIP | REL)? KEY         # ConstraintKey
   | REQUIRE propertyList IS NOT NULL                                 # ConstraintIsNotNull
   ;

dropConstraint
   : CONSTRAINT commandNameExpression (IF EXISTS)?
   ;

createIndex
   : RANGE INDEX createIndex_
   | TEXT INDEX createIndex_
   | POINT INDEX createIndex_
   | VECTOR INDEX createVectorIndex
   | LOOKUP INDEX createLookupIndex
   | FULLTEXT INDEX createFulltextIndex
   | INDEX createIndex_
   ;

createIndex_
   : commandNameExpression? (IF NOT EXISTS)? FOR (commandNodePattern | commandRelPattern) ON propertyList commandOptions?
   ;

createFulltextIndex
   : commandNameExpression? (IF NOT EXISTS)? FOR (multiLabelNodePattern | multiRelTypeRelPattern) ON EACH LBRACKET enclosedPropertyList RBRACKET commandOptions?
   ;

createVectorIndex
   : commandNameExpression? (IF NOT EXISTS)? FOR (multiLabelNodePattern | multiRelTypeRelPattern) ON propertyList withProperties? commandOptions?
   ;

multiLabelNodePattern
   : LPAREN variable COLON symbolicNameString (BAR symbolicNameString)* RPAREN
   ;

multiRelTypeRelPattern
   : LPAREN RPAREN leftArrow? arrowLine LBRACKET variable COLON symbolicNameString (BAR symbolicNameString)* RBRACKET arrowLine rightArrow? LPAREN RPAREN
   ;

createLookupIndex
   : commandNameExpression? (IF NOT EXISTS)? FOR (lookupIndexNodePattern | lookupIndexRelPattern) symbolicNameString LPAREN variable RPAREN commandOptions?
   ;

lookupIndexNodePattern
   : LPAREN variable RPAREN ON EACH
   ;

lookupIndexRelPattern
   : LPAREN RPAREN leftArrow? arrowLine LBRACKET variable RBRACKET arrowLine rightArrow? LPAREN RPAREN ON EACH?
   ;

dropIndex
   : INDEX commandNameExpression (IF EXISTS)?
   ;

propertyList
   : variable property | LPAREN enclosedPropertyList RPAREN
   ;

enclosedPropertyList
   : variable property (COMMA variable property)*
   ;

withProperties
   : WITH LBRACKET enclosedPropertyList RBRACKET
   ;

// Graph Type Specification

alterCurrentGraphType
   : CURRENT GRAPH TYPE ( (SET | ADD | ALTER) graphTypeSpecification | DROP graphTypeDropSpecification )
   ;

graphTypeSpecification
   : LCURLY graphTypeSpecificationBody? RCURLY
   ;

graphTypeDropSpecification
   : LCURLY graphTypeDropSpecificationBody? RCURLY
   ;

graphTypeSpecificationBody
   : graphTypeElement (COMMA graphTypeElement)*
   ;

graphTypeDropSpecificationBody
   : graphTypeDropElement (COMMA graphTypeDropElement)*
   ;

graphTypeElement
  : edgeTypeSpecification
  | nodeTypeSpecification
  | constraintSpecification
  ;

graphTypeDropElement
  : edgeTypeSpecification
  | nodeTypeSpecification
  | CONSTRAINT symbolicNameString
  ;

nodeTypeInlineConstraintList
   : (constraintType commandOptions?)+
   ;

edgeTypeInlineConstraintList
   : (constraintType commandOptions?)+
   ;

implies
  : EQ rightArrow
  | IMPLIES
  ;

// Node type specification

nodeTypeSpecification
  : LPAREN variable? identifyingLabel impliedLabelSet? propertyTypeList? RPAREN nodeTypeInlineConstraintList?
  ;

impliedLabelSet
  : labelType ( AMPERSAND symbolicNameString )*
  ;

identifyingLabel
  : labelType implies
  ;

// Node type reference

nodeTypeReference
  : nodeTypeAliasReference
  | nodeTypeInSituReference
  ;

nodeTypeAliasReference
  : LPAREN variable RPAREN
  ;

nodeTypeInSituReference
  : LPAREN ( variable? labelType implies? )? RPAREN
  ;

// Edge type specifcation

edgeTypeSpecification
  : nodeTypeReference arcTypePointingRight nodeTypeReference edgeTypeInlineConstraintList?
  ;

arcTypePointingRight
  : arrowLine LBRACKET variable? identifyingRelationship propertyTypeList? RBRACKET arrowLine rightArrow
  ;

identifyingRelationship
  : relType implies
  ;

// Edge type reference

edgeTypeReference
  : edgeTypeAliasReference
  | edgeTypeInSituReference
  ;

edgeTypeAliasReference
  : LPAREN RPAREN arrowLine LBRACKET variable RBRACKET arrowLine rightArrow LPAREN RPAREN
  ;

edgeTypeInSituReference
  : LPAREN RPAREN arrowLine LBRACKET variable? relType implies? RBRACKET arrowLine rightArrow LPAREN RPAREN
  ;

// Property Types

propertyTypeList
  : LCURLY (propertyType ( COMMA propertyType )* )? RCURLY
  ;

propertyType
  : propertyKeyName typed? type propertyTypeInlineConstraint?
  ;

propertyTypeInlineConstraint
  : IS  ( NODE | RELATIONSHIP | REL )? (KEY | UNIQUE)
  ;

typed
  : COLONCOLON
  | TYPED
  ;

// Graph Type constraint specification

constraintSpecification
  : CONSTRAINT symbolicNameString? FOR (nodeTypeReference | edgeTypeReference) constraintType commandOptions?
  ;

// Admin commands

renameCommand
   : RENAME (renameRole | renameServer | renameUser | renameAuthRule)
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

authRuleNames
   : symbolicNameOrStringParameterList
   ;

roleToken
   : ROLES
   | ROLE
   ;

tagToken
   : TAG
   | TAGS
   ;

authRuleKeywords
    : AUTH (RULE | RULES)
    ;

commandToken
    : COMMAND
    | COMMANDS
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
   : IMMUTABLE? ROLE commandNameExpression (IF NOT EXISTS)? (AS COPY OF commandNameExpression)?
   ;

dropRole
   : ROLE commandNameExpression (IF EXISTS)?
   ;

renameRole
   : ROLE commandNameExpression (IF EXISTS)? TO commandNameExpression
   ;

showRoles
   : (ALL | POPULATED)? roleToken (WITH (USER | USERS | authRuleKeywords))? (AS commandToken)? showCommandYield?
   ;

grantRole
   : roleNames TO usersOrAuthRule
   ;

revokeRole
   : roleNames FROM usersOrAuthRule
   ;

usersOrAuthRule
    : authRuleKeywords authRuleNames
    | (USER | USERS)? userNames
    ;

// User commands

createUser
   : USER commandNameExpression (IF NOT EXISTS)? (SET (
      password
      | PASSWORD passwordChangeRequired
      | userStatus
      | homeDatabase
      | setAuthClause
      | userSetTagsClause
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
      | userRemoveTagsClause
   ))* (ADD (
      userAddTagsClause
   ))* (SET (
      password
      | PASSWORD passwordChangeRequired
      | userStatus
      | homeDatabase
      | setAuthClause
      | userSetTagsClause
   ))*
   ;

alterUsers
   : USERS commandNameExpression (COMMA commandNameExpression)* (IF EXISTS)?
     (REMOVE userRemoveTagsClause)*
     (ADD userAddTagsClause)*
     (SET userSetTagsClause)*
   ;

removeNamedProvider
   : AUTH (PROVIDER | PROVIDERS)? (stringLiteral | stringListLiteral | parameter["ANY"])
   ;

userSetTagsClause
   : tagToken (stringLiteral | stringListLiteral | parameter["ANY"])
   ;

userAddTagsClause
   : tagToken (stringLiteral | stringListLiteral | parameter["ANY"])
   ;

userRemoveTagsClause
   : ALL tagToken
   | tagToken (stringLiteral | stringListLiteral | parameter["ANY"])
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
   : AS REVOKE? commandToken
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
   : HOME (DATABASE | GRAPH)                                # DefaultTarget
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
   | AUTH RULE
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
      | (ALIAS | AUTH RULE | PRIVILEGE | ROLE | SERVER | SERVERS | settingToken settingQualifier | USER METADATA? | SECRETS) ON DBMS
   )
   ;

setPrivilege
   : SET (
      (passwordToken | USER (STATUS | HOME DATABASE | METADATA) | DATABASE (ACCESS | DEFAULT LANGUAGE) | AUTH) ON DBMS
      | DATABASE (ACCESS | DEFAULT LANGUAGE) ON databaseScope
      | LABEL labelsResource ON graphScope
      | PROPERTY propertiesResource ON graphScope graphQualifier
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
      | ALTER COMPOSITE? DATABASE
      | START
      | STOP
      | (indexToken | constraintToken | NAME) MANAGEMENT?
      | (TRANSACTION MANAGEMENT? | TERMINATE transactionToken) userQualifier?
   )
   ON databaseScope
   ;

dbmsPrivilege
   : (
      ALTER (ALIAS | AUTH RULE | COMPOSITE? DATABASE | USER)
      | ASSIGN (PRIVILEGE | ROLE)
      | (ALIAS | COMPOSITE? DATABASE | PRIVILEGE | ROLE | SERVER | USER METADATA? | AUTH RULE | SECRETS) MANAGEMENT
      | dbmsPrivilegeExecute
      | RENAME (AUTH RULE | ROLE | USER)
      | WRITE SECRETS
      | READ secretToken secretQualifier
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

secretToken
  : SECRET
  | SECRETS
  ;

secretQualifier
  : TIMES
  | stringOrParameterExpression
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

qualifiedGraphPrivileges
   : (TRAVERSE | DELETE | (READ | MATCH | MERGE) propertiesResource) ON graphScope graphQualifier
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
      | FOR (
        LPAREN variable? (COLON symbolicNameString (BAR symbolicNameString)*)? (RPAREN WHERE expression | (WHERE expression | map) RPAREN)
        | LPAREN RPAREN leftArrow? arrowLine LBRACKET variable? (COLON symbolicNameString (BAR symbolicNameString)*)?
            (RBRACKET arrowLine rightArrow? LPAREN RPAREN WHERE expression | (WHERE expression | map) RBRACKET arrowLine rightArrow? LPAREN RPAREN)
      )
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
   : HOME DATABASE
   | (DATABASE | DATABASES) (TIMES | symbolicAliasNameList)
   ;

graphScope
   : HOME GRAPH
   | (GRAPH | GRAPHS) (TIMES | symbolicAliasNameList)
   ;

// Attribute based role assignment

createAuthRule
    : AUTH RULE commandNameExpression (IF NOT EXISTS)? (authRuleSetClause)+
    ;

authRuleSetClause
    : SET (authRuleSetCondition | authRuleSetEnabled)
    ;

authRuleSetCondition
    : CONDITION expression
    ;

authRuleSetEnabled
    : ENABLED (TRUE | FALSE)
    ;

renameAuthRule
    : AUTH RULE commandNameExpression (IF EXISTS)? TO commandNameExpression
    ;

alterAuthRule
    : AUTH RULE commandNameExpression (IF EXISTS)? (authRuleSetClause)+
    ;

dropAuthRule
    : AUTH RULE commandNameExpression (IF EXISTS)?
    ;

showAuthRules
    : authRuleKeywords (AS commandToken)? showCommandYield?
    ;

// Database commands

createCompositeDatabase
   : COMPOSITE DATABASE symbolicAliasNameOrParameter (IF NOT EXISTS)? (SET? defaultLanguageSpecification)? commandOptions? waitClause?
   ;

createDatabase
   : DATABASE symbolicAliasNameOrParameter (IF NOT EXISTS)? (SET? defaultLanguageSpecification)? (topology | shards)? commandOptions? waitClause?
   ;

createReplicaDatabase
   : REPLICA DATABASE symbolicAliasNameOrParameter (IF NOT EXISTS)? (SET? defaultLanguageSpecification)? topology? commandOptions? waitClause?
   ;

shards
   : (SET? graphShard)? SET? propertyShard
   ;

graphShard
   : GRAPH SHARD LCURLY (topology)? RCURLY
   ;

propertyShard
   : PROPERTY (SHARD | SHARDS) LCURLY COUNT UNSIGNED_DECIMAL_INTEGER (SET? TOPOLOGY uIntOrIntParameter replicaToken)? RCURLY
   ;

topology
   : SET? TOPOLOGY (primaryTopology | secondaryTopology)+
   ;

primaryTopology
   : uIntOrIntParameter primaryToken
   ;

primaryToken
   : PRIMARY | PRIMARIES
   ;

secondaryTopology
   : uIntOrIntParameter secondaryToken
   ;

secondaryToken
   : SECONDARY | SECONDARIES
   ;

replicaToken
   : REPLICA | REPLICAS
   ;

defaultLanguageSpecification
    : DEFAULT LANGUAGE CYPHER UNSIGNED_DECIMAL_INTEGER
    ;

dropDatabase
   : COMPOSITE? DATABASE symbolicAliasNameOrParameter (IF EXISTS)? aliasAction? ((DUMP | DESTROY) DATA)? waitClause?
   ;

aliasAction
   : RESTRICT
   | CASCADE (ALIAS | ALIASES)
   ;

alterDatabase
   : DATABASE symbolicAliasNameOrParameter (IF EXISTS)? (
      (SET (alterDatabaseAccess | alterDatabaseTopology | alterReplicaTopology | alterGraphShard | alterPropertyShards | alterDatabaseOption | defaultLanguageSpecification))+
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

alterGraphShard
   : GRAPH SHARD LCURLY SET alterDatabaseTopology RCURLY
   ;

alterPropertyShards
   : PROPERTY (SHARD | SHARDS) LCURLY SET alterReplicaTopology RCURLY
   ;

alterReplicaTopology
   : TOPOLOGY uIntOrIntParameter replicaToken
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
   : (DEFAULT | HOME) DATABASE showCommandYieldWhere?
   | (DATABASE | DATABASES) symbolicAliasNameOrParameter? showCommandYieldWhere?
   ;

aliasName
   : symbolicAliasNameOrParameter
   ;

aliasTargetName
   : symbolicAliasNameOrParameter
   ;

// Alias commands

createAlias
   : ALIAS aliasName (IF NOT EXISTS)? FOR DATABASE aliasTargetName (AT stringOrParameter remoteTargetConnectionCredentials (DRIVER mapOrParameter)? defaultLanguageSpecification?)? (PROPERTIES mapOrParameter)?
   ;

remoteTargetConnectionCredentials
    : USER commandNameExpression PASSWORD passwordExpression
    | OIDC CREDENTIAL FORWARDING
    ;

dropAlias
   : ALIAS aliasName (IF EXISTS)? FOR DATABASE
   ;

alterAlias
   : ALIAS aliasName (IF EXISTS)? SET DATABASE (
      alterAliasTarget
      | alterAliasUser
      | alterAliasPassword
      | alterAliasDriver
      | alterAliasProperties
      | defaultLanguageSpecification
   )+
   ;

alterAliasTarget
   : TARGET aliasTargetName (AT stringOrParameter)?
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
   : (ALIAS | ALIASES) aliasName? FOR (DATABASE | DATABASES) showCommandYield?
   ;

// Various strings, symbolic names, lists and maps

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

// Should return an Either[Integer, Parameter]
// There is no unsigned integer Cypher Type so the parameter permits signed values.
uIntOrIntParameter
    :UNSIGNED_DECIMAL_INTEGER
    | parameter["INTEGER"]
    ;

mapOrParameter
   : map
   | parameter["MAP"]
   ;

map
   : LCURLY (propertyKeyName COLON expression (COMMA propertyKeyName COLON expression)*)? RCURLY
   ;

symbolicVariableNameString
   : escapedSymbolicVariableNameString
   | unescapedSymbolicVariableNameString
   ;

escapedSymbolicVariableNameString
   : escapedSymbolicNameString
   ;

unescapedSymbolicVariableNameString
   : unescapedSymbolicNameString
   ;

symbolicNameString
   : escapedSymbolicNameString
   | unescapedSymbolicNameString
   ;

escapedSymbolicNameString
   : ESCAPED_SYMBOLIC_NAME
   ;

// Do not remove this, it is needed for composing the grammar
// with other ones (e.g. language support ones)
unescapedSymbolicNameString
   : unescapedSymbolicNameString_
   ;

unescapedSymbolicNameString_
   : IDENTIFIER
   | ACCESS
   | ACTIVE
   | ACYCLIC
   | ADD
   | ADMIN
   | ADMINISTRATOR
   | ALIAS
   | ALIASES
   | ALL_SHORTEST_PATHS
   | ALL
   | ALLREDUCE
   | ALTER
   | ANALYZER
   | AND
   | ANY
   | ARRAY
   | AS
   | ASC
   | ASCENDING
   | ASSIGN
   | AT
   | AUTH
   | AUTO
   | BINDINGS
   | BOOL
   | BOOLEAN
   | BOOSTED
   | BOTH
   | BREAK
   | BUILT
   | BY
   | CALL
   | CASCADE
   | CASE
   | CHANGE
   | CIDR
   | COLLECT
   | COMMAND
   | COMMANDS
   | COMPOSITE
   | CONCURRENT
   | CONSTRAINT
   | CONSTRAINTS
   | CONTAINS
   | CONTINUE
   | COPY
   | CONDITION
   | COSINE
   | COUNT
   | CREATE
   | CREDENTIAL
   | CSV
   | CURRENT
   | CYPHER
   | DATA
   | DATABASE
   | DATABASES
   | DATE
   | DATETIME
   | DBMS
   | DEALLOCATE
   | DEFAULT
   | DEFINE
   | DEFINED
   | DELETE
   | DENY
   | DESC
   | DESCENDING
   | DESTROY
   | DETACH
   | DIFFERENT
   | DISJOINT
   | DISTINCT
   | DRIVER
   | DOT_METRIC
   | DROP
   | DRYRUN
   | DUMP
   | DURATION
   | EACH
   | EDGE
   | ELEMENT
   | ELEMENTS
   | ELSE
   | ENABLED
   | ENABLE
   | ENCRYPTED
   | END
   | ENDS
   | ERROR
   | EUCLIDEAN
   | EUCLIDEAN_SQUARED
   | EXECUTABLE
   | EXECUTE
   | EXIST
   | EXISTENCE
   | EXISTS
   | EXPAND
   | FAIL
   | FALSE
   | FIELDTERMINATOR
   | FILTER
   | FINISH
   | FLOAT   
   | FLOAT64
   | FLOAT32
   | FOREACH
   | FOR
   | FORWARDING
   | FROM
   | FULLTEXT
   | FUNCTION
   | FUNCTIONS
   | GRANT
   | GRAPH
   | GRAPHS
   | GROUP
   | GROUPS
   | HAMMING
   | HEADERS
   | HOME
   | ID
   | IF
   | IMMUTABLE
   | IMPERSONATE
   | IMPLIES
   | IN
   | INDEX
   | INDEXES
   | INF
   | INFINITY
   | INSERT
   | INT
   | INT64
   | INT32
   | INT16
   | INT8
   | INTEGER
   | INTEGER64
   | INTEGER32
   | INTEGER16
   | INTEGER8
   | INTO
   | IS
   | JOIN
   | KEY
   | LABEL
   | LABELED
   | LABELS
   | LANGUAGE
   | LEADING
   | LET
   | LIMITROWS
   | LIST
   | LOAD
   | LOCAL
   | LOOKUP
   | MATCH
   | MANAGEMENT
   | MANHATTAN
   | MAP
   | MERGE
   | METADATA
   | NAME
   | NAMES
   | NAN
   | NEW
   | NEXT
   | NFC
   | NFD
   | NFKC
   | NFKD
   | NODE
   | NODETACH
   | NODES
   | NONE
   | NORMALIZE
   | NORMALIZED
   | NOT
   | NOTHING
   | NOWAIT
   | NULL
   | OF
   | OFFSET
   | OIDC
   | ON
   | ONLY
   | OPTIONAL
   | OPTIONS
   | OPTION
   | OR
   | ORDER
   | PASSWORD
   | PASSWORDS
   | PATH
   | PATHS
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
   | PROPERTY_EXISTS
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
   | REPLICA
   | REPLICAS
   | REPORT
   | REQUIRE
   | REQUIRED
   | RESTRICT
   | RETRY
   | RETURN
   | REVOKE
   | ROLE
   | ROLES
   | ROW
   | ROWS
   | RULE
   | RULES
   | SCAN
   | SCORE
   | SEARCH
   | SECONDARY
   | SECONDARIES
   | SEC
   | SECOND
   | SECONDS
   | SECRET
   | SECRETS
   | SEEK
   | SERVER
   | SERVERS
   | SET
   | SETTING
   | SETTINGS
   | SHARD
   | SHARDS
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
   | TAG
   | TAGS
   | TARGET
   | TERMINATE
   | TEXT
   | THEN
   | TIME
   | TIMESTAMP
   | TIMEZONE
   | TO
   | TOPOLOGY
   | TRAIL
   | TRAILING
   | TRANSACTION
   | TRANSACTIONS
   | TRAVERSE
   | TRIM
   | TRUE
   | TYPE
   | TYPED
   | TYPES
   | UUID
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
   | VIA
   | VECTOR
   | VECTOR_DISTANCE
   | VECTOR_NORM
   | VERTEX
   | WAIT
   | WALK
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