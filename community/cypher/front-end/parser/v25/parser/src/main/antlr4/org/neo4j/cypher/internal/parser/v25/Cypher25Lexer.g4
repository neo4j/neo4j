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
lexer grammar Cypher25Lexer;

SPACE
   : ( '\u0009'
      | '\n' //can't parse this in unicode
      | '\u000B'
      | '\u000C'
      | '\r' //can't parse this in unicode
      | '\u001C'
      | '\u001D'
      | '\u001E'
      | '\u001F'
      | '\u0020'
      | '\u0085'
      | '\u00A0'
      | '\u1680'
      | '\u2000'
      | '\u2001'
      | '\u2002'
      | '\u2003'
      | '\u2004'
      | '\u2005'
      | '\u2006'
      | '\u2007'
      | '\u2008'
      | '\u2009'
      | '\u200A'
      | '\u2028'
      | '\u2029'
      | '\u202F'
      | '\u205F'
      | '\u3000'
   ) -> channel (HIDDEN)
   ;

SINGLE_LINE_COMMENT
   : '//' ~[\r\n]* -> channel (HIDDEN)
   ;

MULTI_LINE_COMMENT
   : '/*' .*? '*/' -> channel (HIDDEN)
   ;

DECIMAL_DOUBLE
   : ([0-9] (INTEGER_PART)* '.' [0-9] (INTEGER_PART)* (DECIMAL_EXPONENT)? (IDENTIFIER)? | '.' [0-9] (INTEGER_PART)* (DECIMAL_EXPONENT)? (IDENTIFIER)? | [0-9] (INTEGER_PART)* DECIMAL_EXPONENT (IDENTIFIER)?)
   ;

UNSIGNED_DECIMAL_INTEGER
   : ([1-9] (INTEGER_PART)* (PART_LETTER)* | '0')
   ;

fragment DECIMAL_EXPONENT
   : [eE] ([+\-])? (INTEGER_PART)+ (PART_LETTER)*
   ;

fragment INTEGER_PART
   : ('_')? [0-9]
   ;

UNSIGNED_HEX_INTEGER
   : '0x' (PART_LETTER)*
   ;

UNSIGNED_OCTAL_INTEGER
   : '0o' (PART_LETTER)*
   ;

STRING_LITERAL1
   : '\'' (~['\\] | EscapeSequence)* '\''
   ;

STRING_LITERAL2
   : '"' (~["\\] | EscapeSequence)* '"'
   ;

// In Cypher it is allowed to have any character following a backslash.
// In the cases it is an actual escape code it is handled in the AST builder.
fragment EscapeSequence
   : '\\' .
   ;

ESCAPED_SYMBOLIC_NAME
   : '`' ( ~'`' | '``' )* '`'
   ;

ACCESS
   : A C C E S S
   ;

ACTIVE
   : A C T I V E
   ;

ACYCLIC
   : A C Y C L I C
   ;

ADD
   : A D D
   ;

ADMIN
   : A D M I N
   ;

ADMINISTRATOR
   : A D M I N I S T R A T O R
   ;

ALIAS
   : A L I A S
   ;

ALIASES
   : A L I A S E S
   ;

ALL_SHORTEST_PATHS
   : A L L S H O R T E S T P A T H S
   ;

ALL
   : A L L
   ;

ALLREDUCE
  : A L L R E D U C E
  ;

ALTER
   : A L T E R
   ;

AND
   : A N D
   ;

ANY
   : A N Y
   ;

ARRAY
   : A R R A Y
   ;

AS
   : A S
   ;

ASC
   : A S C
   ;

ASCENDING
   : A S C E N D I N G
   ;

ASSIGN
   : A S S I G N
   ;

AT
   : A T
   ;

AUTH
   : A U T H
   ;

AUTO
   : A U T O
   ;

BAR
   : '|'
   ;

BINDINGS
   : B I N D I N G S
   ;

BOOL
   : B O O L
   ;

BOOLEAN
   : B O O L E A N
   ;

BOOSTED
   : B O O S T E D
   ;

BOTH
   : B O T H
   ;

BREAK
   : B R E A K
   ;

BUILT
   : B U I L T
   ;

BY
   : B Y
   ;

CALL
   : C A L L
   ;

CASCADE
   : C A S C A D E
   ;

CASE
   : C A S E
   ;

CHANGE
   : C H A N G E
   ;

CIDR
   : C I D R
   ;

COLLECT
   : C O L L E C T
   ;

COLON
   : ':'
   ;

COLONCOLON
   : '::'
   ;

COMMA
   : ','
   ;

COMMAND
   : C O M M A N D
   ;

COMMANDS
   : C O M M A N D S
   ;

COMPOSITE
   : C O M P O S I T E
   ;

CONCURRENT
   : C O N C U R R E N T
   ;

CONSTRAINT
   : C O N S T R A I N T
   ;

CONSTRAINTS
   : C O N S T R A I N T S
   ;

CONTAINS
   : C O N T A I N S
   ;

COPY
   : C O P Y
   ;

CONDITION
    : C O N D I T I O N
    ;

CONTINUE
   : C O N T I N U E
   ;

COSINE
   : C O S I N E
   ;

COUNT
   : C O U N T
   ;

CREATE
   : C R E A T E
   ;

CREDENTIAL
   : C R E D E N T I A L
   ;

CSV
   : C S V
   ;

CURRENT
   : C U R R E N T
   ;

CYPHER
   : C Y P H E R
   ;

DATA
   : D A T A
   ;

DATABASE
   : D A T A B A S E
   ;

DATABASES
   : D A T A B A S E S
   ;

DATE
   : D A T E
   ;

DATETIME
   : D A T E T I M E
   ;

DBMS
   : D B M S
   ;

DEALLOCATE
   : D E A L L O C A T E
   ;

DEFAULT
   : D E F A U L T
   ;

DEFINE
   : D E F I N E
   ;

DEFINED
   : D E F I N E D
   ;

DELETE
   : D E L E T E
   ;

DENY
   : D E N Y
   ;

DESC
   : D E S C
   ;

DESCENDING
   : D E S C E N D I N G
   ;

DESTROY
   : D E S T R O Y
   ;

DETACH
   : D E T A C H
   ;

DIFFERENT
   : D I F F E R E N T
   ;

DOLLAR
   : '$'
   ;

DISJOINT
   : D I S J O I N T
   ;

DISTINCT
   : D I S T I N C T
   ;

DIVIDE
   : '/'
   ;

DOT
   : '.'
   ;

DOT_METRIC
   : D O T
   ;

DOTDOT
   : '..'
   ;

DOUBLEBAR
   : '||'
   ;

DRIVER
   : D R I V E R
   ;

DROP
   : D R O P
   ;

DRYRUN
   : D R Y R U N
   ;

DUMP
   : D U M P
   ;

DURATION
   : D U R A T I O N
   ;

EACH
   : E A C H
   ;

EDGE
   : E D G E
   ;

ENABLE
   : E N A B L E
   ;

ELEMENT
   : E L E M E N T
   ;

ELEMENTS
   : E L E M E N T S
   ;

ELSE
   : E L S E
   ;

ENABLED
    : E N A B L E D
    ;

ENCRYPTED
   : E N C R Y P T E D
   ;
   
EUCLIDEAN
    : E U C L I D E A N
    ;
   
EUCLIDEAN_SQUARED
    : E U C L I D E A N UNDERSCORE S Q U A R E D
    ;

END
   : E N D
   ;

ENDS
   : E N D S
   ;

EQ
   : '='
   ;

EXECUTABLE
   : E X E C U T A B L E
   ;

EXECUTE
   : E X E C U T E
   ;

EXIST
   : E X I S T
   ;

EXISTENCE
   : E X I S T E N C E
   ;

EXISTS
   : E X I S T S
   ;

EXPAND
   : E X P A N D
   ;

ERROR
   : E R R O R
   ;

FAIL
   : F A I L
   ;

FALSE
   : F A L S E
   ;

FIELDTERMINATOR
   : F I E L D T E R M I N A T O R
   ;

FILTER
   : F I L T E R
   ;

FINISH
   : F I N I S H
   ;

FLOAT
   : F L O A T
   ;

FLOAT64
   : F L O A T SIX FOUR
   ;
FLOAT32
   : F L O A T THREE TWO
   ;

FOR
   : F O R
   ;

FOREACH
   : F O R E A C H
   ;

FORWARDING
   : F O R W A R D I N G
   ;

FROM
   : F R O M
   ;

FULLTEXT
   : F U L L T E X T
   ;

FUNCTION
   : F U N C T I O N
   ;

FUNCTIONS
   : F U N C T I O N S
   ;

GE
   : '>='
   ;

GRANT
   : G R A N T
   ;

GRAPH
   : G R A P H
   ;

GRAPHS
   : G R A P H S
   ;

GROUP
   : G R O U P
   ;

GROUPS
   : G R O U P S
   ;

GT
   : '>'
   ;

HAMMING
   : H A M M I N G
   ;

HEADERS
   : H E A D E R S
   ;

HOME
   : H O M E
   ;

ID
   : I D
   ;

IF
   : I F
   ;

IMPERSONATE
   : I M P E R S O N A T E
   ;

IMMUTABLE
   : I M M U T A B L E
   ;

IN
   : I N
   ;

INDEX
   : I N D E X
   ;

INDEXES
   : I N D E X E S
   ;

INF
   : I N F
   ;

INFINITY
   : I N F I N I T Y
   ;

INSERT
   : I N S E R T
   ;

INTO
   : I N T O
   ;

INT
   : I N T
   ;

INT64
   : I N T SIX FOUR
   ;
INT32
   : I N T THREE TWO
   ;
INT16
   : I N T ONE SIX
   ;
INT8
   : I N T EIGHT
   ;

INTEGER
   : I N T E G E R
   ;

INTEGER64
   : I N T E G E R SIX FOUR
   ;
INTEGER32
   : I N T E G E R THREE TWO
   ;
INTEGER16
   : I N T E G E R ONE SIX
   ;
INTEGER8
   : I N T E G E R EIGHT
   ;

IS
   : I S
   ;

JOIN
   : J O I N
   ;

KEY
   : K E Y
   ;

LABEL
   : L A B E L
   ;

LABELED
   : L A B E L E D
   ;

LABELS
   : L A B E L S
   ;

AMPERSAND
   : '&'
   ;

EXCLAMATION_MARK
   : '!'
   ;

LANGUAGE
   : L A N G U A G E
   ;

LBRACKET
   : '['
   ;

LCURLY
   : '{'
   ;

LE
   : '<='
   ;

LEADING
   : L E A D I N G
   ;

LET
   : L E T
   ;

LIMITROWS
   : L I M I T
   ;

LIST
   : L I S T
   ;

LOAD
   : L O A D
   ;

LOCAL
   : L O C A L
   ;

LOOKUP
   : L O O K U P
   ;

LPAREN
   : '('
   ;

LT
   : '<'
   ;

MANAGEMENT
   : M A N A G E M E N T
   ;

MANHATTAN
   : M A N H A T T A N
   ;

MAP
   : M A P
   ;

MATCH
   : M A T C H
   ;

MERGE
   : M E R G E
   ;

METADATA
   : M E T A D A T A
   ;

MINUS
   : '-'
   ;

PERCENT
   : '%'
   ;

IMPLIES
  : I M P L I E S
  ;

INVALID_NEQ
   : '!='
   ;

NEQ
   : '<>'
   ;

NAME
   : N A M E
   ;

NAMES
   : N A M E S
   ;

NAN
   : N A N
   ;

NFC
   : N F C
   ;

NFD
   : N F D
   ;

NFKC
   : N F K C
   ;

NFKD
   : N F K D
   ;

NEW
   : N E W
   ;

NEXT
   : N E X T
   ;

NODE
   : N O D E
   ;

NODETACH
   : N O D E T A C H
   ;

NODES
   : N O D E S
   ;

NONE
   : N O N E
   ;

NORMALIZE
   : N O R M A L I Z E
   ;

NORMALIZED
   : N O R M A L I Z E D
   ;

NOT
   : N O T
   ;

NOTHING
   : N O T H I N G
   ;

NOWAIT
   : N O W A I T
   ;

NULL
   : N U L L
   ;

OBFUSCATION
   : '******'
   ;

OF
   : O F
   ;

OFFSET
   : O F F S E T
   ;

OIDC
    : O I D C
    ;

ON
   : O N
   ;

ONLY
   : O N L Y
   ;

OPTIONAL
   : O P T I O N A L
   ;

OPTIONS
   : O P T I O N S
   ;

OPTION
   : O P T I O N
   ;

OR
   : O R
   ;

ORDER
   : O R D E R
   ;

PASSWORD
   : P A S S W O R D
   ;

PASSWORDS
   : P A S S W O R D S
   ;

PATH
   : P A T H
   ;

PATHS
   : P A T H S
   ;

PLAINTEXT
   : P L A I N T E X T
   ;

PLUS
   : '+'
   ;

PLUSEQUAL
   : '+='
   ;

POINT
   : P O I N T
   ;

POPULATED
   : P O P U L A T E D
   ;

POW
   : '^'
   ;

PRIMARY
   : P R I M A R Y
   ;

PRIMARIES
   : P R I M A R I E S
   ;

PRIVILEGE
   : P R I V I L E G E
   ;

PRIVILEGES
   : P R I V I L E G E S
   ;

PROCEDURE
   : P R O C E D U R E
   ;

PROCEDURES
   : P R O C E D U R E S
   ;

PROPERTIES
   : P R O P E R T I E S
   ;

PROPERTY_EXISTS
   : P R O P E R T Y '_' E X I S T S
   ;

PROPERTY
   : P R O P E R T Y
   ;

PROVIDER
   : P R O V I D E R
   ;

PROVIDERS
   : P R O V I D E R S
   ;

QUESTION
   : '?'
   ;

RANGE
   : R A N G E
   ;

RBRACKET
   : ']'
   ;

RCURLY
   : '}'
   ;

READ
   : R E A D
   ;

REALLOCATE
   : R E A L L O C A T E
   ;

REDUCE
   : R E D U C E
   ;

RENAME
   : R E N A M E
   ;

REGEQ
   : '=~'
   ;

REL
   : R E L
   ;

RELATIONSHIP
   : R E L A T I O N S H I P
   ;

RELATIONSHIPS
   : R E L A T I O N S H I P S
   ;

REMOVE
   : R E M O V E
   ;

REPEATABLE
   : R E P E A T A B L E
   ;

REPLACE
   : R E P L A C E
   ;

REPLICA
   : R E P L I C A
   ;

REPLICAS
   : R E P L I C A S
   ;

REPORT
   : R E P O R T
   ;

REQUIRE
   : R E Q U I R E
   ;

REQUIRED
   : R E Q U I R E D
   ;

RESTRICT
   : R E S T R I C T
   ;

RETRY
   : R E T R Y
   ;

RETURN
   : R E T U R N
   ;

REVOKE
   : R E V O K E
   ;

ROLE
   : R O L E
   ;

ROLES
   : R O L E S
   ;

ROW
   : R O W
   ;

ROWS
   : R O W S
   ;

RULE
    : R U L E
    ;

RULES
    : R U L E S
    ;

RPAREN
   : ')'
   ;

SCAN
   : S C A N
   ;

SCORE
   : S C O R E
   ;

SEARCH
   : S E A R C H
   ;

SEC
   : S E C
   ;

SECOND
   : S E C O N D
   ;

SECONDARY
   : S E C O N D A R Y
   ;

SECONDARIES
   : S E C O N D A R I E S
   ;

SECONDS
   : S E C O N D S
   ;

SECRET
   : S E C R E T
   ;

SECRETS
   : S E C R E T S
   ;

SEEK
   : S E E K
   ;

SEMICOLON
   : ';'
   ;

SERVER
   : S E R V E R
   ;

SERVERS
   : S E R V E R S
   ;

SET
   : S E T
   ;

SETTING
   : S E T T I N G
   ;

SETTINGS
   : S E T T I N G S
   ;

SHARD
   : S H A R D
   ;

SHARDS
   : S H A R D S
   ;

SHORTEST_PATH
   : S H O R T E S T P A T H
   ;

SHORTEST
   : S H O R T E S T
   ;

SHOW
   : S H O W
   ;

SIGNED
   : S I G N E D
   ;

SINGLE
   : S I N G L E
   ;

SKIPROWS
   : S K I P
   ;

START
   : S T A R T
   ;

STARTS
   : S T A R T S
   ;

STATUS
   : S T A T U S
   ;

STOP
   : S T O P
   ;

STRING
   : S T R I N G
   ;

SUPPORTED
   : S U P P O R T E D
   ;

SUSPENDED
   : S U S P E N D E D
   ;

TAG
   : T A G
   ;

TAGS
   : T A G S
   ;

TARGET
   : T A R G E T
   ;

TERMINATE
   : T E R M I N A T E
   ;

TEXT
   : T E X T
   ;

THEN
   : T H E N
   ;

TIME
   : T I M E
   ;

TIMES
   : '*'
   ;

TIMESTAMP
   : T I M E S T A M P
   ;

TIMEZONE
   : T I M E Z O N E
   ;

TO
   : T O
   ;

TOPOLOGY
   : T O P O L O G Y
   ;

TRAIL
    : T R A I L
    ;

TRAILING
   : T R A I L I N G
   ;

TRANSACTION
   : T R A N S A C T I O N
   ;

TRANSACTIONS
   : T R A N S A C T I O N S
   ;

TRAVERSE
   : T R A V E R S E
   ;

TRIM
   : T R I M
   ;

TRUE
   : T R U E
   ;

TYPE
   : T Y P E
   ;

TYPED
   : T Y P E D
   ;

TYPES
   : T Y P E S
   ;

UUID
   : U U I D
   ;

UNION
   : U N I O N
   ;

UNIQUE
   : U N I Q U E
   ;

UNIQUENESS
   : U N I Q U E N E S S
   ;

UNWIND
   : U N W I N D
   ;

URL
   : U R L
   ;

USE
   : U S E
   ;

USER
   : U S E R
   ;

USERS
   : U S E R S
   ;

USING
   : U S I N G
   ;

VALUE
   : V A L U E
   ;

VARCHAR
   : V A R C H A R
   ;

VIA
   : V I A
   ;

VECTOR
   : V E C T O R
   ;

VECTOR_DISTANCE
   : V E C T O R UNDERSCORE D I S T A N C E
   ;

VECTOR_NORM
   : V E C T O R UNDERSCORE N O R M
   ;

VERTEX
   : V E R T E X
   ;

WAIT
   : W A I T
   ;

WALK
    : W A L K
    ;

WHEN
   : W H E N
   ;

WHERE
   : W H E R E
   ;

WITH
   : W I T H
   ;

WITHOUT
   : W I T H O U T
   ;

WRITE
   : W R I T E
   ;

XOR
   : X O R
   ;

YIELD
   : Y I E L D
   ;

ZONE
   : Z O N E
   ;

ZONED
   : Z O N E D
   ;

// Making changes here? Don't forget to update org.neo4j.util.UnicodeHelper!
IDENTIFIER
   : LETTER (PART_LETTER)*
   ;

EXTENDED_IDENTIFIER
   : PART_LETTER+
   ;

ARROW_LINE
   : [\-\u00AD‐‑‒–—―﹘﹣－]
   ;

ARROW_LEFT_HEAD
   : [⟨〈﹤＜]
   ;

ARROW_RIGHT_HEAD
   : [⟩〉﹥＞]
   ;

// Making changes here? Don't forget to update org.neo4j.util.UnicodeHelper!
fragment LETTER
   : [\u0041-\u005a\u005f\u0061-\u007a\u00aa\u00b5\u00ba\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u02c1\u02c6-\u02d1\u02e0-\u02e4\u02ec\u02ee\u0370-\u0374\u0376-\u0377\u037a-\u037d\u037f\u0386\u0388-\u038a\u038c\u038e-\u03a1\u03a3-\u03f5\u03f7-\u0481\u048a-\u052f\u0531-\u0556\u0559\u0560-\u0588\u05d0-\u05ea\u05ef-\u05f2\u0620-\u064a\u066e-\u066f\u0671-\u06d3\u06d5\u06e5-\u06e6\u06ee-\u06ef\u06fa-\u06fc\u06ff\u0710\u0712-\u072f\u074d-\u07a5\u07b1\u07ca-\u07ea\u07f4-\u07f5\u07fa\u0800-\u0815\u081a\u0824\u0828\u0840-\u0858\u0860-\u086a\u08a0-\u08b4\u08b6-\u08c7\u0904-\u0939\u093d\u0950\u0958-\u0961\u0971-\u0980\u0985-\u098c\u098f-\u0990\u0993-\u09a8\u09aa-\u09b0\u09b2\u09b6-\u09b9\u09bd\u09ce\u09dc-\u09dd\u09df-\u09e1\u09f0-\u09f1\u09fc\u0a05-\u0a0a\u0a0f-\u0a10\u0a13-\u0a28\u0a2a-\u0a30\u0a32-\u0a33\u0a35-\u0a36\u0a38-\u0a39\u0a59-\u0a5c\u0a5e\u0a72-\u0a74\u0a85-\u0a8d\u0a8f-\u0a91\u0a93-\u0aa8\u0aaa-\u0ab0\u0ab2-\u0ab3\u0ab5-\u0ab9\u0abd\u0ad0\u0ae0-\u0ae1\u0af9\u0b05-\u0b0c\u0b0f-\u0b10\u0b13-\u0b28\u0b2a-\u0b30\u0b32-\u0b33\u0b35-\u0b39\u0b3d\u0b5c-\u0b5d\u0b5f-\u0b61\u0b71\u0b83\u0b85-\u0b8a\u0b8e-\u0b90\u0b92-\u0b95\u0b99-\u0b9a\u0b9c\u0b9e-\u0b9f\u0ba3-\u0ba4\u0ba8-\u0baa\u0bae-\u0bb9\u0bd0\u0c05-\u0c0c\u0c0e-\u0c10\u0c12-\u0c28\u0c2a-\u0c39\u0c3d\u0c58-\u0c5a\u0c60-\u0c61\u0c80\u0c85-\u0c8c\u0c8e-\u0c90\u0c92-\u0ca8\u0caa-\u0cb3\u0cb5-\u0cb9\u0cbd\u0cde\u0ce0-\u0ce1\u0cf1-\u0cf2\u0d04-\u0d0c\u0d0e-\u0d10\u0d12-\u0d3a\u0d3d\u0d4e\u0d54-\u0d56\u0d5f-\u0d61\u0d7a-\u0d7f\u0d85-\u0d96\u0d9a-\u0db1\u0db3-\u0dbb\u0dbd\u0dc0-\u0dc6\u0e01-\u0e30\u0e32-\u0e33\u0e40-\u0e46\u0e81-\u0e82\u0e84\u0e86-\u0e8a\u0e8c-\u0ea3\u0ea5\u0ea7-\u0eb0\u0eb2-\u0eb3\u0ebd\u0ec0-\u0ec4\u0ec6\u0edc-\u0edf\u0f00\u0f40-\u0f47\u0f49-\u0f6c\u0f88-\u0f8c\u1000-\u102a\u103f\u1050-\u1055\u105a-\u105d\u1061\u1065-\u1066\u106e-\u1070\u1075-\u1081\u108e\u10a0-\u10c5\u10c7\u10cd\u10d0-\u10fa\u10fc-\u1248\u124a-\u124d\u1250-\u1256\u1258\u125a-\u125d\u1260-\u1288\u128a-\u128d\u1290-\u12b0\u12b2-\u12b5\u12b8-\u12be\u12c0\u12c2-\u12c5\u12c8-\u12d6\u12d8-\u1310\u1312-\u1315\u1318-\u135a\u1380-\u138f\u13a0-\u13f5\u13f8-\u13fd\u1401-\u166c\u166f-\u167f\u1681-\u169a\u16a0-\u16ea\u16ee-\u16f8\u1700-\u170c\u170e-\u1711\u1720-\u1731\u1740-\u1751\u1760-\u176c\u176e-\u1770\u1780-\u17b3\u17d7\u17dc\u1820-\u1878\u1880-\u1884\u1887-\u18a8\u18aa\u18b0-\u18f5\u1900-\u191e\u1950-\u196d\u1970-\u1974\u1980-\u19ab\u19b0-\u19c9\u1a00-\u1a16\u1a20-\u1a54\u1aa7\u1b05-\u1b33\u1b45-\u1b4b\u1b83-\u1ba0\u1bae-\u1baf\u1bba-\u1be5\u1c00-\u1c23\u1c4d-\u1c4f\u1c5a-\u1c7d\u1c80-\u1c88\u1c90-\u1cba\u1cbd-\u1cbf\u1ce9-\u1cec\u1cee-\u1cf3\u1cf5-\u1cf6\u1cfa\u1d00-\u1dbf\u1e00-\u1f15\u1f18-\u1f1d\u1f20-\u1f45\u1f48-\u1f4d\u1f50-\u1f57\u1f59\u1f5b\u1f5d\u1f5f-\u1f7d\u1f80-\u1fb4\u1fb6-\u1fbc\u1fbe\u1fc2-\u1fc4\u1fc6-\u1fcc\u1fd0-\u1fd3\u1fd6-\u1fdb\u1fe0-\u1fec\u1ff2-\u1ff4\u1ff6-\u1ffc\u203f-\u2040\u2054\u2071\u207f\u2090-\u209c\u2102\u2107\u210a-\u2113\u2115\u2119-\u211d\u2124\u2126\u2128\u212a-\u212d\u212f-\u2139\u213c-\u213f\u2145-\u2149\u214e\u2160-\u2188\u2c00-\u2c2e\u2c30-\u2c5e\u2c60-\u2ce4\u2ceb-\u2cee\u2cf2-\u2cf3\u2d00-\u2d25\u2d27\u2d2d\u2d30-\u2d67\u2d6f\u2d80-\u2d96\u2da0-\u2da6\u2da8-\u2dae\u2db0-\u2db6\u2db8-\u2dbe\u2dc0-\u2dc6\u2dc8-\u2dce\u2dd0-\u2dd6\u2dd8-\u2dde\u3005-\u3007\u3021-\u3029\u3031-\u3035\u3038-\u303c\u3041-\u3096\u309d-\u309f\u30a1-\u30fa\u30fc-\u30ff\u3105-\u312f\u3131-\u318e\u31a0-\u31bf\u31f0-\u31ff\u3400-\u4dbf\u4e00-\u9ffc\ua000-\ua48c\ua4d0-\ua4fd\ua500-\ua60c\ua610-\ua61f\ua62a-\ua62b\ua640-\ua66e\ua67f-\ua69d\ua6a0-\ua6ef\ua717-\ua71f\ua722-\ua788\ua78b-\ua7bf\ua7c2-\ua7ca\ua7f5-\ua801\ua803-\ua805\ua807-\ua80a\ua80c-\ua822\ua840-\ua873\ua882-\ua8b3\ua8f2-\ua8f7\ua8fb\ua8fd-\ua8fe\ua90a-\ua925\ua930-\ua946\ua960-\ua97c\ua984-\ua9b2\ua9cf\ua9e0-\ua9e4\ua9e6-\ua9ef\ua9fa-\ua9fe\uaa00-\uaa28\uaa40-\uaa42\uaa44-\uaa4b\uaa60-\uaa76\uaa7a\uaa7e-\uaaaf\uaab1\uaab5-\uaab6\uaab9-\uaabd\uaac0\uaac2\uaadb-\uaadd\uaae0-\uaaea\uaaf2-\uaaf4\uab01-\uab06\uab09-\uab0e\uab11-\uab16\uab20-\uab26\uab28-\uab2e\uab30-\uab5a\uab5c-\uab69\uab70-\uabe2\uac00-\ud7a3\ud7b0-\ud7c6\ud7cb-\ud7fb\uf900-\ufa6d\ufa70-\ufad9\ufb00-\ufb06\ufb13-\ufb17\ufb1d\ufb1f-\ufb28\ufb2a-\ufb36\ufb38-\ufb3c\ufb3e\ufb40-\ufb41\ufb43-\ufb44\ufb46-\ufbb1\ufbd3-\ufd3d\ufd50-\ufd8f\ufd92-\ufdc7\ufdf0-\ufdfb\ufe33-\ufe34\ufe4d-\ufe4f\ufe70-\ufe74\ufe76-\ufefc\uff21-\uff3a\uff3f\uff41-\uff5a\uff66-\uffbe\uffc2-\uffc7\uffca-\uffcf\uffd2-\uffd7\uffda-\uffdc]
   ;

// Making changes here? Don't forget to update org.neo4j.util.UnicodeHelper!
fragment PART_LETTER
   : LETTER
   | [\u0030-\u0039\u0300-\u036f\u0483-\u0487\u058f\u0591-\u05bd\u05bf\u05c1-\u05c2\u05c4-\u05c5\u05c7\u060b\u0610-\u061a\u064b-\u0669\u0670\u06d6-\u06dc\u06df-\u06e4\u06e7-\u06e8\u06ea-\u06ed\u06f0-\u06f9\u0711\u0730-\u074a\u07a6-\u07b0\u07c0-\u07c9\u07eb-\u07f3\u07fd-\u07ff\u0816-\u0819\u081b-\u0823\u0825-\u0827\u0829-\u082d\u0859-\u085b\u08d3-\u08e1\u08e3-\u0903\u093a-\u093c\u093e-\u094f\u0951-\u0957\u0962-\u0963\u0966-\u096f\u0981-\u0983\u09bc\u09be-\u09c4\u09c7-\u09c8\u09cb-\u09cd\u09d7\u09e2-\u09e3\u09e6-\u09ef\u09f2-\u09f3\u09fb\u09fe\u0a01-\u0a03\u0a3c\u0a3e-\u0a42\u0a47-\u0a48\u0a4b-\u0a4d\u0a51\u0a66-\u0a71\u0a75\u0a81-\u0a83\u0abc\u0abe-\u0ac5\u0ac7-\u0ac9\u0acb-\u0acd\u0ae2-\u0ae3\u0ae6-\u0aef\u0af1\u0afa-\u0aff\u0b01-\u0b03\u0b3c\u0b3e-\u0b44\u0b47-\u0b48\u0b4b-\u0b4d\u0b55-\u0b57\u0b62-\u0b63\u0b66-\u0b6f\u0b82\u0bbe-\u0bc2\u0bc6-\u0bc8\u0bca-\u0bcd\u0bd7\u0be6-\u0bef\u0bf9\u0c00-\u0c04\u0c3e-\u0c44\u0c46-\u0c48\u0c4a-\u0c4d\u0c55-\u0c56\u0c62-\u0c63\u0c66-\u0c6f\u0c81-\u0c83\u0cbc\u0cbe-\u0cc4\u0cc6-\u0cc8\u0cca-\u0ccd\u0cd5-\u0cd6\u0ce2-\u0ce3\u0ce6-\u0cef\u0d00-\u0d03\u0d3b-\u0d3c\u0d3e-\u0d44\u0d46-\u0d48\u0d4a-\u0d4d\u0d57\u0d62-\u0d63\u0d66-\u0d6f\u0d81-\u0d83\u0dca\u0dcf-\u0dd4\u0dd6\u0dd8-\u0ddf\u0de6-\u0def\u0df2-\u0df3\u0e31\u0e34-\u0e3a\u0e3f\u0e47-\u0e4e\u0e50-\u0e59\u0eb1\u0eb4-\u0ebc\u0ec8-\u0ecd\u0ed0-\u0ed9\u0f18-\u0f19\u0f20-\u0f29\u0f35\u0f37\u0f39\u0f3e-\u0f3f\u0f71-\u0f84\u0f86-\u0f87\u0f8d-\u0f97\u0f99-\u0fbc\u0fc6\u102b-\u103e\u1040-\u1049\u1056-\u1059\u105e-\u1060\u1062-\u1064\u1067-\u106d\u1071-\u1074\u1082-\u108d\u108f-\u109d\u135d-\u135f\u1712-\u1714\u1732-\u1734\u1752-\u1753\u1772-\u1773\u17b4-\u17d3\u17db\u17dd\u17e0-\u17e9\u180b-\u180d\u1810-\u1819\u1885-\u1886\u18a9\u1920-\u192b\u1930-\u193b\u1946-\u194f\u19d0-\u19d9\u1a17-\u1a1b\u1a55-\u1a5e\u1a60-\u1a7c\u1a7f-\u1a89\u1a90-\u1a99\u1ab0-\u1abd\u1abf-\u1ac0\u1b00-\u1b04\u1b34-\u1b44\u1b50-\u1b59\u1b6b-\u1b73\u1b80-\u1b82\u1ba1-\u1bad\u1bb0-\u1bb9\u1be6-\u1bf3\u1c24-\u1c37\u1c40-\u1c49\u1c50-\u1c59\u1cd0-\u1cd2\u1cd4-\u1ce8\u1ced\u1cf4\u1cf7-\u1cf9\u1dc0-\u1df9\u1dfb-\u1dff\u20a0-\u20bf\u20d0-\u20dc\u20e1\u20e5-\u20f0\u2cef-\u2cf1\u2d7f\u2de0-\u2dff\u302a-\u302f\u3099-\u309a\ua620-\ua629\ua66f\ua674-\ua67d\ua69e-\ua69f\ua6f0-\ua6f1\ua802\ua806\ua80b\ua823-\ua827\ua82c\ua838\ua880-\ua881\ua8b4-\ua8c5\ua8d0-\ua8d9\ua8e0-\ua8f1\ua8ff-\ua909\ua926-\ua92d\ua947-\ua953\ua980-\ua983\ua9b3-\ua9c0\ua9d0-\ua9d9\ua9e5\ua9f0-\ua9f9\uaa29-\uaa36\uaa43\uaa4c-\uaa4d\uaa50-\uaa59\uaa7b-\uaa7d\uaab0\uaab2-\uaab4\uaab7-\uaab8\uaabe-\uaabf\uaac1\uaaeb-\uaaef\uaaf5-\uaaf6\uabe3-\uabea\uabec-\uabed\uabf0-\uabf9\ufb1e\ufdfc\ufe00-\ufe0f\ufe20-\ufe2f\ufe69\uff04\uff10-\uff19\uffe0-\uffe1\uffe5-\uffe6]
   ;

fragment A
   : [aA]
   ;

fragment B
   : [bB]
   ;

fragment C
   : [cC]
   ;

fragment D
   : [dD]
   ;

fragment E
   : [eE]
   ;

fragment F
   : [fF]
   ;

fragment G
   : [gG]
   ;

fragment H
   : [hH]
   ;

fragment I
   : [iI]
   ;

fragment J
   : [jJ]
   ;

fragment K
   : [kK]
   ;

fragment L
   : [lL]
   ;

fragment M
   : [mM]
   ;

fragment N
   : [nN]
   ;

fragment O
   : [oO]
   ;

fragment P
   : [pP]
   ;

fragment Q
   : [qQ]
   ;

fragment R
   : [rR]
   ;

fragment S
   : [sS]
   ;

fragment T
   : [tT]
   ;

fragment U
   : [uU]
   ;

fragment V
   : [vV]
   ;

fragment W
   : [wW]
   ;

fragment X
   : [xX]
   ;

fragment Y
   : [yY]
   ;

fragment Z
   : [zZ]
   ;

fragment ZERO
   : [0]
   ;

fragment ONE
   : [1]
   ;

fragment TWO
   : [2]
   ;

fragment THREE
   : [3]
   ;

fragment FOUR
   : [4]
   ;

fragment FIVE
   : [5]
   ;

fragment SIX
   : [16]
   ;

fragment SEVEN
   : [7]
   ;

fragment EIGHT
   : [8]
   ;

fragment NINE
   : [9]
   ;
   
fragment UNDERSCORE 
       : [\u005F]
       ;

// Should always be last in the file before modes
ErrorChar
    : .
    ;
