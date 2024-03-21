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
lexer grammar CypherLexer;

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
   : '//' (~ [\n\r])* (('\n' | '\r' | '\r\n'))? -> channel (HIDDEN)
   ;

MORE0
   : '/**' ~ [/] -> more , mode (IN_FORMAL_COMMENT)
   ;

MORE1
   : '/*' -> more , mode (IN_MULTI_LINE_COMMENT)
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
   : '0' [xX] (PART_LETTER)*
   ;

UNSIGNED_OCTAL_INTEGER
   : '0' ('o')? (PART_LETTER)*
   ;

STRING1_OPEN
   : '\'' -> more , mode (STRING1)
   ;

STRING2_OPEN
   : '"' -> more , mode (STRING2)
   ;

ESCAPED_SYMBOLIC_NAME_OPEN
   : '`' -> more , mode (ESC_SYMB_NAME)
   ;

ACCESS
   : A C C E S S
   ;

ACTIVE
   : A C T I V E
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

ALL_SHORTEST_PATH
   : A L L S H O R T E S T P A T H S
   ;

ALL
   : A L L
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
   : A S C E N D I N G -> type (ASC)
   ;

ASSERT
   : A S S E R T
   ;

ASSIGN
   : A S S I G N
   ;

AT
   : A T
   ;

BAR
   : '|'
   ;

BINDINGS
   : B I N D I N G S
   ;

BOOL
   : B O O L -> type (BOOLEAN)
   ;

BOOLEAN
   : B O O L E A N
   ;

BOOSTED
   : B O O S T E D
   ;

BREAK
   : B R E A K
   ;

BRIEF
   : B R I E F
   ;

BTREE
   : B T R E E
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

COMMIT
   : C O M M I T
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

CONTINUE
   : C O N T I N U E
   ;

COUNT
   : C O U N T
   ;

CREATE
   : C R E A T E
   ;

CSV
   : C S V
   ;

CURRENT
   : C U R R E N T
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
   : D E S C E N D I N G -> type (DESC)
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

DISTINCT
   : D I S T I N C T
   ;

DIVIDE
   : '/'
   ;

DOT
   : '.'
   ;

DOTDOT
   : '..'
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

ENCRYPTED
   : E N C R Y P T E D
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

FINISH
   : F I N I S H
   ;

FLOAT
   : F L O A T
   ;

FOR
   : F O R
   ;

FOREACH
   : F O R E A C H
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
   : G R O U P S -> type (GROUP)
   ;

GT
   : '>'
   ;

HEADERS
   : H E A D E R S
   ;

HOME
   : H O M E
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
   : I N F -> type (INFINITY)
   ;

INFINITY
   : I N F I N I T Y
   ;

INSERT
   : I N S E R T
   ;

INT
   : I N T
   ;

INTEGER
   : I N T E G E R
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

LABELS
   : L A B E L S
   ;

AMPERSAND
   : '&'
   ;

EXCLAMATION_MARK
   : '!'
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

MAP
   : M A P
   ;

MATCH
   : M A T C H
   ;

MERGE
   : M E R G E
   ;

MINUS
   : '-'
   ;

PERCENT
   : '%'
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

OF
   : O F
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

OUTPUT
   : O U T P U T
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
   : P A T H S -> type (PATH)
   ;

PERIODIC
   : P E R I O D I C
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
   : P R I M A R I E S -> type (PRIMARY)
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

PROPERTY
   : P R O P E R T Y
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

REPORT
   : R E P O R T
   ;

REQUIRE
   : R E Q U I R E
   ;

REQUIRED
   : R E Q U I R E D
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

RPAREN
   : ')'
   ;

SCAN
   : S C A N
   ;

SEC
   : S E C -> type (SECONDS)
   ;

SECOND
   : S E C O N D -> type (SECONDS)
   ;

SECONDARY
   : S E C O N D A R Y
   ;

SECONDARIES
   : S E C O N D A R I E S -> type (SECONDARY)
   ;

SECONDS
   : S E C O N D S
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
   : S E T T I N G S -> type(SETTING)
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

TRANSACTION
   : T R A N S A C T I O N
   ;

TRANSACTIONS
   : T R A N S A C T I O N S
   ;

TRAVERSE
   : T R A V E R S E
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
   : V A R C H A R -> type (STRING)
   ;

VECTOR
   : V E C T O R
   ;

VERBOSE
   : V E R B O S E
   ;

VERTEX
   : V E R T E X
   ;

WAIT
   : W A I T
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

ZONED
   : Z O N E D
   ;

IDENTIFIER
   : LETTER (PART_LETTER)*
   ;

ErrorChar
   : .
   ;

fragment LETTER
   : [A-Z_a-zªµºÀ-ÖØ-öø-ˁˆ-ˑˠ-ˤˬˮͰ-ʹͶ-ͷͺ-ͽͿΆΈ-ΊΌΎ-ΡΣ-ϵϷ-ҁҊ-ԯԱ-Ֆՙՠ-ֈא-תׯ-ײؠ-يٮ-ٯٱ-ۓەۥ-ۦۮ-ۯۺ-ۼۿܐܒ-ܯݍ-ޥޱߊ-ߪߴ-ߵߺࠀ-ࠕࠚࠤࠨࡀ-ࡘࡠ-ࡪࢠ-ࢴࢶ-ࣇऄ-हऽॐक़-ॡॱ-ঀঅ-ঌএ-ঐও-নপ-রলশ-হঽৎড়-ঢ়য়-ৡৰ-ৱৼਅ-ਊਏ-ਐਓ-ਨਪ-ਰਲ-ਲ਼ਵ-ਸ਼ਸ-ਹਖ਼-ੜਫ਼ੲ-ੴઅ-ઍએ-ઑઓ-નપ-રલ-ળવ-હઽૐૠ-ૡૹଅ-ଌଏ-ଐଓ-ନପ-ରଲ-ଳଵ-ହଽଡ଼-ଢ଼ୟ-ୡୱஃஅ-ஊஎ-ஐஒ-கங-சஜஞ-டண-தந-பம-ஹௐఅ-ఌఎ-ఐఒ-నప-హఽౘ-ౚౠ-ౡಀಅ-ಌಎ-ಐಒ-ನಪ-ಳವ-ಹಽೞೠ-ೡೱ-ೲഄ-ഌഎ-ഐഒ-ഺഽൎൔ-ൖൟ-ൡൺ-ൿඅ-ඖක-නඳ-රලව-ෆก-ะา-ำเ-ๆກ-ຂຄຆ-ຊຌ-ຣລວ-ະາ-ຳຽເ-ໄໆໜ-ໟༀཀ-ཇཉ-ཬྈ-ྌက-ဪဿၐ-ၕၚ-ၝၡၥ-ၦၮ-ၰၵ-ႁႎႠ-ჅჇჍა-ჺჼ-ቈቊ-ቍቐ-ቖቘቚ-ቝበ-ኈኊ-ኍነ-ኰኲ-ኵኸ-ኾዀዂ-ዅወ-ዖዘ-ጐጒ-ጕጘ-ፚᎀ-ᎏᎠ-Ᏽᏸ-ᏽᐁ-ᙬᙯ-ᙿᚁ-ᚚᚠ-ᛪᛮ-ᛸᜀ-ᜌᜎ-ᜑᜠ-ᜱᝀ-ᝑᝠ-ᝬᝮ-ᝰក-ឳៗៜᠠ-ᡸᢀ-ᢄᢇ-ᢨᢪᢰ-ᣵᤀ-ᤞᥐ-ᥭᥰ-ᥴᦀ-ᦫᦰ-ᧉᨀ-ᨖᨠ-ᩔᪧᬅ-ᬳᭅ-ᭋᮃ-ᮠᮮ-ᮯᮺ-ᯥᰀ-ᰣᱍ-ᱏᱚ-ᱽᲀ-ᲈᲐ-ᲺᲽ-Ჿᳩ-ᳬᳮ-ᳳᳵ-ᳶᳺᴀ-ᶿḀ-ἕἘ-Ἕἠ-ὅὈ-Ὅὐ-ὗὙὛὝὟ-ώᾀ-ᾴᾶ-ᾼιῂ-ῄῆ-ῌῐ-ΐῖ-Ίῠ-Ῥῲ-ῴῶ-ῼ‿-⁀⁔ⁱⁿₐ-ₜℂℇℊ-ℓℕℙ-ℝℤΩℨK-ℭℯ-ℹℼ-ℿⅅ-ⅉⅎⅠ-ↈⰀ-Ⱞⰰ-ⱞⱠ-ⳤⳫ-ⳮⳲ-ⳳⴀ-ⴥⴧⴭⴰ-ⵧⵯⶀ-ⶖⶠ-ⶦⶨ-ⶮⶰ-ⶶⶸ-ⶾⷀ-ⷆⷈ-ⷎⷐ-ⷖⷘ-ⷞⸯ々-〇〡-〩〱-〵〸-〼ぁ-ゖゝ-ゟァ-ヺー-ヿㄅ-ㄯㄱ-ㆎㆠ-ㆿㇰ-ㇿ㐀-䶿一-鿼ꀀ-ꒌꓐ-ꓽꔀ-ꘌꘐ-ꘟꘪ-ꘫꙀ-ꙮꙿ-ꚝꚠ-ꛯꜗ-ꜟꜢ-ꞈꞋ-ꞿꟂ-ꟊꟵ-ꠁꠃ-ꠅꠇ-ꠊꠌ-ꠢꡀ-ꡳꢂ-ꢳꣲ-ꣷꣻꣽ-ꣾꤊ-ꤥꤰ-ꥆꥠ-ꥼꦄ-ꦲꧏꧠ-ꧤꧦ-ꧯꧺ-ꧾꨀ-ꨨꩀ-ꩂꩄ-ꩋꩠ-ꩶꩺꩾ-ꪯꪱꪵ-ꪶꪹ-ꪽꫀꫂꫛ-ꫝꫠ-ꫪꫲ-ꫴꬁ-ꬆꬉ-ꬎꬑ-ꬖꬠ-ꬦꬨ-ꬮꬰ-ꭚꭜ-ꭩꭰ-ꯢ가-힣ힰ-ퟆퟋ-ퟻ豈-舘並-龎ﬀ-ﬆﬓ-ﬗיִײַ-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻ︳-︴﹍-﹏ﹰ-ﹴﹶ-ﻼＡ-Ｚ＿ａ-ｚｦ-ﾾￂ-ￇￊ-ￏￒ-ￗￚ-ￜ]
   ;

fragment PART_LETTER
   : [ --$0-9A-Z_a-z-¢-¥ª\u00ADµºÀ-ÖØ-öø-ˁˆ-ˑˠ-ˤˬˮ̀-ʹͶ-ͷͺ-ͽͿΆΈ-ΊΌΎ-ΡΣ-ϵϷ-ҁ҃-҇Ҋ-ԯԱ-Ֆՙՠ-ֈ֏֑-ֽֿׁ-ׂׄ-ׇׅא-תׯ-ײ؀-؅؋ؐ-ؚ\u061Cؠ-٩ٮ-ۓە-۝۟-۪ۨ-ۼۿ܏-݊ݍ-ޱ߀-ߵߺ߽-࠭ࡀ-࡛ࡠ-ࡪࢠ-ࢴࢶ-ࣇ࣓-ॣ०-९ॱ-ঃঅ-ঌএ-ঐও-নপ-রলশ-হ়-ৄে-ৈো-ৎৗড়-ঢ়য়-ৣ০-৳৻-ৼ৾ਁ-ਃਅ-ਊਏ-ਐਓ-ਨਪ-ਰਲ-ਲ਼ਵ-ਸ਼ਸ-ਹ਼ਾ-ੂੇ-ੈੋ-੍ੑਖ਼-ੜਫ਼੦-ੵઁ-ઃઅ-ઍએ-ઑઓ-નપ-રલ-ળવ-હ઼-ૅે-ૉો-્ૐૠ-ૣ૦-૯૱ૹ-૿ଁ-ଃଅ-ଌଏ-ଐଓ-ନପ-ରଲ-ଳଵ-ହ଼-ୄେ-ୈୋ-୍୕-ୗଡ଼-ଢ଼ୟ-ୣ୦-୯ୱஂ-ஃஅ-ஊஎ-ஐஒ-கங-சஜஞ-டண-தந-பம-ஹா-ூெ-ைொ-்ௐௗ௦-௯௹ఀ-ఌఎ-ఐఒ-నప-హఽ-ౄె-ైొ-్ౕ-ౖౘ-ౚౠ-ౣ౦-౯ಀ-ಃಅ-ಌಎ-ಐಒ-ನಪ-ಳವ-ಹ಼-ೄೆ-ೈೊ-್ೕ-ೖೞೠ-ೣ೦-೯ೱ-ೲഀ-ഌഎ-ഐഒ-ൄെ-ൈൊ-ൎൔ-ൗൟ-ൣ൦-൯ൺ-ൿඁ-ඃඅ-ඖක-නඳ-රලව-ෆ්ා-ුූෘ-ෟ෦-෯ෲ-ෳก-ฺ฿-๎๐-๙ກ-ຂຄຆ-ຊຌ-ຣລວ-ຽເ-ໄໆ່-ໍ໐-໙ໜ-ໟༀ༘-༙༠-༩༹༵༷༾-ཇཉ-ཬཱ-྄྆-ྗྙ-ྼ࿆က-၉ၐ-ႝႠ-ჅჇჍა-ჺჼ-ቈቊ-ቍቐ-ቖቘቚ-ቝበ-ኈኊ-ኍነ-ኰኲ-ኵኸ-ኾዀዂ-ዅወ-ዖዘ-ጐጒ-ጕጘ-ፚ፝-፟ᎀ-ᎏᎠ-Ᏽᏸ-ᏽᐁ-ᙬᙯ-ᙿᚁ-ᚚᚠ-ᛪᛮ-ᛸᜀ-ᜌᜎ-᜔ᜠ-᜴ᝀ-ᝓᝠ-ᝬᝮ-ᝰᝲ-ᝳក-៓ៗ៛-៝០-៩᠋-᠎᠐-᠙ᠠ-ᡸᢀ-ᢪᢰ-ᣵᤀ-ᤞᤠ-ᤫᤰ-᤻᥆-ᥭᥰ-ᥴᦀ-ᦫᦰ-ᧉ᧐-᧙ᨀ-ᨛᨠ-ᩞ᩠-᩿᩼-᪉᪐-᪙ᪧ᪰-᪽ᪿ-ᫀᬀ-ᭋ᭐-᭙᭫-᭳ᮀ-᯳ᰀ-᰷᱀-᱉ᱍ-ᱽᲀ-ᲈᲐ-ᲺᲽ-Ჿ᳐-᳔᳒-ᳺᴀ-᷹᷻-ἕἘ-Ἕἠ-ὅὈ-Ὅὐ-ὗὙὛὝὟ-ώᾀ-ᾴᾶ-ᾼιῂ-ῄῆ-ῌῐ-ΐῖ-Ίῠ-Ῥῲ-ῴῶ-ῼ​-‏‪-‮‿-⁀⁔⁠-⁤⁦-⁯ⁱⁿₐ-ₜ₠-₿⃐-⃥⃜⃡-⃰ℂℇℊ-ℓℕℙ-ℝℤΩℨK-ℭℯ-ℹℼ-ℿⅅ-ⅉⅎⅠ-ↈⰀ-Ⱞⰰ-ⱞⱠ-ⳤⳫ-ⳳⴀ-ⴥⴧⴭⴰ-ⵧⵯ⵿-ⶖⶠ-ⶦⶨ-ⶮⶰ-ⶶⶸ-ⶾⷀ-ⷆⷈ-ⷎⷐ-ⷖⷘ-ⷞⷠ-ⷿⸯ々-〇〡-〯〱-〵〸-〼ぁ-ゖ゙-゚ゝ-ゟァ-ヺー-ヿㄅ-ㄯㄱ-ㆎㆠ-ㆿㇰ-ㇿ㐀-䶿一-鿼ꀀ-ꒌꓐ-ꓽꔀ-ꘌꘐ-ꘫꙀ-꙯ꙴ-꙽ꙿ-꛱ꜗ-ꜟꜢ-ꞈꞋ-ꞿꟂ-ꟊꟵ-ꠧ꠬꠸ꡀ-ꡳꢀ-ꣅ꣐-꣙꣠-ꣷꣻꣽ-꤭ꤰ-꥓ꥠ-ꥼꦀ-꧀ꧏ-꧙ꧠ-ꧾꨀ-ꨶꩀ-ꩍ꩐-꩙ꩠ-ꩶꩺ-ꫂꫛ-ꫝꫠ-ꫯꫲ-꫶ꬁ-ꬆꬉ-ꬎꬑ-ꬖꬠ-ꬦꬨ-ꬮꬰ-ꭚꭜ-ꭩꭰ-ꯪ꯬-꯭꯰-꯹가-힣ힰ-ퟆퟋ-ퟻ豈-舘並-龎ﬀ-ﬆﬓ-ﬗיִ-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-﷼︀-️︠-︯︳-︴﹍-﹏﹩ﹰ-ﹴﹶ-ﻼ\uFEFF＄０-９Ａ-Ｚ＿ａ-ｚｦ-ﾾￂ-ￇￊ-ￏￒ-ￗￚ-ￜ￠-￡￥-￦￹-￻]
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

mode IN_FORMAL_COMMENT;
FORMAL_COMMENT
   : '*/' -> channel (HIDDEN) , mode (DEFAULT_MODE)
   ;

MORE2
   : . -> more
   ;

mode STRING1;
MORE3
   : '\\\\' -> more
   ;

MORE4
   : '\\\'' -> more
   ;

MORE5
   : '\\"' -> more
   ;

MORE6
   : '\\b' -> more
   ;

MORE7
   : '\\f' -> more
   ;

MORE8
   : '\\n' -> more
   ;

MORE9
   : '\\r' -> more
   ;

MORE10
   : '\\t' -> more
   ;

MORE11
   : '\\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]' -> more
   ;

MORE12
   : ~ ['] -> more
   ;

STRING_LITERAL1
   : '\'' -> mode (DEFAULT_MODE)
   ;

mode STRING2;
MORE13
   : '\\\\' -> more
   ;

MORE14
   : '\\\'' -> more
   ;

MORE15
   : '\\"' -> more
   ;

MORE16
   : '\\b' -> more
   ;

MORE17
   : '\\f' -> more
   ;

MORE18
   : '\\n' -> more
   ;

MORE19
   : '\\r' -> more
   ;

MORE20
   : '\\t' -> more
   ;

MORE21
   : '\\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]' -> more
   ;

MORE22
   : ~ ["] -> more
   ;

STRING_LITERAL2
   : '"' -> mode (DEFAULT_MODE)
   ;

mode IN_MULTI_LINE_COMMENT;
MULTI_LINE_COMMENT
   : '*/' -> channel (HIDDEN) , mode (DEFAULT_MODE)
   ;

IN_MULTI_LINE_COMMENT_MORE2
   : MORE2 -> more
   ;

mode ESC_SYMB_NAME;
MORE23
   : ~ [`] -> more
   ;

MORE24
   : '``' -> more
   ;

ESCAPED_SYMBOLIC_NAME
   : '`' -> mode (DEFAULT_MODE)
   ;

mode IN_SINGLE_LINE_COMMENT;
IN_SINGLE_LINE_COMMENT_MORE2
   : MORE2 -> more
   ;

