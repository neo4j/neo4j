grammar FeatureResults;

value : node
      | relationship
      | path
      | integer
      | floatingPoint
      | string
      | bool
      | nullValue
      | list
      | map
      ;

node : '(' (label)* (WS propertyMap)? ')' ;

relationship : '[' relationshipType (WS propertyMap)* ']' ;

path : '<' (pathElement)? '>'  ;

pathElement : node (', ' relationship ', ' node)* ;

integer : IntegerLiteral ;

floatingPoint : FloatingPointLiteral
              | INFINITY ;

bool : 'true' | 'false' ;

nullValue : 'null' ;

list : '[' (listContents)? ']' ;

listContents : listElement (', ' listElement)* ;

listElement : value ;

map : propertyMap ;

propertyMap : '{' (mapContents)? '}' ;

mapContents : keyValuePair (', ' keyValuePair)* ;

keyValuePair: propertyKey ':' propertyValue ;

propertyKey : SymbolicNameString ;

propertyValue : value ;

relationshipType : ':' SymbolicNameString ;

label : ':' SymbolicNameString ;

IntegerLiteral : ('-')? DecimalLiteral ;

DecimalLiteral : '0' | NONZERODIGIT DIGIT* ;

DIGIT          :  '0' | NONZERODIGIT ;
NONZERODIGIT   :  [1-9] ;

INFINITY : '-'? 'Inf' ;

FloatingPointLiteral : '-'? FloatingPointRepr ;

FloatingPointRepr :  DIGIT+ '.' DIGIT+ EXPONENTPART?
                  |  '.' DIGIT+ EXPONENTPART?
                  |  DIGIT EXPONENTPART
                  |  DIGIT+ EXPONENTPART? ;

EXPONENTPART :  ('E' | 'e') ('+' | '-')? DIGIT+;

SymbolicNameString : IDENTIFIER ;

WS : ' ' ;

IDENTIFIER : [a-zA-Z0-9$_]+ ;

string : StringLiteral ;

StringLiteral : '\'' StringElement* '\'' ;

fragment StringElement    :  '\u0000' .. '\u0026' | '\u0028' .. '\u01FF' // escaping for ' (apostrophe/single quote)
                          | EscapedSingleQuote ;
fragment EscapedSingleQuote : '\\\'' ;
