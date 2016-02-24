/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

node : '(' (label)* WS? (propertyMap)? ')' ;

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

keyValuePair: propertyKey ':' WS? propertyValue ;

propertyKey : SymbolicNameString ;

propertyValue : value ;

relationshipType : ':' relationshipTypeName ;

relationshipTypeName : SymbolicNameString ;

label : ':' labelName ;

labelName : SymbolicNameString ;

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
