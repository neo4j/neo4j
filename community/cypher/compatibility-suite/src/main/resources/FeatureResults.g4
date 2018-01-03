/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

node : nodeDesc ;

nodeDesc : '(' (label)* WS? (propertyMap)? ')' ;

relationship : relationshipDesc ;

relationshipDesc : '[' relationshipType (WS propertyMap)* ']' ;

path : '<' pathBody '>' ;

pathBody : nodeDesc (pathLink)* ;

pathLink : (forwardsRelationship | backwardsRelationship) nodeDesc ;

forwardsRelationship : '-' relationshipDesc '->' ;

backwardsRelationship : '<-' relationshipDesc '-' ;

integer : INTEGER_LITERAL ;

floatingPoint : FLOAT_LITERAL
              | INFINITY ;

bool : 'true'
     | 'false'
     ;

nullValue : 'null' ;

list : '[' (listContents)? ']' ;

listContents : listElement (', ' listElement)* ;

listElement : value ;

map : propertyMap ;

propertyMap : '{' (mapContents)? '}' ;

mapContents : keyValuePair (', ' keyValuePair)* ;

keyValuePair: propertyKey ':' WS? propertyValue ;

propertyKey : SYMBOLIC_NAME ;

propertyValue : value ;

relationshipType : ':' relationshipTypeName ;

relationshipTypeName : SYMBOLIC_NAME ;

label : ':' labelName ;

labelName : SYMBOLIC_NAME ;

INTEGER_LITERAL : ('-')? DECIMAL_LITERAL ;

DECIMAL_LITERAL : '0'
                | NONZERODIGIT DIGIT*
                ;

DIGIT : '0'
      | NONZERODIGIT
      ;

NONZERODIGIT : [1-9] ;

INFINITY : '-'? 'Inf' ;

FLOAT_LITERAL : '-'? FLOAT_REPR ;

FLOAT_REPR : DIGIT+ '.' DIGIT+ EXPONENTPART?
           | '.' DIGIT+ EXPONENTPART?
           | DIGIT EXPONENTPART
           | DIGIT+ EXPONENTPART?
           ;

EXPONENTPART :  ('E' | 'e') ('+' | '-')? DIGIT+ ;

SYMBOLIC_NAME : IDENTIFIER ;

WS : ' ' ;

IDENTIFIER : [a-zA-Z0-9$_]+ ;

// The string rule should ideally not include the apostrophes in the parsed value,
// but a lexer rule may not match the empty string, so I haven't found a way
// to define that quite well yet.

string : STRING_LITERAL ;

STRING_LITERAL : '\'' STRING_BODY* '\'' ;

STRING_BODY : '\u0000' .. '\u0026' // \u0027 is the string delimiter character (')
            | '\u0028' .. '\u01FF'
            | ESCAPED_APOSTROPHE
            ;

ESCAPED_APOSTROPHE : '\\\'' ;
