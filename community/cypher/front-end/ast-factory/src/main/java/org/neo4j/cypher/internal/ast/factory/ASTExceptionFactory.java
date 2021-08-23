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
package org.neo4j.cypher.internal.ast.factory;

import java.util.Arrays;
import java.util.List;

import static org.neo4j.cypher.internal.ast.factory.ConstraintType.NODE_EXISTS;
import static org.neo4j.cypher.internal.ast.factory.ConstraintType.NODE_IS_NOT_NULL;
import static org.neo4j.cypher.internal.ast.factory.ConstraintType.NODE_KEY;
import static org.neo4j.cypher.internal.ast.factory.ConstraintType.UNIQUE;

public interface ASTExceptionFactory
{
    Exception syntaxException( String got, List<String> expected, Exception source, int offset, int line, int column );

    Exception syntaxException( Exception source, int offset, int line, int column );

    //Exception messages

    String undefinedConstraintType = String.format( "No constraint type %s is defined",
                                                    Arrays.asList( NODE_EXISTS.description(), UNIQUE.description(), NODE_IS_NOT_NULL.description(),
                                                                   NODE_KEY.description() ) );
    String invalidDropCommand = "Unsupported drop constraint command: Please delete the constraint by name instead";
    String invalidCatalogStatement = "CATALOG is not allowed for this statement";

    static String relationshipPattternNotAllowed( ConstraintType type )
    {
        return String.format( "'%s' does not allow relationship patterns", type.description() );
    }

    static String onlySinglePropertyAllowed( ConstraintType type )
    {
        return String.format("'%s' does not allow multiple properties", type.description());
    }

    static String constraintTypeNotAllowed( ConstraintType newType, ConstraintType oldType )
    {
        return String.format( "Invalid input '%s': conflicting with '%s'", newType.description(), oldType.description() );
    }

    static String invalidShowFilterType( String command, ShowCommandFilterTypes got )
    {
        return String.format( "Filter type %s is not defined for show %s command.", got.description(), command );
    }

    static String invalidCreateIndexType( CreateIndexTypes got )
    {
        return String.format( "Index type %s is not defined for create index command.", got.description() );
    }
}
