/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.commands.Query;

/**
 * This is the entry point to parse strings to {@link Query}-objects
 */
public class CypherParser
{
    private org.neo4j.cypher.parser.CypherParser inner;

    public CypherParser()
    {
        inner = new org.neo4j.cypher.parser.CypherParser();
    }

    public Query parse( String query ) throws SyntaxException
    {
        return inner.parse( query );
    }

    public static Query parseStrict(String query) throws SyntaxException {
        return new org.neo4j.cypher.parser.CypherParser().parse( query );
    }

    public static Query parseConsole(String query) throws SyntaxException {
        return new org.neo4j.cypher.parser.ConsoleCypherParser().parse( query );
    }
}
