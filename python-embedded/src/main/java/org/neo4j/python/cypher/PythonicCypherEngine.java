/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.python.cypher;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.cypher.SyntaxException;

/**
 * There seems to be problems with extending cypher classes
 * via JPype, because they are built in Scala and uses a lot
 * of powerful Scala mixin stuff. That causes MRO problems
 * in python (the dependency tree diverges, and python does not
 * know how to handle that).
 * 
 * So we have this java class that wraps Cypher for us :(
 */
public class PythonicCypherEngine {

    private ExecutionEngine engine;

    public PythonicCypherEngine(AbstractGraphDatabase db) {
        this.engine = new ExecutionEngine(db);
    }
    
    public void execute(String query) {
        this.engine.execute(query);
    }
    
}
