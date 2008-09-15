/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.shell.apps;

import org.neo4j.api.core.Node;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. renames a property. It
 * could also (regarding POSIX) move nodes, but it doesn't).
 */
public class Mv extends NodeOrRelationshipApp
{
    /**
     * Constructs a new "mv" application.
     */
    public Mv()
    {
        super();
        this.addValueType( "o", new OptionContext( OptionValueType.NONE,
            "To override if the key already exists" ) );
    }

    @Override
    public String getDescription()
    {
        return "Renames a property. Usage: mv <key> <new-key>";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
        throws ShellException
    {
        if ( parser.arguments().size() < 2 )
        {
            throw new ShellException( "Must supply <from-key> <to-key> "
                + "arguments, like: mv name \"given name\"" );
        }
        String fromKey = parser.arguments().get( 0 );
        String toKey = parser.arguments().get( 1 );
        boolean mayOverwrite = parser.options().containsKey( "o" );
        Node node = this.getCurrentNode( session );
        NodeOrRelationship thing = getNodeOrRelationship( node, parser );
        if ( !thing.hasProperty( fromKey ) )
        {
            throw new ShellException( "Property '" + fromKey
                + "' doesn't exist" );
        }
        if ( thing.hasProperty( toKey ) )
        {
            if ( !mayOverwrite )
            {
                throw new ShellException( "Property '" + toKey
                    + "' already exists, supply -o flag to overwrite" );
            }
            else
            {
                thing.removeProperty( toKey );
            }
        }

        Object value = thing.removeProperty( fromKey );
        thing.setProperty( toKey, value );
        return null;
    }
}