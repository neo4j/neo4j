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
 * Sets a property for the current node.
 */
public class Set extends NodeOrRelationshipApp
{
    /**
     * Constructs a new "set" application.
     */
    public Set()
    {
        super();
        this.addValueType( "t", new OptionContext( OptionValueType.MUST,
            "Value type, String, Integer, Long, Byte a.s.o." ) );
    }

    @Override
    public String getDescription()
    {
        return "Sets a property on the current node. Usage: set <key> <value>";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
        throws ShellException
    {
        if ( parser.arguments().size() < 2 )
        {
            throw new ShellException( "Must supply key and value, "
                + "like: set -t String title \"This is a neo node\"" );
        }

        String key = parser.arguments().get( 0 );
        Class<?> type = this.getValueType( parser );
        Object value = null;
        try
        {
            value = type.getConstructor( String.class ).newInstance(
                parser.arguments().get( 1 ) );
        }
        catch ( Exception e )
        {
            throw new ShellException( e );
        }

        Node node = this.getCurrentNode( session );
        NodeOrRelationship thing = getNodeOrRelationship( node, parser );
        thing.setProperty( key, value );
        return null;
    }

    private Class<?> getValueType( AppCommandParser parser )
        throws ShellException
    {
        String type = parser.options().containsKey( "t" ) ? parser.options()
            .get( "t" ) : String.class.getName();
        Class<?> cls = null;
        try
        {
            cls = Class.forName( type );
        }
        catch ( ClassNotFoundException e )
        {
            // Ok
        }

        try
        {
            cls = Class.forName( String.class.getPackage().getName() + "."
                + type );
        }
        catch ( ClassNotFoundException e )
        {
            // Ok
        }

        if ( cls == null )
        {
            throw new ShellException( "Invalid value type '" + type + "'" );
        }
        return cls;
    }
}