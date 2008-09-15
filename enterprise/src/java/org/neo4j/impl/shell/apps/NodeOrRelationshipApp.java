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
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.ShellException;

abstract class NodeOrRelationshipApp extends NeoApp
{
    public NodeOrRelationshipApp()
    {
        this.addValueType( "e", new OptionContext( OptionValueType.MUST,
            "Temporarily select a connected relationship to do the "
                + "operation on" ) );
    }

    protected NodeOrRelationship getNodeOrRelationship( Node node,
        AppCommandParser parser ) throws ShellException
    {
        if ( parser.options().containsKey( "e" ) )
        {
            long relId = Long.parseLong( parser.options().get( "e" ) );
            Relationship rel = findRelationshipOnNode( node, relId );
            if ( rel == null )
            {
                throw new ShellException( "No relationship " + relId
                    + " connected to the current node" );
            }
            return NodeOrRelationship.wrap( rel );
        }
        return NodeOrRelationship.wrap( node );
    }

    protected Relationship findRelationshipOnNode( Node node, long id )
    {
        for ( Relationship rel : node.getRelationships() )
        {
            if ( rel.getId() == id )
            {
                return rel;
            }
        }
        return null;
    }
}