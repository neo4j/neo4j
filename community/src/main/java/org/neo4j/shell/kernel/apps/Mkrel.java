/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Mimics the POSIX application "mkdir", but neo4j has relationships instead of
 * directories (if you look at Neo4j in a certain perspective).
 */
public class Mkrel extends GraphDatabaseApp
{
    /**
     * Constructs a new application which can create relationships in Neo4j.
     */
    public Mkrel()
    {
        this.addValueType( "t", new OptionContext( OptionValueType.MUST,
            "The relationship type" ) );
        this.addValueType( "n", new OptionContext( OptionValueType.MUST,
            "The node id to connect to" ) );
        this.addValueType( "d", new OptionContext( OptionValueType.MUST,
            "The direction: " + this.directionAlternatives() + "." ) );
        this.addValueType( "c", new OptionContext( OptionValueType.NONE,
            "Supplied if there should be created a new node" ) );
        this.addValueType( "v", new OptionContext( OptionValueType.NONE,
            "Verbose mode: display created nodes/relationships" ) );
    }

    @Override
    public String getDescription()
    {
        return "Creates a relationship to a node";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
        throws ShellException, RemoteException
    {
        assertCurrentIsNode( session );
        
        boolean createNode = parser.options().containsKey( "c" );
        boolean suppliedNode = parser.options().containsKey( "n" );
        Node node = null;
        if ( createNode )
        {
            node = getServer().getDb().createNode();
        }
        else if ( suppliedNode )
        {
            node = getNodeById( Long.parseLong( parser.options().get( "n" ) ) );
        }
        else
        {
            throw new ShellException( "Must either create node (-c)"
                + " or supply node id (-n <id>)" );
        }

        if ( parser.options().get( "t" ) == null )
        {
            throw new ShellException( "Must supply relationship type "
                + "(-t <relationship-type-name>)" );
        }
        RelationshipType type = this.getRelationshipType( parser.options().get(
            "t" ) );
        Direction direction = this.getDirection( parser.options().get( "d" ) );
        Node currentNode = getCurrent( session ).asNode();
        Node startNode = direction == Direction.OUTGOING ? currentNode : node;
        Node endNode = direction == Direction.OUTGOING ? node : currentNode;
        Relationship relationship =
            startNode.createRelationshipTo( endNode, type );
        boolean verbose = parser.options().containsKey( "v" );
        if ( createNode && verbose )
        {
            out.println( "Node " + getDisplayName(
                getServer(), session, node, false ) + " created" );
        }
        if ( verbose )
        {
            out.println( "Relationship " + getDisplayName(
                getServer(), session, relationship, true, false ) +
                " created" );
        }
        return null;
    }
}