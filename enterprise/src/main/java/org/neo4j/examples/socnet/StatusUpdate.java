/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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


package org.neo4j.examples.socnet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Traversal;

import java.util.Date;

import static org.neo4j.examples.socnet.RelTypes.NEXT;
import static org.neo4j.examples.socnet.RelTypes.STATUS;

public class StatusUpdate
{
    private final Node underlyingNode;
    static final String TEXT = "TEXT";
    static final String DATE = "DATE";

    public StatusUpdate( Node underlyingNode )
    {

        this.underlyingNode = underlyingNode;
    }

    public Node getUnderlyingNode()
    {
        return underlyingNode;
    }

    public Person getPerson()
    {
        return new Person( getPersonNode() );
    }

    private Node getPersonNode()
    {
        TraversalDescription traversalDescription = Traversal.description().
                depthFirst().
                relationships( NEXT, Direction.INCOMING ).
                relationships( STATUS, Direction.INCOMING ).
                filter( Traversal.returnWhereLastRelationshipTypeIs( STATUS ));

        Traverser traverser = traversalDescription.traverse( getUnderlyingNode() );

        return IteratorUtil.singleOrNull( traverser.iterator() ).endNode();
    }

    public String getStatusText()
    {
        return (String)underlyingNode.getProperty( TEXT );
    }

    public Date getDate()
    {
        Long l = (Long)underlyingNode.getProperty( DATE );

        return new Date( l );
    }

}
