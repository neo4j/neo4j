/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples.socnet;

import java.util.Date;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;

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
        TraversalDescription traversalDescription = underlyingNode.getGraphDatabase()
                .traversalDescription()
                .depthFirst()
                .relationships( NEXT, Direction.INCOMING )
                .relationships( STATUS, Direction.INCOMING )
                .evaluator( Evaluators.includeWhereLastRelationshipTypeIs( STATUS ) );

        Traverser traverser = traversalDescription.traverse( getUnderlyingNode() );

        return IteratorUtil.singleOrNull( traverser.iterator() ).endNode();
    }

    public String getStatusText()
    {
        return (String) underlyingNode.getProperty( TEXT );
    }

    public Date getDate()
    {
        Long l = (Long) underlyingNode.getProperty( DATE );

        return new Date( l );
    }

}
