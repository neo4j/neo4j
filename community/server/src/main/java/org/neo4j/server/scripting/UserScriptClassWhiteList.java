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
package org.neo4j.server.scripting;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;

/**
 * A set of classes that we trust unknown entities to work with. These will be accessible to users that have remote access
 * to a Neo4j database.
 *
 * Please make sure that, when you add a class to this whitelist, it does not allow any sort of side effects that could
 * be dangerous, such as accessing the file system or starting remote connections or threads, unless we clearly know
 * the effect of it.
 *
 * Assume that people using these classes will be using a Neo4j database that runs in a co-located environment, and that
 * the world will burn if someone is able to access a database they are not supposed to have access to.
 *
 * This White List should not be end-user configurable. If we let end-users set up their own whitelists, then each database
 * would have it's own "language" for extension, which will lead to massive complications in the longer term. Better
 * to have an authoritative white list that is the same for all databases.
 */
public class UserScriptClassWhiteList
{

    public static Set<String> getWhiteList()
    {
        HashSet<String> safe = new HashSet<String>();

        // Core API concepts

        safe.add( Path.class.getName() );
        safe.add( Node.class.getName() );
        safe.add( Relationship.class.getName() );
        safe.add( RelationshipType.class.getName() );
        safe.add( DynamicRelationshipType.class.getName() );
        safe.add( Lock.class.getName() );

        safe.add( NotFoundException.class.getName() );

        // Traversal concepts

        safe.add( Direction.class.getName() );
        safe.add( Evaluation.class.getName() );

        // Java Core API

        safe.add( Object.class.getName() );
        safe.add( String.class.getName() );
        safe.add( Integer.class.getName() );
        safe.add( Long.class.getName() );
        safe.add( Float.class.getName() );
        safe.add( Double.class.getName() );
        safe.add( Boolean.class.getName() );


        // This is a work-around, since these are not supposed to be publicly available.
        // The reason we need to add it here is, most likely, that some methods in the API
        // returns these rather than the corresponding interfaces, which means our white list
        // checker doesn't know which interface to cast to (since there could be several). Instead
        // we allow users direct access to these classes for now.
        safe.add( "org.neo4j.kernel.impl.traversal.StartNodeTraversalBranch" );
        safe.add( "org.neo4j.kernel.impl.traversal.TraversalBranchImpl" );
        safe.add( "org.neo4j.kernel.impl.core.NodeProxy" );


        return safe;
    }

}
