/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.graphdb;

/**
 * This exception will be thrown if a request is made to a node, relationship or
 * property that does not exist. As an example, using
 * {@link GraphDatabaseService#getNodeById} passing in an id that does not exist
 * will cause this exception to be thrown.
 * {@link PropertyContainer#getProperty(String)} will also throw this exception
 * if the given key does not exist.
 * <p>
 * Another scenario when this exception will be thrown is if one or more
 * transactions keep a reference to a node or relationship that gets deleted in
 * some other transaction. If the deleting transaction commits all other
 * transactions having a reference to the deleted node or relationship will
 * throw this exception when invoking any of the methods on the node or
 * relationship.
 * 
 * @see GraphDatabaseService
 */
public class NotFoundException extends RuntimeException
{
    public NotFoundException()
    {
        super();
    }

    public NotFoundException( String message )
    {
        super( message );
    }

    public NotFoundException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public NotFoundException( Throwable cause )
    {
        super( cause );
    }
}