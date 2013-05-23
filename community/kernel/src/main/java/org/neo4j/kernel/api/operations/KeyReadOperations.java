/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.impl.core.Token;

public interface KeyReadOperations
{
    /**
     * Returns a label id for a label name. If the label doesn't exist a
     * {@link org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException} will be thrown.
     */
    long labelGetForName( String labelName ) throws LabelNotFoundKernelException;

    /** Returns the label name for the given label id. */
    String labelGetName( long labelId ) throws LabelNotFoundKernelException;

    /**
     * Returns a property key id for the given property key. If the property key doesn't exist a
     * {@link org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException} will be thrown.
     */
    long propertyKeyGetForName( String propertyKeyName ) throws PropertyKeyNotFoundException;

    /** Returns the name of a property given its property key id */
    String propertyKeyGetName( long propertyKeyId ) throws PropertyKeyIdNotFoundException;

    /** Returns the labels currently stored in the database **/
    Iterator<Token> labelsGetAllTokens(); // TODO: Token is a store level concern, should not make it this far up the stack
}
