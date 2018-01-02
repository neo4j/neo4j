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
package org.neo4j.kernel.impl.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.core.Token;

public interface KeyReadOperations
{
    int NO_SUCH_LABEL = StatementConstants.NO_SUCH_LABEL;
    int NO_SUCH_PROPERTY_KEY = StatementConstants.NO_SUCH_PROPERTY_KEY;
    int NO_SUCH_RELATIONSHIP_TYPE = StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

    /** Returns a label id for a label name. If the label doesn't exist, {@link #NO_SUCH_LABEL} will be returned. */
    int labelGetForName( Statement state, String labelName );

    /** Returns the label name for the given label id. */
    String labelGetName( Statement state, int labelId ) throws LabelNotFoundKernelException;

    /**
     * Returns a property key id for the given property key. If the property key doesn't exist,
     * {@link #NO_SUCH_PROPERTY_KEY} will be returned.
     */
    int propertyKeyGetForName( Statement state, String propertyKeyName );

    /** Returns the name of a property given its property key id */
    String propertyKeyGetName( Statement state, int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException;

    /** Returns the property keys currently stored in the database */
    Iterator<Token> propertyKeyGetAllTokens( Statement state );

    /** Returns the labels currently stored in the database **/
    Iterator<Token> labelsGetAllTokens( Statement state ); // TODO: Token is a store level concern, should not make it this far up the stack

    int relationshipTypeGetForName( Statement state, String relationshipTypeName );

    String relationshipTypeGetName( Statement state, int relationshipTypeId )
            throws RelationshipTypeIdNotFoundKernelException;

}
