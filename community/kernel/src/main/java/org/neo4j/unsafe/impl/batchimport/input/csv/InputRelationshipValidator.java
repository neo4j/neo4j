/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.MissingRelationshipDataException;

class InputRelationshipValidator implements Validator<InputRelationship>
{
    @Override
    public void validate( InputRelationship entity )
    {
        if ( entity.startNode() == null )
        {
            throw new MissingRelationshipDataException(Type.START_ID,
                                entity + " is missing " + Type.START_ID + " field" );
        }
        if ( entity.endNode() == null )
        {
            throw new MissingRelationshipDataException(Type.END_ID,
                                entity + " is missing " + Type.END_ID + " field" );
        }
        if ( !entity.hasTypeId() && entity.type() == null )
        {
            throw new MissingRelationshipDataException(Type.TYPE,
                                entity + " is missing " + Type.TYPE + " field" );
        }
    }
}
