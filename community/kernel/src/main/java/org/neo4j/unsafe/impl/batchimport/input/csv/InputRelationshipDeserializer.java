/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.function.Function;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

/**
 * {@link InputEntityDeserializer} that knows the semantics of an {@link InputRelationship} and how to extract that from
 * csv values using a {@link Header}.
 */
class InputRelationshipDeserializer extends InputEntityDeserializer<InputRelationship>
{
    private final IdSequence idSequence;

    // Additional data
    private long id;
    private String type;
    private Object startNode;
    private Object endNode;

    InputRelationshipDeserializer( Header header, CharSeeker data, int[] delimiter, IdSequence idSequence,
            Function<InputRelationship,InputRelationship> decorator )
    {
        super( header, data, delimiter, decorator );
        this.idSequence = idSequence;
    }

    @Override
    protected void handleValue( Header.Entry entry, Object value )
    {
        switch ( entry.type() )
        {
        case TYPE:
            type = (String) value;
            break;
        case START_ID:
            startNode = value;
            break;
        case END_ID:
            endNode = value;
            break;
        }
    }

    @Override
    protected InputRelationship convertToInputEntity( Object[] properties )
    {
        return new InputRelationship( idSequence.nextId(), properties, null, startNode, endNode, type, null );
    }
}
