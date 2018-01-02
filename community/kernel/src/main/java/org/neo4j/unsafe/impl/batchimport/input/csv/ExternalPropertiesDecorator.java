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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.function.Function;
import org.neo4j.function.Functions;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.UpdateBehaviour;

/**
 * Pulls in properties from an external CSV source and amends them to the "main" input nodes.
 * Imagine some node input source:
 * <pre>
 * :ID,name
 * 1,First
 * 2,Second
 * 3,Third
 * 4,Fourth
 * </pre>
 * and an external properties source:
 * <pre>
 * :ID,email
 * 1.abc@somewhere
 * 1,def@somewhere
 * 3,ghi@someplace
 * <Pre>
 * Then properties {@code abc@somewhere} and {@code def@somewhere} will be amended to input node {@code 1}
 * and {@code ghi@someplace} to input node {@code 3}.
 */
public class ExternalPropertiesDecorator implements Function<InputNode,InputNode>
{
    private final InputEntityDeserializer<InputNode> deserializer;
    private InputNode currentExternal;
    private final UpdateBehaviour updateBehaviour;

    /**
     * @param headerFactory creates a {@link Header} that will specify which field is the {@link Type#ID id field}
     * and which properties to extract. All other should be {@link Type#IGNORE ignored}. I think.
     */
    public ExternalPropertiesDecorator( DataFactory<InputNode> data, Header.Factory headerFactory,
            Configuration config, IdType idType, UpdateBehaviour updateBehaviour, Collector badCollector )
    {
        this.updateBehaviour = updateBehaviour;
        CharSeeker dataStream = data.create( config ).stream();
        Header header = headerFactory.create( dataStream, config, idType );
        this.deserializer = new InputEntityDeserializer<>( header, dataStream, config.delimiter(),
                new InputNodeDeserialization( dataStream, header, new Groups(), idType.idsAreExternal() ),
                Functions.<InputNode>identity(), Validators.<InputNode>emptyValidator(), badCollector );
    }

    @Override
    public InputNode apply( InputNode from ) throws RuntimeException
    {
        // Nodes come in here. Correlate by id to the external properties data
        Object id = from.id();
        if ( currentExternal != null )
        {
            if ( id.equals( currentExternal.id() ) )
            {
                decorate( from );
                currentExternal = null;
            }
            else
            {
                return from;
            }
        }

        while ( deserializer.hasNext() )
        {
            currentExternal = deserializer.next();
            if ( id.equals( currentExternal.id() ) )
            {
                // decorate as well. I.e. there were multiple rows for this node id
                decorate( from );
            }
            else
            {
                break;
            }
        }
        return from;
    }

    private void decorate( InputNode from )
    {
        from.updateProperties( updateBehaviour, currentExternal.properties() );
    }
}
