/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

/**
 * Dynamic store that stores strings.
 */
public class DynamicStringStore extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    static final String VERSION = "StringPropertyStore v0.A.0";
    public static final String TYPE_DESCRIPTOR = "StringPropertyStore";

    public DynamicStringStore( String fileName, Map<?,?> config, IdType idType )
    {
        super( fileName, config, idType );
    }
    
    @Override
    public void accept( RecordStore.Processor processor, DynamicRecord record )
    {
        processor.processString( this, record );
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    public static void createStore( String fileName, int blockSize,
            IdGeneratorFactory idGeneratorFactory, IdType idType )
    {
        createEmptyStore( fileName, blockSize, VERSION, idGeneratorFactory, idType );
    }

    @Override
    public void setHighId( long highId )
    {
        super.setHighId( highId );
    }

    @Override
    public long nextBlockId()
    {
        return super.nextBlockId();
    }

}
