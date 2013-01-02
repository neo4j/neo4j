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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Dynamic store that stores strings.
 */
public class DynamicStringStore extends AbstractDynamicStore
{
    public static abstract class Configuration
        extends AbstractDynamicStore.Configuration
    {

    }

    // store version, each store ends with this string (byte encoded)
    public static final String VERSION = "StringPropertyStore v0.A.0";
    public static final String TYPE_DESCRIPTOR = "StringPropertyStore";

    public DynamicStringStore( File fileName, Config configuration, IdType idType,
                               IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                               FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger)
    {
        super( fileName, configuration, idType, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }
    
    @Override
    public void accept( RecordStore.Processor processor, DynamicRecord record )
    {
        processor.processString( this, record, idType );
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
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
