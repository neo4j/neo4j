/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Implementation of the property store.
 */
public class PropertyIndexStore extends AbstractNameStore<PropertyIndexRecord>
{
    public static abstract class Configuration
        extends AbstractNameStore.Configuration
    {

    }

    public static final String TYPE_DESCRIPTOR = "PropertyIndexStore";
    private static final int RECORD_SIZE = 1/*inUse*/ + 4/*prop count*/ + 4/*nameId*/;

    public PropertyIndexStore(String fileName, Config config,
                              IdGeneratorFactory idGeneratorFactory, FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, DynamicStringStore nameStore)
    {
        super(fileName, config, IdType.PROPERTY_INDEX, idGeneratorFactory, fileSystemAbstraction, stringLogger, nameStore);
    }

    @Override
    public void accept( RecordStore.Processor processor, PropertyIndexRecord record )
    {
        processor.processPropertyIndex(this, record);
    }

    @Override
    protected PropertyIndexRecord newRecord( int id )
    {
        return new PropertyIndexRecord( id );
    }
    
    @Override
    protected void readRecord( PropertyIndexRecord record, Buffer buffer )
    {
        record.setPropertyCount( buffer.getInt() );
        record.setNameId( buffer.getInt() );
    }
    
    @Override
    protected void writeRecord( PropertyIndexRecord record, Buffer buffer )
    {
        buffer.putInt( record.getPropertyCount() );
        buffer.putInt( record.getNameId() );
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }
}