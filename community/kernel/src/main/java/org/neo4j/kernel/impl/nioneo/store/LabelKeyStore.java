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
 * Implementation of the property store.
 */
public class LabelKeyStore extends AbstractNameStore<LabelKeyRecord>
{
    public static abstract class Configuration
        extends AbstractNameStore.Configuration
    {

    }

    public static final String TYPE_DESCRIPTOR = "LabelKeyStore";
    private static final int RECORD_SIZE = 1/*inUse*/ + 4/*nameId*/;

    public LabelKeyStore( File fileName, Config config,
                          IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                          FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger,
                          DynamicStringStore nameStore )
    {
        super(fileName, config, IdType.LABEL_NAME, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger, nameStore);
    }

    @Override
    public void accept( Processor processor, LabelKeyRecord record )
    {
        processor.processLabelName( this, record );
    }

    @Override
    protected LabelKeyRecord newRecord( int id )
    {
        return new LabelKeyRecord( id );
    }
    
    @Override
    protected void readRecord( LabelKeyRecord record, Buffer buffer )
    {
        record.setNameId( buffer.getInt() );
    }
    
    @Override
    protected void writeRecord( LabelKeyRecord record, Buffer buffer )
    {
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