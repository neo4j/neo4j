/**
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.impl.nioneo.store.SchemaRule.Kind.deserialize;

public class SchemaStore extends AbstractDynamicStore implements Iterable<SchemaRule>
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "SchemaStore";
    public static final String VERSION = buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );
    public static final int BLOCK_SIZE = 56; // + BLOCK_HEADER_SIZE == 64
    
    @SuppressWarnings("deprecation")
    public SchemaStore( File fileName, Config conf, IdType idType, IdGeneratorFactory idGeneratorFactory,
            WindowPoolFactory windowPoolFactory, FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger )
    {
        super( fileName, conf, idType, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, stringLogger );
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, DynamicRecord record ) throws FAILURE
    {
        processor.processSchema( this, record );
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }
    
    public Collection<DynamicRecord> allocateFrom( SchemaRule rule )
    {
        RecordSerializer serializer = new RecordSerializer();
        serializer = serializer.append( rule );
        return allocateRecordsFromBytes( serializer.serialize(), asList( forceGetRecord( rule.getId() ) ).iterator(),
                recordAllocator );
    }
    
    public Iterator<SchemaRule> loadAllSchemaRules()
    {
        return new SchemaStorage( this ).loadAllSchemaRules();
    }

    @Override
    public Iterator<SchemaRule> iterator()
    {
        return loadAllSchemaRules();
    }

    public static SchemaRule readSchemaRule( long id, Collection<DynamicRecord> records )
            throws MalformedSchemaRuleException
    {
        return readSchemaRule( id, records, new byte[ BLOCK_SIZE * 4 ] );
    }

    static SchemaRule readSchemaRule( long id, Collection<DynamicRecord> records, byte[] buffer )
            throws MalformedSchemaRuleException
    {
        ByteBuffer scratchBuffer = concatData( records, buffer );
        return deserialize( id, scratchBuffer );
    }
}
