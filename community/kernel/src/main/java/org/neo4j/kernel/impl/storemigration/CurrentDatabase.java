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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.buildTypeDescriptorAndVersion;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;

public class CurrentDatabase
{
    private Map<String, String> fileNamesToTypeDescriptors = new HashMap<String, String>();

    public CurrentDatabase()
    {
        fileNamesToTypeDescriptors.put( NeoStore.DEFAULT_NAME, NeoStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.nodestore.db", NodeStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db", PropertyStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.arrays", DynamicArrayStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.index", PropertyIndexStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.index.keys", DynamicStringStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.strings", DynamicStringStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.relationshipstore.db", RelationshipStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.relationshiptypestore.db", RelationshipTypeStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.relationshiptypestore.db.names", DynamicStringStore.TYPE_DESCRIPTOR );
    }

    public boolean storeFilesAtCurrentVersion( File storeDirectory )
    {
        for ( String fileName : fileNamesToTypeDescriptors.keySet() )
        {
            String expectedVersion = buildTypeDescriptorAndVersion( fileNamesToTypeDescriptors.get( fileName ) );
            FileChannel fileChannel = null;
            byte[] expectedVersionBytes = UTF8.encode( expectedVersion );
            try
            {
                File storeFile = new File( storeDirectory, fileName );
                if ( !storeFile.exists() )
                {
                    return false;
                }
                fileChannel = new RandomAccessFile( storeFile, "r" ).getChannel();
                fileChannel.position( fileChannel.size() - expectedVersionBytes.length );
                byte[] foundVersionBytes = new byte[expectedVersionBytes.length];
                fileChannel.read( ByteBuffer.wrap( foundVersionBytes ) );
                if ( !expectedVersion.equals( UTF8.decode( foundVersionBytes ) ) )
                {
                    return false;
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( fileChannel != null )
                {
                    try
                    {
                        fileChannel.close();
                    }
                    catch ( IOException e )
                    {
                        // Ignore exception on close
                    }
                }
            }
        }
        return true;
    }
}
