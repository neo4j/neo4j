/*
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;

import static org.neo4j.kernel.impl.store.CommonAbstractStore.buildTypeDescriptorAndVersion;

public class CurrentDatabase
{
    private final StoreVersionCheck storeVersionCheck;
    private static final Map<String, String> fileNamesToTypeDescriptors = new HashMap<String, String>();

    static
    {
        fileNamesToTypeDescriptors.put( NeoStore.DEFAULT_NAME, NeoStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.nodestore.db", NodeStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db", PropertyStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.arrays", DynamicArrayStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.index", PropertyKeyTokenStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.index.keys", DynamicStringStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.strings", DynamicStringStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.relationshipstore.db", RelationshipStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.relationshiptypestore.db", RelationshipTypeTokenStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( "neostore.relationshiptypestore.db.names", DynamicStringStore.TYPE_DESCRIPTOR );
    }

    public CurrentDatabase(StoreVersionCheck storeVersionCheck)
    {
        this.storeVersionCheck = storeVersionCheck;
    }

    public boolean storeFilesAtCurrentVersion( File storeDirectory )
    {
        for ( String fileName : fileNamesToTypeDescriptors.keySet() )
        {
            String expectedVersion = buildTypeDescriptorAndVersion( fileNamesToTypeDescriptors.get( fileName ) );

            if ( !storeVersionCheck.hasVersion(
                    new File( storeDirectory, fileName ), expectedVersion ).outcome.isSuccessful() )
            {
                return false;
            }
        }
        return true;
    }

    public static Collection<String> fileNames()
    {
        return fileNamesToTypeDescriptors.keySet();
    }
}
