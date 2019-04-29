/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;

import static org.neo4j.io.fs.FileUtils.path;

enum OldIndexProvider
{
    LUCENE( "lucene", "1.0", GraphDatabaseSettings.SchemaIndex.NATIVE30 )
            {
                @Override
                File providerRootDirectory( DatabaseLayout layout )
                {
                    return directoryRootByProviderKey( layout.databaseDirectory(), providerKey );
                }
            },
    NATIVE10( "lucene+native", "1.0", GraphDatabaseSettings.SchemaIndex.NATIVE30 )
            {
                @Override
                File providerRootDirectory( DatabaseLayout layout )
                {
                    return directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                }
            },
    NATIVE20( "lucene+native", "2.0", GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 )
            {
                @Override
                File providerRootDirectory( DatabaseLayout layout )
                {
                    return directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                }
            };

    final String providerKey;
    final String providerVersion;
    final GraphDatabaseSettings.SchemaIndex desiredAlternativeProvider;

    OldIndexProvider( String providerKey, String providerVersion, GraphDatabaseSettings.SchemaIndex desiredAlternativeProvider )
    {
        this.providerKey = providerKey;
        this.providerVersion = providerVersion;
        this.desiredAlternativeProvider = desiredAlternativeProvider;
    }

    abstract File providerRootDirectory( DatabaseLayout layout );

    /**
     * Returns the base schema index directory, i.e.
     *
     * <pre>
     * &lt;db&gt;/schema/index/
     * </pre>
     *
     * @param databaseStoreDir database store directory, i.e. {@code db} in the example above, where e.g. {@code nodestore} lives.
     * @return the base directory of schema indexing.
     */
    private static File baseSchemaIndexFolder( File databaseStoreDir )
    {
        return path( databaseStoreDir, "schema", "index" );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static File directoryRootByProviderKey( File databaseStoreDir, String providerKey )
    {
        return path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( providerKey ) );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static File directoryRootByProviderKeyAndVersion( File databaseStoreDir, String providerKey, String providerVersion )
    {
        return path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( providerKey + "-" + providerVersion ) );
    }

    private static String fileNameFriendly( String name )
    {
        return name.replaceAll( "\\+", "_" );
    }
}
