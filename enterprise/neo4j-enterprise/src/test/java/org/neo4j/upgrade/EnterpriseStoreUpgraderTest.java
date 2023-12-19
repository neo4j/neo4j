/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.upgrade;

import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.storemigration.StoreUpgraderTest;
import org.neo4j.test.Unzip;

import static java.util.Collections.singletonList;

public class EnterpriseStoreUpgraderTest extends StoreUpgraderTest
{
    public EnterpriseStoreUpgraderTest( RecordFormats recordFormats )
    {
        super( recordFormats );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<RecordFormats> versions()
    {
        return singletonList( HighLimitV3_0_0.RECORD_FORMATS );
    }

    @Override
    protected RecordFormats getRecordFormats()
    {
        return HighLimit.RECORD_FORMATS;
    }

    @Override
    protected String getRecordFormatsName()
    {
        return HighLimit.NAME;
    }

    @Override
    protected void prepareSampleDatabase( String version, FileSystemAbstraction fileSystem, File dbDirectory,
            File databaseDirectory ) throws IOException
    {
        File resourceDirectory = findFormatStoreDirectoryForVersion( version, databaseDirectory );
        fileSystem.deleteRecursively( dbDirectory );
        fileSystem.mkdirs( dbDirectory );
        fileSystem.copyRecursively( resourceDirectory, dbDirectory );
    }

    private File findFormatStoreDirectoryForVersion( String version, File databaseDirectory ) throws IOException
    {
        if ( version.equals( HighLimitV3_0_0.STORE_VERSION ) )
        {
            return highLimit3_0Store( databaseDirectory );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown enterprise store version." );
        }
    }

    private File highLimit3_0Store( File databaseDirectory ) throws IOException
    {
        return Unzip.unzip( EnterpriseStoreUpgraderTest.class, "upgradeTest30HighLimitDb.zip", databaseDirectory );
    }
}
