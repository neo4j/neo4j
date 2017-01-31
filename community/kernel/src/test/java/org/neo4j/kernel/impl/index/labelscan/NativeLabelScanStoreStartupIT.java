/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.LabelScanStoreStartupIT;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.LabelIndex;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.label_index;

public class NativeLabelScanStoreStartupIT extends LabelScanStoreStartupIT
{
    @Override
    protected void addSpecificConfig( GraphDatabaseBuilder builder )
    {
        builder.setConfig( label_index, LabelIndex.NATIVE.name() );
    }

    @Override
    protected void corruptLabelScanStoreFiles( File storeDirectory ) throws IOException
    {
        scrambleFile( storeFile( storeDirectory ) );
    }

    @Override
    protected void deleteLabelScanStoreFiles( File storeDirectory ) throws IOException
    {
        assertTrue( storeFile( storeDirectory ).delete() );
    }

    private static File storeFile( File directory )
    {
        return new File( directory, NativeLabelScanStore.FILE_NAME );
    }
}
