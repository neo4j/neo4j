/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.index.label;

import org.neo4j.test.Race;
import org.neo4j.util.FeatureToggles;

public class RelationshipTypeScanStoreUtil
{
    public static void withRTSS( boolean enableRTSS, Race.ThrowingRunnable runnable ) throws Throwable
    {
        if ( enableRTSS )
        {
            enableRelationshipTypeStoreScan();
        }
        else
        {
            disableRelationshipTypeStoreScan();
        }
        try
        {
            runnable.run();
        }
        finally
        {
            clearRelationshipTypeStoreScan();
        }
    }

    public static void withRTSS( Race.ThrowingRunnable runnable ) throws Throwable
    {
        enableRelationshipTypeStoreScan();
        try
        {
            runnable.run();
        }
        finally
        {
            clearRelationshipTypeStoreScan();
        }
    }

    public static void withoutRTSS( Race.ThrowingRunnable runnable ) throws Throwable
    {
        disableRelationshipTypeStoreScan();
        try
        {
            runnable.run();
        }
        finally
        {
            clearRelationshipTypeStoreScan();
        }
    }

    public static void enableRelationshipTypeStoreScan()
    {
        FeatureToggles.set( TokenScanStore.class, TokenScanStore.RELATIONSHIP_TYPE_SCAN_STORE_ENABLE_STRING, true );
    }

    public static void clearRelationshipTypeStoreScan()
    {
        FeatureToggles.clear( TokenScanStore.class, TokenScanStore.RELATIONSHIP_TYPE_SCAN_STORE_ENABLE_STRING );
    }

    private static void disableRelationshipTypeStoreScan()
    {
        FeatureToggles.set( TokenScanStore.class, TokenScanStore.RELATIONSHIP_TYPE_SCAN_STORE_ENABLE_STRING, false );
    }
}
