/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import javax.annotation.Nonnull;

/**
 * All known store formats are collected here.
 */
public class StoreVersions
{
    // Community formats
    public static final String STANDARD_V2_0 = "v0.A.1";
    public static final String STANDARD_V2_1 = "v0.A.3";
    public static final String STANDARD_V2_2 = "v0.A.5";
    public static final String STANDARD_V2_3 = "v0.A.6";
    public static final String STANDARD_V3_0 = "v0.A.7";

    // Enterprise formats
    public static final String HIGH_LIMIT_V3_0 = "vE.H.0";

    /**
     *
     * @param storeVersion string
     * @return true if the store version is a known community format, false otherwise
     */
    public static boolean isCommunityStoreVersion( @Nonnull String storeVersion )
    {
        return STANDARD_V2_0.equals( storeVersion ) ||
                STANDARD_V2_1.equals( storeVersion ) ||
                STANDARD_V2_2.equals( storeVersion ) ||
                STANDARD_V2_3.equals( storeVersion ) ||
                STANDARD_V3_0.equals( storeVersion );
    }

    /**
     *
     * @param storeVersion string
     * @return true if the store version is a known enterprise format, false otherwise
     */
    public static boolean isEnterpriseStoreVersion( @Nonnull String storeVersion )
    {
        return HIGH_LIMIT_V3_0.equals( storeVersion );
    }
}
