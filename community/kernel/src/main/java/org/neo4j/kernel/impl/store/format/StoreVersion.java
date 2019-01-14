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
package org.neo4j.kernel.impl.store.format;

/**
 * All known store formats are collected here.
 */
public enum StoreVersion
{
    STANDARD_V2_3( "v0.A.6", "2.3.0" ),
    STANDARD_V3_0( "v0.A.7", "3.0.0" ),
    STANDARD_V3_2( "v0.A.8", "3.2.0" ),
    STANDARD_V3_4( "v0.A.9", "3.4.0" ),

    HIGH_LIMIT_V3_0_0( "vE.H.0", "3.0.0" ),
    HIGH_LIMIT_V3_0_6( "vE.H.0b", "3.0.6" ),
    HIGH_LIMIT_V3_1_0( "vE.H.2", "3.1.0" ),
    HIGH_LIMIT_V3_2_0( "vE.H.3", "3.2.0" ),
    HIGH_LIMIT_V3_4_0( "vE.H.4", "3.4.0" );

    private final String versionString;
    private final String introductionVersion;

    StoreVersion( String versionString, String introductionVersion )
    {
        this.versionString = versionString;
        this.introductionVersion = introductionVersion;
    }

    public String versionString()
    {
        return versionString;
    }

    public String introductionVersion()
    {
        return introductionVersion;
    }
}
