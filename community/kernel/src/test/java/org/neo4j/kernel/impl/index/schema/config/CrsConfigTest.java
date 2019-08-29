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
package org.neo4j.kernel.impl.index.schema.config;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
class CrsConfigTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testCrsConfigMigration() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "unsupported.dbms.db.spatial.crs.cartesian-3d.min.x=1",
                "unsupported.dbms.db.spatial.crs.cartesian-3d.min.y=2",
                "unsupported.dbms.db.spatial.crs.cartesian-3d.min.z=3",
                "unsupported.dbms.db.spatial.crs.cartesian.min.x=3",
                "unsupported.dbms.db.spatial.crs.cartesian.max.x=4"
        ) );

        Config config = Config.newBuilder()
                .fromFile( confFile )
                .build();

        final double DELTA = 0.00000001;

        List<Double> cartesian3Dmin = config.get( CrsConfig.group( CoordinateReferenceSystem.Cartesian_3D ).min );
        assertArrayEquals( new double[] { 1.0, 2.0, 3.0 }, ArrayUtils.toPrimitive( cartesian3Dmin.toArray( new Double[0] ) ), DELTA );
        assertEquals( 3.0, config.get( CrsConfig.group( CoordinateReferenceSystem.Cartesian ).min ).get( 0 ), DELTA );
        assertEquals( 4.0, config.get( CrsConfig.group( CoordinateReferenceSystem.Cartesian ).max ).get( 0 ), DELTA );
    }

}
