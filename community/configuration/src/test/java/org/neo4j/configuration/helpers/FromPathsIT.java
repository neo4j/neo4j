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
package org.neo4j.configuration.helpers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestDirectoryExtension
class FromPathsIT
{
    @Inject
    private TestDirectory testDirectory;

    private Path neo4j1Directory;
    private Path mongo1Directory;
    private Path redis1Directory;
    private Path dbRoot1Directory;
    private Path neo4j2Directory;
    private Path dbRoot2Directory;

    @BeforeEach
    void setUp()
    {
        this.neo4j1Directory = testDirectory.directory( "neo4j", "db1" ).toPath();
        this.mongo1Directory = testDirectory.directory( "mongo", "db1" ).toPath();
        this.redis1Directory = testDirectory.directory( "redis", "db1" ).toPath();
        this.neo4j2Directory = testDirectory.directory( "neo4j", "db2" ).toPath();
        this.dbRoot1Directory = neo4j1Directory.getParent();
        this.dbRoot2Directory = neo4j2Directory.getParent();
    }

    @Test
    void shouldReturnTheInputValueIfFilterIsNotApplied()
    {
        final var filteredPaths = new FromPaths( dbRoot1Directory.toAbsolutePath().toString() ).paths( Optional.empty() );

        final var expected = Set.of( dbRoot1Directory );
        assertThat( filteredPaths ).containsAll( expected );
    }

    @Test
    void shouldGetAllFoldersThatMatchIfFilterIsApplied()
    {
        final var fromPaths = new FromPaths( dbRoot1Directory.toAbsolutePath().toString() );

        assertThat( fromPaths.paths( Optional.of( new DatabaseNamePattern( "n*" ) ) ) ).containsAll( Set.of( neo4j1Directory ) );
        assertThat( fromPaths.paths( Optional.of( new DatabaseNamePattern( "neo4?" ) ) ) ).containsAll( Set.of( neo4j1Directory ) );
        assertThat( fromPaths.paths( Optional.of( new DatabaseNamePattern( "neo4j" ) ) ) ).containsAll( Set.of( neo4j1Directory ) );
        assertThat( fromPaths.paths( Optional.of( new DatabaseNamePattern( "*" ) ) ) )
                .containsAll( Set.of( neo4j1Directory, mongo1Directory, redis1Directory ) );
    }

    @Test
    void shouldBeSingleValueInCaseThereIsNoCommaInTheInput()
    {
        assertThat( new FromPaths( "test" ).isSingle() ).isTrue();
    }

    @Test
    void shouldBeNotSingleValueInCaseThereIsACommaInTheInput()
    {
        assertThat( new FromPaths( "test, test2" ).isSingle() ).isFalse();
    }

    @Test
    void shouldReturnTheInputListIfFilterIsNotApplied()
    {
        final var filteredPaths = new FromPaths( dbRoot1Directory.toAbsolutePath().toString() + ", " + dbRoot2Directory.toAbsolutePath().toString() )
                .paths( Optional.empty() );

        final var expected = Set.of( dbRoot1Directory, dbRoot2Directory );
        assertThat( filteredPaths ).containsAll( expected );
    }

    @Test
    void shouldGetAllFoldersFromTheListOfPathsIfFilterIsApplied()
    {
        final var filteredPaths = new FromPaths( dbRoot1Directory.toAbsolutePath().toString() + ", " + dbRoot2Directory.toAbsolutePath().toString() )
                .paths( Optional.of( new DatabaseNamePattern( "n*" ) ) );

        final var expected = Set.of( neo4j1Directory, neo4j2Directory );
        assertThat( filteredPaths ).containsAll( expected );
    }

    @Test
    void shouldThrowExceptionIfFromPathIsEmpty()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () -> assertValid( "" ) );
        assertEquals( "The provided from parameter is empty.", e.getMessage() );

        Exception e2 = assertThrows( NullPointerException.class, () -> assertValid( null ) );
        assertEquals( "The provided from parameter is empty.", e2.getMessage() );
    }

    private void assertValid( String name )
    {
        new FromPaths( name );
    }

}
