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
package org.neo4j.graphdb.factory.module.edition;

import org.junit.jupiter.api.Test;

import org.neo4j.logging.Log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class EditionModuleTest
{
    @Test
    void shouldFailWhenAuthEnabledAndNoSecurityModuleFound()
    {
        IllegalArgumentException argumentException = assertThrows( IllegalArgumentException.class,
                () -> AbstractEditionModule.setupSecurityModule( null, null, mock( Log.class ), null, "non-existent-security-module" ) );
        assertEquals( "Failed to load security module with key 'non-existent-security-module'.", argumentException.getMessage() );
    }
}
