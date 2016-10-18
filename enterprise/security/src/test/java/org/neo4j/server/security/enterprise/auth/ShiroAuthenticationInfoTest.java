/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Test;

import org.neo4j.kernel.api.security.AuthenticationResult;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;

public class ShiroAuthenticationInfoTest
{
    private ShiroAuthenticationInfo successInfo = new ShiroAuthenticationInfo( "user", "realm", SUCCESS );
    private ShiroAuthenticationInfo failureInfo = new ShiroAuthenticationInfo( "user", "realm", FAILURE );
    private ShiroAuthenticationInfo tooManyAttemptsInfo = new ShiroAuthenticationInfo( "user", "realm", TOO_MANY_ATTEMPTS );
    private ShiroAuthenticationInfo pwChangeRequiredInfo = new ShiroAuthenticationInfo( "user", "realm", PASSWORD_CHANGE_REQUIRED );

    // These tests are here to remind you that you need to update the ShiroAuthenticationInfo.mergeMatrix[][]
    // whenever you add/remove/move values in the AuthenticationResult enum

    @Test
    public void shouldChangeMergeMatrixIfAuthenticationResultEnumChanges()
    {
        // These are the assumptions made for ShiroAuthenticationInfo.mergeMatrix[][]
        // which have to stay in sync with the enum
        assertEquals( AuthenticationResult.SUCCESS.ordinal(), 0 );
        assertEquals( AuthenticationResult.FAILURE.ordinal(), 1 );
        assertEquals( AuthenticationResult.TOO_MANY_ATTEMPTS.ordinal(), 2 );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED.ordinal(), 3 );
        assertEquals( AuthenticationResult.values().length, 4 );
    }

    @Test
    public void shouldMergeTwoSuccessToSameValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", SUCCESS );
        info.merge( successInfo );

        assertEquals( info.getAuthenticationResult(), SUCCESS );
    }

    @Test
    public void shouldMergeTwoFailureToSameValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", FAILURE );
        info.merge( failureInfo );

        assertEquals( info.getAuthenticationResult(), FAILURE );
    }

    @Test
    public void shouldMergeTwoTooManyAttemptsToSameValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", TOO_MANY_ATTEMPTS );
        info.merge( tooManyAttemptsInfo );

        assertEquals( info.getAuthenticationResult(), TOO_MANY_ATTEMPTS );
    }

    @Test
    public void shouldMergeTwoPasswordChangeRequiredToSameValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", PASSWORD_CHANGE_REQUIRED );
        info.merge( pwChangeRequiredInfo );

        assertEquals( info.getAuthenticationResult(), PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    public void shouldMergeFailureWithSuccessToNewValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", FAILURE );
        info.merge( successInfo );

        assertEquals( info.getAuthenticationResult(), SUCCESS );
    }

    @Test
    public void shouldMergeFailureWithTooManyAttemptsToNewValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", FAILURE );
        info.merge( tooManyAttemptsInfo );

        assertEquals( info.getAuthenticationResult(), TOO_MANY_ATTEMPTS );
    }

    @Test
    public void shouldMergeFailureWithPasswordChangeRequiredToNewValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", FAILURE );
        info.merge( pwChangeRequiredInfo );

        assertEquals( info.getAuthenticationResult(), PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    public void shouldMergeToNewValue()
    {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo( "user", "realm", FAILURE );
        info.merge( pwChangeRequiredInfo );

        assertEquals( info.getAuthenticationResult(), PASSWORD_CHANGE_REQUIRED );
    }
}
