/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Test;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;

import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;

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
