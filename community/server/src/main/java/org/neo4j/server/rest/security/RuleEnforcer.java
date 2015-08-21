/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.rest.security;

import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;

import javax.servlet.http.HttpServletRequest;

/**
 * A RuleEnforcer is a facade that implements a superset of all SecurityRule variants, used
 * to apply whatever variants a particular rule has implemented.
 *
 */
public class RuleEnforcer implements SecurityRule, ForbiddenSecurityRule {

    private class ForbiddenAccessPredicate implements Predicate<HttpServletRequest> {
        private final ForbiddenSecurityRule forbiddingRule;

        public ForbiddenAccessPredicate(ForbiddenSecurityRule securityRule) {
            this.forbiddingRule = securityRule;
        }

        @Override
        public boolean test(HttpServletRequest httpServletRequest) {
            return forbiddingRule.isForbidden(httpServletRequest);
        }
    }

    /**
     * The underyling SecurityRule provided by a user.
     */
    private final SecurityRule userRule;

    private final Predicate forbiddingPredicate;

    public RuleEnforcer(SecurityRule userRule) {
        this.userRule = userRule;
        if (userRule instanceof ForbiddenSecurityRule) {
            forbiddingPredicate = new ForbiddenAccessPredicate((ForbiddenSecurityRule)userRule);
        } else {
            forbiddingPredicate = Predicates.alwaysFalse();
        }
    }

    public boolean isAuthorized(HttpServletRequest httpReq) {
        return userRule.isAuthorized((httpReq));
    }

    @Override
    public String forUriPath() {
        return userRule.forUriPath();
    }

    public String wwwAuthenticateHeader() {
        return userRule.wwwAuthenticateHeader();
    }

    public boolean isForbidden(HttpServletRequest httpReq) {
        return forbiddingPredicate.test(httpReq);
    }

}
