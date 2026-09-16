/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.ssl.config;

import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.ssl.SslPolicy;

public interface SslPolicyProvider {
    boolean hasPolicyForScope(SslPolicyScope scope);

    SslPolicy getPolicy(SslPolicyScope scope);

    void addPolicyChangeListener(SslPolicyChangeListener listener);

    void removePolicyChangeListener(SslPolicyChangeListener listener);

    /**
     * Reloads all SSL policies, e.g. in response to an explicit request such as the
     * {@code dbms.security.reloadTLS()} procedure. Implementations are expected to gate this on whichever
     * setting controls manual/procedure-triggered reload, independently of
     * {@link #reloadPoliciesOnFileChange()}.
     */
    void reloadPolicies();

    /**
     * Reloads all SSL policies in response to a detected change to the underlying certificate files on disk.
     * Implementations are expected to gate this on whichever setting controls automatic, file-change-triggered
     * reload, independently of {@link #reloadPolicies()} - enabling one is not meant to implicitly enable the
     * other.
     */
    void reloadPoliciesOnFileChange();
}
