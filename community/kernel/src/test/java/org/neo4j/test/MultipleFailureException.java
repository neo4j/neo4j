/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Snipped from jUnits MultipleFailureException
 */
public class MultipleFailureException extends Exception {
    private static final long serialVersionUID= 1L;
    
    private final List<Throwable> fErrors;

    public MultipleFailureException(List<Throwable> errors) {
        fErrors= new ArrayList<Throwable>(errors);
    }

    public List<Throwable> getFailures() {
        return Collections.unmodifiableList(fErrors);
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder(
                String.format("There were %d errors:", fErrors.size()));
        for (Throwable e : fErrors) {
            sb.append(String.format("\n  %s(%s)", e.getClass().getName(), e.getMessage()));
        }
        return sb.toString();
    }

}
