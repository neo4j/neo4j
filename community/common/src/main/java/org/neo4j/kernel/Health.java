/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel;

/**
 * Communicate health status between components within a system and let components report system panic.
 */
public interface Health
{
    /**
     * Asserts that the system is in good health. If that is not the case then the cause of the
     * unhealthy state is wrapped in an exception of the given type, i.e. the panic disguise.
     * <p>
     * To check health without throwing exception, use {@link #isHealthy()}.
     *
     * @param panicDisguise the cause of the unhealthy state wrapped in an exception of this type.
     * @throws EXCEPTION exception type to wrap cause in.
     */
    <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION;

    /**
     * Report panic with cause to the system, meaning system is not longer in a healthy state.
     * If system is already in a non health state cause will be ignored.
     *
     * @param cause Cause of panic. Can not be {@code null}.
     */
    void panic( Throwable cause );

    /**
     * Check that the system is in good health, without throwing any exception if it is not.
     *
     * @return true if system is in good health, otherwise false.
     */
    boolean isHealthy();

    /**
     * Reset system state to healthy. This should override any previous calls to {@link #panic(Throwable)},
     * subsequent calls to {@link #isHealthy()} should return {@code true}, subsequent calls to
     * {@link #assertHealthy(Class)} should not throw.
     */
    void healed();

    /**
     * @return Cause of panic or {@code null} if system is in good health.
     */
    Throwable cause();

    class Adapter implements Health
    {

        @Override
        public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION
        {
            // no-op
        }

        @Override
        public void panic( Throwable cause )
        {
            // no-op
        }

        @Override
        public boolean isHealthy()
        {
            return true;
        }

        @Override
        public void healed()
        {
            // no-op
        }

        @Override
        public Throwable cause()
        {
            return null;
        }
    }
}
