/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.helpers;

public interface Clock
{
    final Clock SYSTEM = new Clock()
    {
        @Override
        public long currentTimeMillis()
        {
            return System.currentTimeMillis();
        }

        @Override
        public long nanoTime()
        {
            return System.nanoTime();
        }
    };

    long currentTimeMillis();

    long nanoTime();
}
