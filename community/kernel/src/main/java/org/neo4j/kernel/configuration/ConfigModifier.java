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
package org.neo4j.kernel.configuration;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.factory.GraphDatabaseSetting;

public interface ConfigModifier {
    
    // Modifier helpers
    
    /**
     * A builder api for performing multiple simple
     * modifications in one go.
     */
    public static class Modifications implements ConfigModifier
    {
        private Set<Modification> modifications = new HashSet<Modification>();
        
        @Override
        public void applyTo(Session session)
        {
            for(Modification m : modifications)
            {
                session.set(m.getSetting(), m.getValue());
            }
        }
        
        public <T> Modifications set(GraphDatabaseSetting<T> setting, String value)
        {
            modifications.add(new Modification(setting, value));
            return this;
        }
    }
    
    // Implementation of modification session
    
    public static class Modification
    {
        private String value;
        private GraphDatabaseSetting<?> setting;

        public Modification(GraphDatabaseSetting<?> setting, String value)
        {
            this.setting = setting;
            this.value = value;
        }
        
        public String getValue()
        {
            return value;
        }
        
        public GraphDatabaseSetting<?> getSetting() 
        {
            return setting;
        }
        
    }
    
    /**
     * An API for constructing a set of modifications to 
     * be applied to the config instance.
     * 
     * This allows grouping configuration changes together.
     */
    public static class Session
    {
        private Config config;
        private Set<Modification> modifications = new HashSet<Modification>();

        public Session(Config config) 
        {
            this.config = config;
        }
        
        /**
         * Get a value from the original configuration.
         * @param setting
         * @return
         */
        public <T> T get(GraphDatabaseSetting<T> setting) 
        {
            return config.get(setting);
        }
        
        /**
         * List keys from the original configuration.
         * @return
         */
        public Collection<String> getKeys() 
        {
            return config.getKeys();
        }
        
        public <T> void set(GraphDatabaseSetting<T> setting, String value)
        {
            modifications.add(new Modification(setting, value));
        }

        protected Set<Modification> getModifications()
        {
            return modifications;
        }
    }
    
    public void applyTo(Session session);
    
}