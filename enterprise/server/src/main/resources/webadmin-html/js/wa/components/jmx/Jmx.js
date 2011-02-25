/*
 * Copyright (c) 2002-2011 "Neo Technology,"
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

/**
 * JMX exploration page module for the monitor component.
 */
wa.components.jmx.Jmx = (function($) {
	
    var me = {},
        NEO4J_DOMAIN = "org.neo4j";
    
    me.basePage = $("<div></div>");
    me.ui = {};
    
    me.uiLoaded  = false;
    
    me.jmxData = null;
    me.currentBean = null;
    
    me.visible = false;
    
    //
    // PUBLIC
    //
    
    me.api = {
            
            getPage :  function() {
                return me.basePage;
            },
            
            /**
             * Get bean data, given a bean name. This takes a callback since it
             * is not for sure that data has been loaded yet.
             * 
             * @param name
             *            is the bean name to find
             * @param cb
             *            is a callback that will be called with an array of
             *            matching beans (usually just one, but could be several
             *            if you use wildcard markers in the name)
             */
            findBean : function(name, cb) {

                var beanInfo = me.api.parseBeanName(name),
                    server = wa.Servers.getCurrentServer(); 
                
                if( ! server ) {
                    cb([]);
                } else {
                    // Server available
                    server.manage.jmx.getBean(beanInfo.domain, beanInfo.name, (function(cb) {
                        return function(data) {
                            cb(data);
                        };
                    })(cb));
                }
                
            },
            
            /**
             * Extract data from a bean name
             */
            parseBeanName : function(beanName) {
                
                var parts = beanName.split(":");
                
                return {domain:parts[0], name:parts[1]};
                
            },
            
            pageChanged : function(ev) {
                
                if(ev.data === "jmx") {
                    me.visible = true;
                    
                    if( me.uiLoaded === false ) {
                        me.basePage.setTemplateURL("templates/components/monitor/jmx.tp");
                    }
                    
                    var server = wa.Servers.getCurrentServer(); 
                    
                    // If jmx data has not been loaded for the current server
                    if( server ) {
                        
                        me.loadJMXDomains(me.server);
                        
                    }
                    
                } else {
                    me.visible = false;
                }
            },
            
            init : function() {
                $( window ).bind( "hashchange", me.hashchange );
                me.hashchange();
            }
            
    };
    
    // 
    // PRIVATE
    //
    
    me.loadJMXDomains = function() {
        
        var server = wa.Servers.getCurrentServer(); 
        
        if( server ) {
            server.manage.jmx.query( ["*:*"], function(data) {
                
                me.jmxData = [];
                    
                for( var i=0, l=data.length; i<l; i++ ) {
                    var bean = data[i];
                    me.getDomain(bean.domain).beans.push(bean);
                }

                me.render();
            });   
        }
    };
    
    me.getDomain = function(domain) {
        for( var index in me.jmxData ) {
            if(me.jmxData[index].name === domain) {
                return me.jmxData[index];
            }
        }
        
        var domainObject = { name: domain, beans:[] };
        me.jmxData.push(domainObject);
        
        return domainObject;
    };
    
    /**
     * Triggered when the URL hash state changes.
     */
    me.hashchange = function(ev) {
        var beanName = $.bbq.getState( "jmxbean" );
        
        if( typeof(beanName) !== "undefined" && (me.currentBean === null || beanName !== me.currentBean.name)) {
            me.api.findBean(beanName, function(bean) { 
                if(bean) {
                    me.currentBean = bean;
                    me.render();
                }
            });
            
        }
    };
    
    me.render = function() {
        if(me.visible) {
        	
        	// Re-order jmx data to show neo4j domain first
        	if(me.jmxData !== null) {
        		
        		jmxData = me.sortJmxData(me.jmxData);
        		
        	} else {
        		jmxData = [];
        	}
        	
            me.basePage.processTemplate({
                jmx : jmxData,
                server : me.server,
                bean : me.currentBean
            });
        }
        
    };
    
    me.sortJmxData = function(jmxData) {
        jmxData = jmxData.sort(function(a,b) {
            if( a.name === NEO4J_DOMAIN ) {
                return -1;
            } else if( b.name === NEO4J_DOMAIN ) {
                return 1;
            }
            return a.name.toLowerCase() > b.name.toLowerCase();
        });
        
        _.each(jmxData, function(domain){
            domain.beans = domain.beans.sort(function(a,b) {
                return a.getName().toLowerCase() > b.getName().toLowerCase();
            });
        });
        
        return jmxData;
    };
    
    //
    // CONSTRUCT
    //
    
    $('.mor_monitor_jmxbean_button').live('click', function(ev) {
        
        setTimeout((function(ev){
            return function() {
                $.bbq.pushState({
                    jmxbean : $('.bean-name',ev.target.parentNode).val()
                });
            };
        })(ev),0);
    
        ev.preventDefault();
    });
    
    wa.bind("servers.current.changed", function() {
        me.jmxData = null;
        
        // If the monitor page is currently visible, load jmx stuff
        if( me.visible === true ) {
            me.loadJMXDomains();
        }
    });
    
    return me.api;
    
})(jQuery);

//
// REGISTER STUFF
//

wa.ui.Pages.add("jmx",wa.components.jmx.Jmx);
wa.ui.MainMenu.add({ label : "Server info", pageKey:"jmx", index:7, requiredServices:['jmx'], perspectives:['server']});

wa.bind("init", wa.components.jmx.Jmx.init);
wa.bind("ui.page.changed", wa.components.jmx.Jmx.pageChanged);