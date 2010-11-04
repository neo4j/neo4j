
/**
 * JMX exploration page module for the monitor component.
 */
wa.components.jmx.Jmx = (function($) {
    
    var me = {};
    
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
            findBeans : function(name, cb) {

                var beanInfo = me.api.parseBeanName(name);
                
                // The requested bean is not available locally, check if there
                // is a server connection
                
                var server = wa.Servers.getCurrentServer(); 
                
                if( ! server ) { // Nope
                    cb([]);
                    return;
                }
                    
                
                // Server available
                server.manage.jmx.getBean(beanInfo.domain, beanInfo.name, (function(cb) {
                    return function(data) {
                        cb(data);
                    };
                })(cb));
                
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
                    
                for( var index in data ) {

                    var bean = data[index];
                    var domainName = me.api.parseBeanName(bean.name).domain;
                    var domain = me.getDomain(domainName);
                    
                    domain.beans.push(bean);
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
            me.api.findBeans(beanName, function(beans) { 
                if(beans.length > 0) {
                    me.currentBean = beans[0];
                    
                    me.render();
                }
                
            });
            
        }
    };
    
    me.render = function() {
        if(me.visible) {
            me.basePage.processTemplate({
                jmx : (me.jmxData === null) ? [] : me.jmxData,
                server : me.server,
                bean : me.currentBean
            });
        }
        
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
wa.ui.MainMenu.add({ label : "JMX", pageKey:"jmx", index:7, requiredServices:['jmx'], perspectives:['server']});

wa.bind("init", wa.components.jmx.Jmx.init);
wa.bind("ui.page.changed", wa.components.jmx.Jmx.pageChanged);