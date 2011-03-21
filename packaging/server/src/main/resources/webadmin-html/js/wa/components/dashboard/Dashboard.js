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
 * Base module for the monitor component.
 */
wa.components.dashboard.Dashboard = (function($) {
    
    var me = {};
    
    me.basePage = $("<div></div>");
    me.ui = {};
    
    me.uiLoaded  = false;
    me.server = null;
    me.kernelInfo = null;
    
    me.visible = false;
    
    me.valueTrackers = [];
    me.charts = [];
    me.visibleChart = null;
    
    //
    // PUBLIC
    //
    
    me.api = {
            
            getPage :  function() {
                return me.basePage;
            },
            
            pageChanged : function(ev) {
                if(ev.data === "dashboard") {
            		me.visible = true;
                
                    if( me.uiLoaded === false ) {
                    	me.uiLoaded = true;
                        me.basePage.setTemplateURL("templates/components/monitor/index.tp");
	                    
                        me.server = wa.Servers.getCurrentServer();
                        
                        if(me.server) {
                            me.reload();
                        }
                    } else {
                    	me.runMonitors();
                    }
                	
                } else {
                    me.visible = false;
                    me.haltMonitors();
                }
            },
            
            serverChanged : function(ev) {
                
                if( me.server != wa.Servers.getCurrentServer() ) {
                    me.server = wa.Servers.getCurrentServer();
                    // If the monitor page is currently visible
                    if( me.visible === true ) {
                    	me.reload();
                    }
                }
            },
            
            init : function() { }
            
    };
    
    // 
    // PRIVATE
    //
    
    /**
     * Resets the state of the dashboard, loads appropriate server
     * information, and dashboard widgets, and re-renders.
     */
    me.reload = function() {

        me.destroyMonitors();
        var server = wa.Servers.getCurrentServer();
        
        if( server ) {
    	
        	server.manage.jmx.getBean("neo4j", "Kernel", function(kernelBean){
        		me.kernelInfo = kernelBean;
    	    	me.render();
            	me.loadMonitors(server);
            	
            	$("#mor_monitor_lifecycle").empty();
            	//Disable for now
            	//$("#mor_monitor_lifecycle").append( wa.widgets.LifecycleWidget(server).render() );
        	});
        } else {
        	me.render();
        }
        
    };
    
    me.render = function() {
    	me.basePage.processTemplate({
            server : me.server,
            kernelInfo : me.kernelInfo
        });
    };
    
    me.destroyMonitors= function() {
    	me.haltMonitors();
    	
    	me.valueTrackers = [];
    	me.charts = [];
    };
    
    me.haltMonitors = function() {
    	for( var i = 0, l = me.charts.length; i < l ; i++ ) {
			me.charts[i].stopDrawing();
    	}
    	
    	for( var i = 0, l = me.valueTrackers.length; i < l ; i++ ) {
			me.valueTrackers[i].stopPolling();
    	}
    };
    
    me.runMonitors = function() {
    	if( me.visibleChart !== null ) {
    		me.visibleChart.startDrawing();
    	}
    	
    	for( var i = 0, l = me.valueTrackers.length; i < l ; i++ ) {
			me.valueTrackers[i].startPolling();
    	}
    };
    
    me.loadMonitors = function(server) {
    	var box = $("#mor_monitor_valuetrackers"),
    	    primitivesChartWrap = $("#mor_monitor_primitives_chart"),
    	    memoryChartWrap = $("#mor_monitor_memory_chart"),
    	    primitiveTracker = wa.components.dashboard.PrimitiveCountWidget(server),
    	    diskTracker      = wa.components.dashboard.DiskUsageWidget(server),
    	    cacheTracker     = wa.components.dashboard.CacheWidget(server);
    	
    	var primitivesChart = wa.components.dashboard.MonitorChart(server, {
    		label : 'Primitive entitites',
    		data : {
    		    node_count : {
				    label : 'Nodes'
				},
				relationship_count : {
				    label : 'Relationships'
			    },
			    property_count : {
				    label : 'Properties'
			    }
    		}
    	});
    	
    	var memoryChart = wa.components.dashboard.MonitorChart(server, {
    		label : 'Heap memory usage',
    		data : {
    			memory_usage_percent : {
				    label : 'Heap memory usage'
				}
    		},
    		yaxis : {
    			min : 0,
    			max : 100
    		},
    		series : {
    			lines: { show: true, fill: true, fillColor: "#4f848f" }
    		},
    		tooltipValueFormatter : function(v) {
    			return Math.floor(v) + "%";
    		}
    	});
    	
    	me.valueTrackers.push(primitiveTracker);
    	me.valueTrackers.push(diskTracker);
    	me.valueTrackers.push(cacheTracker);
    	
    	me.charts.push(primitivesChart);
    	me.charts.push(memoryChart);
    	
    	me.primitivesChart = primitivesChart;
    	me.visibleChart = me.primitivesChart;
    	me.memoryChart = memoryChart;

    	box.append(primitiveTracker.render());
    	box.append(diskTracker.render());
    	box.append(cacheTracker.render());

    	primitivesChartWrap.append(primitivesChart.render());
    	memoryChartWrap.append(memoryChart.render());
    	
    	primitivesChart.startDrawing();
    };
    
    //
    // CONSTRUCT
    //
    
    $("#mor_monitor_memory_chart_tab").live("click", function(ev) {
    	ev.preventDefault();
    	
    	$(ev.target).parent().addClass("current");
    	$("#mor_monitor_primitives_chart_tab").parent().removeClass("current");
    	
    	me.primitivesChart.stopDrawing();
    	$("#mor_monitor_primitives_chart").hide();
    	$("#mor_monitor_memory_chart").show();
    	me.memoryChart.startDrawing();
    	me.visibleChart = me.memoryChart;
    });
    
    $("#mor_monitor_primitives_chart_tab").live("click", function(ev) {
    	ev.preventDefault();
    	
    	$(ev.target).parent().addClass("current");
    	$("#mor_monitor_memory_chart_tab").parent().removeClass("current");
    	
    	me.memoryChart.stopDrawing();
    	$("#mor_monitor_memory_chart").hide();
    	$("#mor_monitor_primitives_chart").show();
    	me.primitivesChart.startDrawing();
    	me.visibleChart = me.primitivesChart;
    });
    
    return me.api;
    
})(jQuery);

//
// REGISTER STUFF
//

wa.ui.Pages.add("dashboard",wa.components.dashboard.Dashboard);
wa.ui.MainMenu.add({ label : "Dashboard", pageKey:"dashboard", index:0, requiredServices:['monitor'], perspectives:['server']});

wa.bind("init", wa.components.dashboard.Dashboard.init);
wa.bind("ui.page.changed", wa.components.dashboard.Dashboard.pageChanged);
wa.bind("servers.current.changed",  wa.components.dashboard.Dashboard.serverChanged);