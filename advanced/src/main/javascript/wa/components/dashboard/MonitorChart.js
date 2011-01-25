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

wa.components.dashboard.monitorChartCounter = 0;

/**
 * A chart that shows data from one or more monitor data sources over time. The
 * data is acquired via the morpheus.monitor.update event, which is
 * triggered for each loaded server at regular intervals.
 * 
 * Available settings include:
 * 
 * <ul>
 * <li>label : the label for the module that the graph is shown in</li>
 * <li>height : integer value, height in pixels of the chart, default is 200</li>
 * <li>data : an object with data source definitions. Each key in this object
 * should correspond to a data set key used by the rrdb monitor system. See
 * {@link org.neo4j.server.webadmin.rrd.RrdManager}. For available settings in each
 * data set, see below</li>
 * </ul>
 * 
 * Data sources have the following settings:
 * 
 * <ul>
 * <li>label : is the label to show in the legend</li>
 * </ul>
 * 
 * @param server
 *            is the server to work with
 * @param settings
 *            is an object that specifies what data to show and how. See above
 *            for available settings.
 */
wa.components.dashboard.MonitorChart = function(server, settings) {
	
	var me = {};
	
	// Default settings
	me.settings = {
		label : "",
		xaxis : {
			mode: "time",
		    timeformat: "%H:%M:%S"
		},
		yaxis : { },
		height : 200,
		data : [],
		series : {
			
		},
		tooltipValueFormatter : function(v) {return v;},
		colors : ["#326a75","#4f848f","#a0c2c8","#00191e"]
	};
	
	// Override defaults with user settings
	var settings = settings || {};
	$.extend( true, me.settings, settings );
	
	// Define data series
	me.series = [];
	for( var key in me.settings.data ) {
		me.settings.data[key].key = key;
		me.series.push(me.settings.data[key]);
	}
	
	me.server = server;
	me.containerId = "mor_monitor_chart_" + wa.components.dashboard.monitorChartCounter++;
	me.controlsClass = me.containerId + "_controls";
	me.zoom = {
		year : { 
			id : me.containerId + "_zoom_0",
			xSpan : 1000 * 60 * 60 * 24 * 365,
		    timeformat: "%d/%m %y"
		},
		month : { 
			id : me.containerId + "_zoom_1",
			xSpan : 1000 * 60 * 60 * 24 * 30,
		    timeformat: "%d/%m"
		},
		week : { 
			id : me.containerId + "_zoom_2",
			xSpan : 1000 * 60 * 60 * 24 * 7,
		    timeformat: "%d/%m"
		},
		day : { 
			id : me.containerId + "_zoom_3",
			xSpan : 1000 * 60 * 60 * 24,
		    timeformat: "%H:%M"
		},
		six_hours : { 
			id : me.containerId + "_zoom_4",
			xSpan : 1000 * 60 * 60 * 6,
		    timeformat: "%H:%M"
		},
		thirty_minutes : { 
			id : me.containerId + "_zoom_5",
			xSpan : 1000 * 60 * 30,
		    timeformat: "%H:%M"
		}
	};
	
	// TODO: Move this into a template
	var controls = "<ul class='mor_module_actions'>";
	controls += "<li><a class='"+me.controlsClass+"' href='#' id='"+me.zoom.year.id +"'>Year</a></li>";
	controls += "<li><a class='"+me.controlsClass+"' href='#' id='"+me.zoom.month.id +"'>Month</a></li>";
	controls += "<li><a class='"+me.controlsClass+"' href='#' id='"+me.zoom.week.id +"'>Week</a></li>";
	controls += "<li><a class='"+me.controlsClass+"' href='#' id='"+me.zoom.day.id +"'>Day</a></li>";
	controls += "<li><a class='"+me.controlsClass+"' href='#' id='"+me.zoom.six_hours.id +"'>6 hours</a></li>";
	controls += "<li class='current'><a class='"+me.controlsClass+"' href='#' id='"+me.zoom.thirty_minutes.id +"'>30 minutes</a></li>";
	controls += "</ul><div class='break'></div>";
	
	me.container = $("<div>" + controls + "<div class='mor_chart_container'><div style='height:"+me.settings.height+"px;' id='" + me.containerId + "'></div></div></div>");
	
	me.drawing = false;
	me.currentZoom = "thirty_minutes";
	me.currentData = [];
	
	me.api = {
		
		render : function() {
			return me.container;
		},
		
		startDrawing : function() {
			if ( ! me.drawing ) {
				
				me.drawing = true;
				
			}
			
			me.draw(server.heartbeat.getCachedData());
		},
		
		stopDrawing : function() {
			me.drawing = false;
		}
			
	};
	
	//
	// INTERNALS
	//
	
	me.draw = function(data) {
		
		// Set zoom and tick label formatting
		if( data.timestamps.length > 0 ) {
			me.settings.xaxis.min = me.convertToLocalTimestamp(data.timestamps[ data.timestamps.length - 1 ]) - me.zoom[me.currentZoom].xSpan;
			me.settings.xaxis.timeformat = me.zoom[me.currentZoom].timeformat;
		}
		
		$.plot($("#" + me.containerId), me.parseData(data), {
			xaxis : me.settings.xaxis,
			yaxis : me.settings.yaxis,
			grid: { hoverable: true },
			legend: {
				position : 'nw'
			},
			series : me.settings.series,
			colors : me.settings.colors
		});
	};
	
	/**
	 * Convert from UTC-timestamps to local timezone timestamps. 
	 * This is the way flot does time zones. Great, isn't it?
	 */
	me.convertToLocalTimestamp = function (v) {
		return v - (new Date()).getTimezoneOffset() * 60 * 1000; 
	};
	
	me.parseData = function(data) {
		var output = [];
		var numberOfDataSeries = me.series.length;
		
		// Initialize data arrays for all data series
		for( var dataIndex = 0; dataIndex < numberOfDataSeries; dataIndex ++ ) {
			output[dataIndex] = {
				data : []
			};
			
			$.extend( true, output[dataIndex], me.settings.data[me.series[dataIndex].key] );
		}
		
		// Format data for jqChart
		for( var i = 0, l = data.timestamps.length; i < l; i++ ) {
			for( var dataIndex = 0; dataIndex < numberOfDataSeries; dataIndex ++ ) {
				output[dataIndex].data.push( [ me.convertToLocalTimestamp(data.timestamps[i]), data.data[ me.series[dataIndex].key ][i] ] );
			}
		}
		
		return output;
	};
	
	me.showTooltip = function (x, y, contents) {
        $('<div id="mor_chart_tooltip">' + contents + '</div>').css( {
            position: 'absolute',
            display: 'none',
            top: y + 5,
            left: x + 5,
            border: '1px solid #a1a8a9',
            padding: '2px',
            'background-color': '#f6f6f6',
            opacity: 0.80
        }).appendTo("body").fadeIn(100);
    };

    me.removeTooltip = function() {
		$("#mor_chart_tooltip").remove();
        me.previousHoverPoint = null;
	};
	
	me.zeroPad = function(val) {
		val = val + "";
		if(val.length == 1) {
			return "0" + val;
		} else {
			return val;
		}
	};
	
	// 
	// CONSTRUCT
	//
	
	for( var key in me.zoom ) {
		$("#" + me.zoom[key].id).live('click', (function(zoomKey) {
			return function(ev) {
				ev.preventDefault();
				me.currentZoom = zoomKey;
				
				$("." +me.controlsClass).parent().removeClass("current");
				$("#" + me.zoom[zoomKey].id).parent().addClass("current");
				
				me.draw( server.heartbeat.getCachedData() );
			};
		})(key));
	}
	
	// Listen for data updates
	server.heartbeat.addListener(function(data) {
		if( me.drawing ) {
			me.draw( data.allData );
		}
	});
	
	// Keep track of the mouse over the chart
	me.previousHoverPoint = null;
	$("#" + me.containerId).live("plothover", function (event, pos, item) {
        if (item) {
            if (me.previousHoverPoint != item.datapoint) {
            	me.previousHoverPoint = item.datapoint;
                
                $("#mor_chart_tooltip").remove();
                var x = new Date(item.datapoint[0]),
                    y = item.datapoint[1];
                
                y = me.settings.tooltipValueFormatter(y);
                me.showTooltip(item.pageX, item.pageY, "<span><b>" + item.series.label + ":</b> " + y + "</span><br /><span style='font-size:12px;'>" +  me.zeroPad(x.getUTCDate()) + "/" + me.zeroPad(x.getUTCMonth()) + " - " + me.zeroPad(x.getUTCHours()) + ":" + me.zeroPad(x.getUTCMinutes()) + ":" + me.zeroPad(x.getUTCSeconds()) + "</span>");
            }
        }
        else {
        	me.removeTooltip();
        }
    });
	
	$("#" + me.containerId).live("mouseout", me.removeTooltip);
	
	$("#mor_chart_tooltip").live("mouseout", me.removeTooltip);

	
	return me.api;
};