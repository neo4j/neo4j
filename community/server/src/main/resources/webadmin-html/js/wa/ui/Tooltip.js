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

wa.ui.Tooltip = function(settings) {
        
    this._tooltip = $("<div class='tooltip-wrap'></div>");
    this._tooltipContent = $("<div class='tooltip'></div>");
    this._closeButton = $("<div class='tooltip-close'></div>");
    
    this._settings = _.extend({
        hideOnMouseOut : true,
        css : {},
        position : "above",
        closeButton : true
    }, settings || {});
    
    this._currentPos = [0,0];
    this._currentContent = "";
    this._visible = false;
    
    _.bindAll(this, "show", "hide", "onWindowResized");
    
    if( this._settings.hideOnMouseOut ) {
        this._tooltip.mouseout(this.hide);
    }
    
    this._tooltip.addClass("tooltip-pos-" + this._settings.position);
    this._tooltip.css(this._settings.css);
    this._tooltip.append(this._tooltipContent);

    if( this._settings.closeButton ) {
        this._tooltip.append(this._closeButton);
        this._closeButton.click(this.hide);
    }
    
    this._tooltip.appendTo("body");
    
    $(window).resize(this.onWindowResized);
    
};

_.extend(wa.ui.Tooltip.prototype, {
    
    show : function(content, pos, timeout) {
        this._currentPos = pos;
        this._currentContent = content;
        
        this._tooltipContent.html(content);
        pos = this._getTooltipPositionFor(this._getPosition(pos));
        this._tooltip.css({left: pos[0], top: pos[1] }).show();
        this._visible = true;
        
        if(timeout) {
            setTimeout(this.hide,timeout);
        }
    },
    
    hide : function() {
        this._tooltip.hide();
        this._visible = false;
    },
    
    onWindowResized : function() {
        if(this._visible) {
            var tooltip = this;
            setTimeout(function(){
                tooltip.show(tooltip._currentContent, tooltip._currentPos);
            }, 0);
        }
    },
    
    /**
     * If pos is an array, return it, assuming it already is a position.
     * Else, assume pos is a html element, return it's top center position.
     */
    _getPosition : function(pos) {
        if(_.isArray(pos)) {
            return pos;
        } else {
            var el = $(pos);
            pos = el.offset();
            if(this._settings.position === "right") {
                return [pos.left + (el.width()), pos.top + (el.height()/2)]
            } else if(this._settings.position === "left") {
                return [pos.left, pos.top + (el.height()/2)]
            } else if(this._settings.position === "above") {
                return [pos.left + (el.width()/2), pos.top]
            } else {
                return [pos.left + (el.width()/2), pos.top - el.height()]
            }
        }
    },
    
    /**
     * Given some point, calculate where we should put the tooltip to make
     * it "point" to that point.
     */
    _getTooltipPositionFor: function(pointToPosition) {
        if(this._settings.position === "right") {
            return [pointToPosition[0] + 10 , 
                    pointToPosition[1] - (this._tooltip.height() / 2) ];
        } else if(this._settings.position === "left") {
            return [pointToPosition[0] - (this._tooltip.width() + 10) , 
                    pointToPosition[1] - (this._tooltip.height() / 2) ];
        } else if(this._settings.position === "above") {
            return [pointToPosition[0] - this._tooltip.width() / 2 , 
                    pointToPosition[1] - (this._tooltip.height() + 10) ];
        } else {
            return [pointToPosition[0] - this._tooltip.width() / 2 , 
                    pointToPosition[1] + (this._tooltip.height()) ];
        }
    }
    
});