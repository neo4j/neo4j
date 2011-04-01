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
(function($){

$.widget("ech.notify", {
	options: {
		speed: 500,
		expires: 5000,
		stack: 'below',
		custom: false
	},
	_create: function(){
		var self = this;
		this.templates = {};
		this.keys = [];
		
		// build and save templates
		this.element.addClass("ui-notify").children().addClass("ui-notify-message ui-notify-message-style").each(function(i){
			var key = this.id || i;
			self.keys.push(key);
			self.templates[key] = $(this).removeAttr("id").wrap("<div></div>").parent().html(); // because $(this).andSelf().html() no workie
		}).end().empty();
		
	},
	create: function(template, msg, opts){
		if(typeof template === "object"){
			opts = msg;
			msg = template;
			template = null;
		}
		
		var tpl = this.templates[ template || this.keys[0]];
		
		// remove default styling class if rolling w/ custom classes
		if(opts && opts.custom){
			tpl = $(tpl).removeClass("ui-notify-message-style").wrap("<div></div>").parent().html();
		}
		
		// return a new notification instance
		return new $.ech.notify.instance(this)._create(msg, $.extend({}, this.options, opts), tpl);
	}
});

// instance constructor
$.extend($.ech.notify, {
	instance: function(widget){
		this.parent = widget;
		this.isOpen = false;
	}
});

// instance methods
$.extend($.ech.notify.instance.prototype, {
	_create: function(params, options, template){
		this.options = options;
		
		var self = this,
			
			// build html template
			html = template.replace(/#(?:\{|%7B)(.*?)(?:\}|%7D)/g, function($1, $2){
				return ($2 in params) ? params[$2] : '';
			}),
			
			// the actual message
			m = (this.element = $(html)),
			
			// close link
			closelink = m.find("a.ui-notify-close");
		
		// clickable?
		if(typeof this.options.click === "function"){
			m.addClass("ui-notify-click").bind("click", function(e){
				self._trigger("click", e, self);
			});
		}
		
		// show close link?
		if(closelink.length){
			closelink.bind("click", function(){
				self.close();
				return false;
			});
		}
		
		this.open();
		
		// auto expire?
		if(typeof options.expires === "number"){
			window.setTimeout(function(){
				self.close();
			}, options.expires);
		}
		
		return this;
	},
	close: function(){
		var self = this, speed = this.options.speed;
		this.isOpen = false;
		
		this.element.fadeTo(speed, 0).slideUp(speed, function(){
			self._trigger("close");
		});
		
		return this;
	},
	open: function(){
		if(this.isOpen || this._trigger("beforeopen") === false){
			return this;
		}

		var self = this;
		this.isOpen = true;
		
		this.element[this.options.stack === 'above' ? 'prependTo' : 'appendTo'](this.parent.element).css({ display:"none", opacity:"" }).fadeIn(this.options.speed, function(){
			self._trigger("open");
		});
		
		return this;
	},
	widget: function(){
		return this.element;
	},
	_trigger: function(type, e, instance){
		return this.parent._trigger.call( this, type, e, instance );
	}
});

})(jQuery);

