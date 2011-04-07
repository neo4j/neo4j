###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/data/ItemUrlResolver'
   'neo4j/webadmin/templates/databrowser/visualizationSettings',
   'neo4j/webadmin/views/View',
   'lib/backbone'], 
  (ItemUrlResolver, template, View) ->
  
    class VisualizationSettingsDialog extends View

      className: "popout"

      events : 
        "click #save-visualization-settings" : "save"

      initialize : (opts) =>
        $("body").append(@el)

        @baseElement = opts.baseElement
        @closeCallback = opts.closeCallback
        @settings = opts.appState.getVisualizationSettings()

        @position()
        @render()

      save : =>
        keys = $("#visualization-label-properties").val().split(",")
        @settings.setLabelProperties(keys)
        @settings.save()
        @closeCallback()

      position : =>
        basePos = $(@baseElement).offset()
        top = basePos.top + $(@baseElement).outerHeight()
        left= basePos.left - ($(@el).outerWidth()-$(@baseElement).outerWidth())
        $(@el).css({position:"absolute", top:top+"px", left:left+"px"})
        
      render : () =>
        $(@el).html(template( labels : @settings.getLabelProperties().join(",") ))
        return this

)
