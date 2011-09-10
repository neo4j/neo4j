###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/utils/ItemUrlResolver'
   './visualizationProfile',
   '../models/VisualizationProfile',
   '../models/StyleRule',
   './StyleRuleView',
   'ribcage/View',
   'lib/backbone'], 
  (ItemUrlResolver, template, VisualizationProfile, StyleRule, StyleRuleView, View) ->
  
    class VisualizationProfileView extends View

      events : 
        "click button.save" : "save"
        "click button.cancel" : "cancel"
        "click button.addStyleRule" : "addStyleRule"

      initialize : (opts) =>
        
        @profiles = opts.dataBrowserSettings.getVisualizationProfiles()
        @styleViews = []
        
      save : () =>
        
        name = $('#profile-name',@el).val()
        
        @profile.setName name
        
        if @isInCreateMode
          @profiles.add @profile
        @profile.save()
        
        window.location = '#/data/visualization/settings/'
        
      cancel : () =>
        window.location = '#/data/visualization/settings/'
          
      addStyleRule : () =>
        rule = new StyleRule()
        @profile.styleRules.add rule
        @addStyleRuleElement rule
        
      addStyleRuleElement : (rule) ->
        view = new StyleRuleView( rule : rule, rules:@profile.styleRules )
        @styleRuleContainer.append view.render().el
      
      render : () =>
        $(@el).html(template( name : @profile.getName(), isInCreateMode:@isInCreateMode ))
        
        @styleRuleContainer = $('.styleRules',@el)
        @profile.styleRules.each (rule) =>
          @addStyleRuleElement rule
        
        return this
        
      setProfileToManage : (@profile) ->
        @setIsCreateMode false
        
      setIsCreateMode : (@isInCreateMode) ->
        if @isInCreateMode        
          @profile = new VisualizationProfile(name:'A unique profile name')
        
      hasUnsavedChanges : () ->
        @profile.name != $('#profile-name',@el).val()

)
