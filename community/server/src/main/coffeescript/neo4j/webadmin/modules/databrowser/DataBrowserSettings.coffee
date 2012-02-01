###
  Copyright (c) 2002-2012 "Neo Technology,"
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
  ['./visualization/models/VisualizationProfiles'], 
  (VisualizationProfiles) ->

    class DataBrowserSettings

      LABEL_PROPERTIES_KEY : 'databrowser.labelProperties'
      LABEL_PROPERTIES_DEFAULT : ['name']
      
      VIZ_PROFILES_KEY : 'databrowser.visualization.profiles'
      VIZ_PROFILES_DEFAULT : [{ id:0, name:"Default profile", builtin:true }]

      CURRENT_VIZ_PROFILE_KEY : 'databrowser.visualization.currentProfile'

      constructor : (@settings) ->
        # Pass

      getLabelProperties : () ->
        s = @settings.get(@LABEL_PROPERTIES_KEY)
        if s and _(s).isArray()
          s
        else @LABEL_PROPERTIES_DEFAULT
        
      setLabelProperties : (properties) ->
        attr = {}
        attr[@LABEL_PROPERTIES_KEY] = properties
        @settings.set(attr)
        @settings.save()
      
      labelPropertiesChanged : (callback) ->
        @settings.bind 'change:' + @LABEL_PROPERTIES_KEY,  callback
        
      getVisualizationProfiles : () ->
        prof = @settings.get @VIZ_PROFILES_KEY, VisualizationProfiles, @VIZ_PROFILES_DEFAULT
        if prof.size() == 0
          @settings.remove @VIZ_PROFILES_KEY
          prof = @settings.get @VIZ_PROFILES_KEY, VisualizationProfiles, @VIZ_PROFILES_DEFAULT
        prof
        
      getCurrentVisualizationProfile : () ->
        id = @settings.get @CURRENT_VIZ_PROFILE_KEY
        profiles = @getVisualizationProfiles()
        if id? and profiles.get id
          return profiles.get id
        else
          return profiles.first()
      
      setCurrentVisualizationProfile : (id) ->
        id = id.id if id.id?
        @settings.set @CURRENT_VIZ_PROFILE_KEY, id
        
      onCurrentVisualizationProfileChange : (cb) ->
        @settings.bind "change:#{@CURRENT_VIZ_PROFILE_KEY}", cb
      
)
