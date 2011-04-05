actions :add, :remove

#name of the repo, used for source.list filename
attribute :repo_name, :kind_of => String, :name_attribute => true
attribute :uri, :kind_of => String
#whether or not to add the repository as a source repo as well
attribute :deb_src, :default => false 
attribute :distribution, :kind_of => String
attribute :components, :kind_of => Array, :default => []
