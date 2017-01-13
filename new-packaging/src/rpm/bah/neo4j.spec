Name: neo4j
Version: 3.2.0
Release: 1%{?dist}
Summary: Neo4j server is a database that stores data as graphs rather than tables.

License: AGPLv3
URL: http://neo4j.org/
Source0: https://github.com/neo4j/neo4j/archive/%{version}.tar.gz

#BuildRequires:
Requires: java-headless = 1:8, javapackages-tools

BuildArch:      noarch


%description

Neo4j is a highly scalable, native graph database purpose-built to
leverage not only data but also its relationships.


%prep
%setup -q


%build

%install

#mkdir -p %{buildroot}/%{_bindir}

#install -m 0755 %{name} %{buildroot}/%{_bindir}/%{name}


%files
# destination on installed system
#%license LICENSE
#%{_bindir}/%{name}
