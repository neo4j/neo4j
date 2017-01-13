Name: ${PACKAGE_NAME}
Provides: neo4j
Version: ${VERSION}
Release: 1%{?dist}
Summary: Neo4j server is a database that stores data as graphs rather than tables.

License: ${LICENSE}
URL: http://neo4j.org/
#Source: https://github.com/neo4j/neo4j/archive/%{version}.tar.gz

#BuildRequires: systemd
#Requires: java-headless = 1.8.0, javapackages-tools

BuildArch:      noarch

%define neo4jhome %{_localstatedir}/lib/neo4j


%description

Neo4j is a highly scalable, native graph database purpose-built to
leverage not only data but also its relationships.


%prep
echo "In prep: $(pwd)"
ls
#%setup -q

%build
echo "In Build: $(pwd)"
ls

%install
echo "In install: $(pwd)"
ls
mkdir -p %{buildroot}/%{_bindir}
mkdir -p %{buildroot}/%{_datadir}/neo4j/lib
mkdir -p %{buildroot}/%{_datadir}/neo4j/bin/tools
mkdir -p %{buildroot}/%{_datadir}/doc/neo4j
mkdir -p %{buildroot}/%{neo4jhome}/plugins
mkdir -p %{buildroot}/%{neo4jhome}/data/databases
mkdir -p %{buildroot}/%{neo4jhome}/import
mkdir -p %{buildroot}/%{_sysconfdir}/neo4j
mkdir -p %{buildroot}/%{_localstatedir}/log/neo4j
mkdir -p %{buildroot}/lib/systemd/system

cd %{name}-%{version}

install neo4j.service %{buildroot}/lib/systemd/system/neo4j.service

install -m 0644 server/conf/* %{buildroot}/%{_sysconfdir}/neo4j

install -m 0755 server/scripts/* %{buildroot}/%{_bindir}

install -m 0755 server/lib/* %{buildroot}/%{_datadir}/neo4j/lib

cp -r server/bin/* %{buildroot}/%{_datadir}/neo4j/bin
chmod -R 0755 %{buildroot}/%{_datadir}/neo4j/bin

#touch %{buildroot}/%{_datadir}/log/neo4j/debug.log

install -m 0644 server/README.txt %{buildroot}/%{_datadir}/doc/neo4j/README.txt
install -m 0644 server/UPGRADE.txt %{buildroot}/%{_datadir}/doc/neo4j/UPGRADE.txt
install -m 0644 server/LICENSES.txt %{buildroot}/%{_datadir}/doc/neo4j/LICENSES.txt

%clean
echo "In clean"

%pre

# Create neo4j user if it doesn't exist.
if ! id neo4j > /dev/null 2>&1 ; then
  adduser --system --home %{neo4jhome} --no-create-home \
          --no-user-group --shell /bin/bash \
          neo4j
fi

%files
%defattr(-,root,root)

%{_datadir}/neo4j
%{_bindir}/*
%attr(-,neo4j,root) %{neo4jhome}
%attr(-,neo4j,root) %dir %{_localstatedir}/log/neo4j
/lib/systemd/system/neo4j.service

%attr(-,neo4j,root) %config(noreplace) %{_sysconfdir}/neo4j

%doc %{_datadir}/doc/neo4j/README.txt
%doc %{_datadir}/doc/neo4j/UPGRADE.txt

%license %{_datadir}/doc/neo4j/LICENSES.txt
