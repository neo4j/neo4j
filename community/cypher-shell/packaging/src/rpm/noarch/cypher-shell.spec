Name: cypher-shell
Version: ${VERSION}
Release: ${RELEASE}%{?dist}
Summary: Command line shell for Neo4j

License: GPLv3
URL: https://github.com/neo4j/neo4j
Source0: https://github.com/neo4j/neo4j/archive/%{version}.tar.gz

#Conflicts:
Requires: which, java-17 >= 17
BuildArch: noarch
Prefix: /usr

%description
A command line shell where you can execute Cypher against an instance
of Neo4j.

%prep
# This macro will unpack the tarball into the appropriate build directory
# Expects tarball to unpack into a directory called {name}-{version}
#%setup -q
%build
#make clean build

%install
rm -rf ${buildroot}

mkdir -p %{buildroot}/%{_bindir}
mkdir -p %{buildroot}/%{_datadir}/cypher-shell/bin
mkdir -p %{buildroot}/%{_datadir}/cypher-shell/lib
mkdir -p %{buildroot}/%{_mandir}/man1

cd %{name}-%{version} || exit

install -m 0755 cypher-shell/bin/cypher-shell %{buildroot}/%{_datadir}/cypher-shell/bin/
ln %{_datadir}/cypher-shell/bin/cypher-shell -s %{buildroot}/%{_bindir}
install -m 0755 cypher-shell/lib/*.jar %{buildroot}/%{_datadir}/cypher-shell/lib/
install -m 0644 manpages/* %{buildroot}/%{_mandir}/man1

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root)
%{_bindir}/cypher-shell
%{_datadir}/cypher-shell
%doc %{_mandir}/man1/*
