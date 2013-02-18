#!/bin/sh
source $(dirname $0)/bootstrap.sh

MAIN_CLASS_PATH=$HADIR/classes
MAINCLASS=org.neo4j.ha.StartLocalHaDb

$SCRIPTDIR/shutdown.sh $MAINCLASS pid/ha

COUNT=1
OPTIONS=()
for OPT in "$@"; do
    case "$OPT" in
        -*)
            OPTIONS=(${OPTIONS[@]} $OPT)
            ;;
        [0-9])
            COUNT=$OPT
            ;;
        *)
            echo UNKNOWN PARAMETER $OPT
            ;;
    esac
done

EXISTING=()
for HA in data/ha?; do
    EXISTING=(${EXISTING[@]} $(basename $HA | cut -b3-))
    if [ ! -f etc/$(basename $HA).cfg ]; then
        EXISTING=()
        break
    fi
done

if [ $# -lt 1 ]; then
    if [ -z "${EXISTING[*]}" ]; then
        INSTANCES=(1)
    else
        INSTANCES=(${EXISTING[@]})
    fi
else
    INSTANCES=()
    for ((HA=1; HA <= $COUNT ; HA++)); do
        INSTANCES=(${INSTANCES[@]} $HA)
    done
fi

ZKCLUSTER=$(grep clientPort etc/zk?.cfg \
          | cut -d= -f2                 \
          | lam -s localhost: -         \
          | paste -s -d , -)

if rebuild; then
    MAIN_CLASS_PATH=$HADIR/target/test-classes
    if [ ! -f "$HAZIP" ]; then
        cd $HADIR
        ant
        cd -
    elif [ ! -d "$MAIN_CLASS_PATH" ]; then
        cd $HADIR
        mvn test-compile
        cd -
    fi
    
    mkdir $LIBDIR
    cd $LIBDIR
    unzip -o $HAZIP
    cd -
fi

CLASSPATH=$MAIN_CLASS_PATH
for JARFILE in $LIBDIR/*.jar; do
    CLASSPATH=$CLASSPATH:$JARFILE
done

mkdir -p data etc pid
rm -f pid/ha

if [ "${INSTANCES[*]}" != "${EXISTING[*]}" ]; then
    rm -rf data/empty
    java -cp $CLASSPATH org.neo4j.ha.CreateEmptyDb data/empty
fi

for HA in "${INSTANCES[@]}"; do
    HACONF=etc/ha$HA.cfg
    if [ "${INSTANCES[*]}" != "${EXISTING[*]}" ]; then
        rm -rf data/ha$HA
        cp -R data/empty data/ha$HA

        echo ha.server_id = $HA                 > $HACONF
        echo ha.server = localhost:600$HA       >>$HACONF
        echo ha.upgrade_coordinators = $ZKCLUSTER       >>$HACONF
        echo enable_remote_shell = port=133$HA  >>$HACONF
    fi

    VMOPTIONS=
    for OPT in "${OPTIONS[@]}"; do
        case "$OPT" in
            -debug)
                VMOPTIONS="$VMOPTIONS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=144$HA"
                ;;
            -empty)
                if [ "${INSTANCES[*]}" == "${EXISTING[*]}" ]; then
                    rm -rf data/ha$HA
                    cp -R data/empty data/ha$HA
                fi
                ;;
        esac
    done

    java -cp $CLASSPATH $VMOPTIONS $MAINCLASS data/ha$HA $HACONF &
    echo $! >> pid/ha
done
