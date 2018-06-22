#
#  Copyright (C) 2016 ScyllaDB

is_debian_variant() {
    [ -f /etc/debian_version ]
}

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}

is_gentoo_variant() {
    [ -f /etc/gentoo-release ]
}

is_systemd() {
    grep -q '^systemd$' /proc/1/comm
}

is_ec2() {
    [ -f /sys/hypervisor/uuid ] && [ "$(head -c 3 /sys/hypervisor/uuid)" = "ec2" ]
}

is_selinux_enabled() {
    STATUS=`getenforce`
    if [ "$STATUS" = "Disabled" ]; then
        return 0
    else
        return 1
    fi
}

ec2_is_supported_instance_type() {
    TYPE=`curl -s http://169.254.169.254/latest/meta-data/instance-type|cut -d . -f 1`
    case $TYPE in
           "i2"|"i3") echo 1;;
            *) echo 0;;
    esac
}

verify_args() {
     if [ -z "$2" ] || [[ "$2" =~ ^--+ ]]; then
        echo "Requires more parameter for $1."
        print_usage
        exit 1
    fi
}

#
#  get_mode_cpu_set <mode name, e.g. 'mq', 'sq', 'sq_split'>
#
get_mode_cpu_set() {
    local mode=$1
    local mode_cpu_mask
    mode_cpu_mask=`/usr/lib/scylla/perftune.py --tune net --nic "$nic" --mode "$mode" --get-cpu-mask 2>&-`

    # If the given mode is not supported - return invalid CPU set
    if [[ "$?" -ne "0" ]]; then
        echo "-1"
    else
        echo "$mode_cpu_mask" | /usr/lib/scylla/hex2list.py
    fi
}

#
# check_cpuset_conf <NIC name>
#
get_tune_mode() {
    local nic=$1

    # if cpuset.conf doesn't exist use the default mode
    [[ ! -e '/etc/scylla.d/cpuset.conf' ]] && return

    local cur_cpuset=`cat /etc/scylla.d/cpuset.conf | cut -d "\"" -f2- | cut -d" " -f2`
    local mq_cpuset=`get_mode_cpu_set 'mq'`
    local sq_cpuset=`get_mode_cpu_set 'sq'`
    local sq_split_cpuset=`get_mode_cpu_set 'sq_split'`
    local tune_mode=""

    case "$cur_cpuset" in
        "$mq_cpuset")
            tune_mode="--mode mq"
            ;;
        "$sq_cpuset")
            tune_mode="--mode sq"
            ;;
        "$sq_split_cpuset")
            tune_mode="--mode sq_split"
            ;;
    esac

    # if cpuset is something different from what we expect - use the default mode
    echo "$tune_mode"
}

#
# create_perftune_conf [<NIC name>]
#
create_perftune_conf() {
    local nic=$1
    [[ -z "$nic" ]] && nic='eth0'

    # if exists - do nothing
    [[ -e '/etc/scylla.d/perftune.yaml' ]] && return

    local mode=`get_tune_mode "$nic"`
    /usr/lib/scylla/perftune.py --tune net --nic "$nic" $mode --dump-options-file > /etc/scylla.d/perftune.yaml
}

. /etc/os-release
if is_debian_variant || is_gentoo_variant; then
    SYSCONFIG=/etc/default
else
    SYSCONFIG=/etc/sysconfig
fi
. $SYSCONFIG/scylla-server

for i in /etc/scylla.d/*.conf; do
    if [ "$i" = "/etc/scylla.d/*.conf" ]; then
        break
    fi
    . "$i"
done
