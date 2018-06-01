override DFLAGS += -w
override LDFLAGS += -llzo2 -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0

ifneq ($(DVER),1)
	DC := dmd-transitional
else
override DFLAGS += -v2 -v2=-static-arr-params
endif

$B/dlsnode: override LDFLAGS += -lpcre -lebtree
$B/dlsnode: src/dlsnode/main.d
$B/dlsredist: override LDFLAGS += -lpcre -lebtree
$B/dlsredist: src/dlsredist/main.d
dlsnode: $B/dlsnode
dlsredist: $B/dlsredist
all += dlsnode dlsredist

# Additional flags needed when unittesting
$O/%unittests: override LDFLAGS += -lpcre -lebtree -lrt


$O/test-dlstest: dlsnode
$O/test-dlstest: override LDFLAGS += -lebtree -lrt -lpcre

$O/test-versioning: dlsnode
$O/test-versioning: override LDFLAGS += -lebtree -lrt -lpcre

# Packages dependencies
$O/pkg-dlsnode-common.stamp: \
	$C/pkg/defaults.py \
	$C/deploy/upstart/dls.conf \
	$C/deploy/logrotate/dlsnode-logs \
	$C/deploy/cron/compress_dls_data \
	$C/deploy/cron/compress_data.sh

$O/pkg-dlsnode.stamp: \
	$C/pkg/defaults.py \
	$C/build/$F/bin/dlsnode

