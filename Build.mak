# check whether to build with position-independent code
# (may be required to build on newer distro releases)
ifeq ($(USE_PIC),1)
	override DFLAGS += -fPIC
endif

# some D compilers are more picky than others, so tolerating
# warnings may be necessary in order to build with them
ifeq ($(ALLOW_WARNINGS), 1)
	override DFLAGS += -wi
else
	override DFLAGS += -w
endif

override LDFLAGS += -llzo2 -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0

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
	$C/deploy/logrotate/dlsnode-logs \
	$C/deploy/cron/compress_dls_data \
	$C/deploy/cron/compress_data.sh

$O/pkg-dlsnode.stamp: \
	$C/pkg/defaults.py \
	$C/build/$F/bin/dlsnode

