#!/bin/bash


rm *
cat << EOF > acinclude.m4
m4_include([../mysql.m4])
EOF

cat << EOF > configure.in
AC_INIT(example, 1.0)
AM_INIT_AUTOMAKE(example, 1.0)
WITH_MYSQL()
AC_OUTPUT(Makefile)
EOF

cat << EOF > Makefile.am
bin_PROGRAMS = example
example_SOURCES = example.cc
EOF

cat << EOF >> acinclude.m4
dnl
dnl configure.in helper macros
dnl

AC_DEFUN([WITH_MYSQL], [
  AC_MSG_CHECKING(for mysql_config executable)

  AC_ARG_WITH(mysql, [  --with-mysql=PATH path to mysql_config binary or mysql prefix dir], [
  if test -x \$withval -a -f \$withval
    then
      MYSQL_CONFIG=\$withval
    elif test -x \$withval/bin/mysql_config -a -f \$withval/bin/mysql_config
    then
     MYSQL_CONFIG=\$withval/bin/mysql_config
    fi
  ], [
  if test -x /usr/local/mysql/bin/mysql_config -a -f /usr/local/mysql/bin/mysql_config
    then
      MYSQL_CONFIG=/usr/local/mysql/bin/mysql_config
    elif test -x /usr/bin/mysql_config -a -f /usr/bin/mysql_config
    then
      MYSQL_CONFIG=/usr/bin/mysql_config
    fi
  ])

  if test "x\$MYSQL_CONFIG" = "x"
  then
    AC_MSG_RESULT(not found)
    exit 3
  else
    AC_PROG_CC
    AC_PROG_CXX

    # add regular MySQL C flags
    ADDFLAGS=\`\$MYSQL_CONFIG --cflags\`

    # add NDB API specific C flags
    IBASE=\`\$MYSQL_CONFIG --include\`
    ADDFLAGS="\$ADDFLAGS \$IBASE/storage/ndb"
    ADDFLAGS="\$ADDFLAGS \$IBASE/storage/ndb/ndbapi"
    ADDFLAGS="\$ADDFLAGS \$IBASE/storage/ndb/mgmapi"

    CFLAGS="\$CFLAGS \$ADDFLAGS"
    CXXFLAGS="\$CXXFLAGS \$ADDFLAGS"

    LDFLAGS="\$LDFLAGS "\`\$MYSQL_CONFIG --libs_r\`" -lndbclient -lmystrings -lmysys"
    LDFLAGS="\$LDFLAGS "\`\$MYSQL_CONFIG --libs_r\`" -lndbclient -lmystrings"

    AC_MSG_RESULT(\$MYSQL_CONFIG)
  fi
])
EOF

