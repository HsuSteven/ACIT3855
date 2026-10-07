#!/usr/bin/env bash

COURSE_DIR="/c/Users/steve/Documents/Term4/ServiceBasedArch/Project"

# 1. Clean prior test artifacts
rm -rf ${COURSE_DIR}/jmeter/results.jtl
rm -rf ${COURSE_DIR}/jmeter/report

# 2. Print parameters (defaults: 10 threads, 5 loops)
echo "${1:-10} threads, ${2:-5} loops"

# 3. Run JMeter headless
jmeter.bat \
    -t ${COURSE_DIR}/jmeter/testplan.jmx \
    -Jthreads=${1:-10} \
    -Jloops=${2:-5} \
    -n -l ${COURSE_DIR}/jmeter/results.jtl -e -o ${COURSE_DIR}/jmeter/report