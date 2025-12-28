#!/bin/bash



###############################################################################
# Wrapper script for setting up and running rsyncdirector tests
#
# name:     run-tests.sh
# author:   Ryan Chapin
#
################################################################################

################################################################################
#
#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  export_env_vars
#   DESCRIPTION:  Will export all of the required environmental variables to run
#                 the tests
#    PARAMETERS:  None
#       RETURNS:  void
#-------------------------------------------------------------------------------
function export_env_vars {
  set -e

  local override_env_vars=$1
  if [ "$override_env_vars" != "0" ]
  then
    if [ ! -f "$override_env_vars" ]
    then
      printf "ERROR! OVERRIDE_ENV_VARS_PATH, -e argument, pointed to a non-existent file\n\n" >&2
      usage
      #
      # We don't just exit here, because we may be sourcing this script and it would
      # then close the current terminal without giving the user an opportunity to
      # see the error message.  If we were not passed in a path to a valid file for
      # the required env vars, we just fall through and exit without doing anything
      # else.
      #
    fi
    echo "Sourcing required env vars script $OVERRIDE_ENV_VARS_PATH"
    source $OVERRIDE_ENV_VARS_PATH
  fi

  # ------------------------------------------------------------------------------
  # Sane defaults.  Can be overriden by first specifying the variable you want to
  # change as an environmental variable in the calling shell.

  export RSYNCDIRECTORINTTEST_PYTHON=${RSYNCDIRECTORINTTEST_PYTHON:-$(which python3.13)}

  export RSYNCDIRECTORINTTEST_TEST_HOST=${RSYNCDIRECTORINTTEST_TEST_HOST:-localhost}

  # Parent directory for all of the integration test files and directories
  export RSYNCDIRECTORINTTEST_PARENT_DIR=${RSYNCDIRECTORINTTEST_PARENT_DIR:-/var/tmp/rsyncdirector-integration-test}
  export RSYNCDIRECTORINTTEST_CONFIG_DIR=${RSYNCDIRECTORINTTEST_CONFIG_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/config}
  export RSYNCDIRECTORINTTEST_CONFIG_FILE=${RSYNCDIRECTORINTTEST_CONFIG_FILE:-rsyncdirector.yaml}
  export RSYNCDIRECTORINTTEST_LOCK_DIR=${RSYNCDIRECTORINTTEST_LOCK_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/lock}
  export RSYNCDIRECTORINTTEST_BLOCK_DIR=${RSYNCDIRECTORINTTEST_BLOCK_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/block}
  export RSYNCDIRECTORINTTEST_PID_DIR=${RSYNCDIRECTORINTTEST_PID_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/pid}
  export RSYNCDIRECTORINTTEST_TEST_DATA_DIR=${RSYNCDIRECTORINTTEST_TEST_DATA_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/test_data}
  export RSYNCDIRECTORINTTEST_TEST_LOCAL_SYNC_TARGET_DIR=${RSYNCDIRECTORINTTEST_TEST_LOCAL_SYNC_TARGET_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/test_local_sync_target}
  export RSYNCDIRECTORINTTEST_DOCKER_DIR=${RSYNCDIRECTORINTTEST_DOCKER_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/docker}
  export RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE=${RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE:-$RSYNCDIRECTORINTTEST_DOCKER_DIR/id_rsa}
  export RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE_PUB=${RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE_PUB:-${RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE}.pub}
  export RSYNCDIRECTORINTTEST_VIRTENV_DIR=${RSYNCDIRECTORINTTEST_VIRTENV_DIR:-$RSYNCDIRECTORINTTEST_PARENT_DIR/virtenv}
  # A container to which we will test rsyncing to "remotely".
  export RSYNCDIRECTORINTTEST_CONTAINER_TARGET_NAME=${RSYNCDIRECTORINTTEST_CONTAINER_TARGET_NAME:-rsyncdirector_inttest_target}
  # A "remote" container that we will use to check for "remote" lock files.
  export RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_NAME=${RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_NAME:-rsyncdirector_inttest_remote}
  export RSYNCDIRECTORINTTEST_IMAGE_NAME=${RSYNCDIRECTORINTTEST_IMAGE_NAME:-rsyncdirector_inttest}
  export RSYNCDIRECTORINTTEST_CONTAINER_TARGET_PORT=${RSYNCDIRECTORINTTEST_CONTAINER_TARGET_PORT:-22222}
  export RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_PORT=${RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_PORT:-22223}

  # It doesn't really matter what this password is. We just need something
  # with which we can ssh/rsync to the container to execute the tests
  export RSYNCDIRECTORINTTEST_CONTAINER_ROOT_PASSWD=${RSYNCDIRECTORINTTEST_CONTAINER_ROOT_PASSWD:-password123}
  export RSYNCDIRECTORINTTEST_CONTAINER_ROOT_PASSWD_FILE=${RSYNCDIRECTORINTTEST_CONTAINER_ROOT_PASSWD_FILE:-$RSYNCDIRECTORINTTEST_PARENT_DIR/test-container-root-passwd.txt}

  export RSYNCDIRECTORINTTEST_WAITFOR_TIMEOUT_SECONDS=${RSYNCDIRECTORINTTEST_WAITFOR_TIMEOUT_SECONDS:-10}
  export RSYNCDIRECTORINTTEST_WAITFOR_POLL_INTERVAL=${RSYNCDIRECTORINTTEST_WAITFOR_POLL_INTERVAL:-0.5}

  export RSYNCDIRECTORINTTEST_METRICS_SCRAPER_TARGET_ADDR=${RSYNCDIRECTORINTTEST_METRICS_SCRAPER_TARGET_ADDR:-localhost}
  export RSYNCDIRECTORINTTEST_METRICS_SCRAPER_TARGET_PORT=${RSYNCDIRECTORINTTEST_METRICS_SCRAPER_TARGET_PORT:-9999}

  export RSYNCDIRECTORINTTEST_WHICH_PATH=${RSYNCDIRECTORINTTEST_WHICH_PATH:-/usr/bin/which}

  set +e
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  which_linux_distro
#   DESCRIPTION:  Returns the enum/name of the linux distro on which we are
#                 running the tests
#-------------------------------------------------------------------------------
function which_linux_distro {
  local retval=""

  if [ -f "/etc/debian_version" ]; then
    retval="debian"
  fi
  # TODO add RHEL

  echo $retval
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  install_dependencies
#   DESCRIPTION:  Installs required packages to setup and run the tests.
#-------------------------------------------------------------------------------
function install_dependencies {
  distro=$(which_linux_distro)
  case $distro in

    debian)
      ssh root@localhost apt-get install -y netcat-traditional python3-venv rsync sshpass
      ;;

    redhat)
      # TODO
      echo "redhat"
      ;;

    *)
      echo -n "unknown"
      ;;

  esac
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  build_docker_test_image
#   DESCRIPTION:  Builds the docker image which we will use to run the tests.
#-------------------------------------------------------------------------------
function build_docker_test_image {
  local start_dir=$(pwd)

  # Generate an ssh key to be added to the docker image when we build it.
  ssh-keygen -q -t rsa -N '' -f $RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE <<<y 2>&1 >/dev/null

  # Copy the docker file to the "build" dir and build the docker image
  cp rsyncdirector/integration_tests/docker/Dockerfile $RSYNCDIRECTORINTTEST_DOCKER_DIR
  cd $RSYNCDIRECTORINTTEST_DOCKER_DIR
  docker build --build-arg root_passwd=$RSYNCDIRECTORINTTEST_CONTAINER_ROOT_PASSWD -t $RSYNCDIRECTORINTTEST_IMAGE_NAME . --network=host

  # Write out the password to a text file
  echo "$RSYNCDIRECTORINTTEST_CONTAINER_ROOT_PASSWD" > $RSYNCDIRECTORINTTEST_CONTAINER_ROOT_PASSWD_FILE

  cd $start_dir
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  start_docker_containers
#   DESCRIPTION:  Start the docker container which we will use to run the tests.
#-------------------------------------------------------------------------------
function start_docker_containers {

  # Fire up the target docker container
  docker run --rm -d --name $RSYNCDIRECTORINTTEST_CONTAINER_TARGET_NAME -p ${RSYNCDIRECTORINTTEST_CONTAINER_TARGET_PORT}:22 $RSYNCDIRECTORINTTEST_IMAGE_NAME
  docker run --rm -d --name $RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_NAME -p ${RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_PORT}:22 $RSYNCDIRECTORINTTEST_IMAGE_NAME

  # Because we are likely going to run this multiple times and idempotency is
  # king, we want to ensure that we do not already have a set of keys for
  # the containers.
  ssh-keygen -f "$HOME/.ssh/known_hosts" -R "[localhost]:$RSYNCDIRECTORINTTEST_CONTAINER_TARGET_PORT"
  ssh-keygen -f "$HOME/.ssh/known_hosts" -R "[localhost]:$RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_PORT"

  # ssh to the docker containers automatically accepting the host keys
  ssh -p $RSYNCDIRECTORINTTEST_CONTAINER_TARGET_PORT -i $RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE -o StrictHostKeyChecking=no root@localhost hostname
  ssh -p $RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_PORT -i $RSYNCDIRECTORINTTEST_SSH_IDENTITY_FILE -o StrictHostKeyChecking=no root@localhost hostname
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  configure_firewall
#   DESCRIPTION:  Configures the firewall on the test machine if it is already
#                 installed and enabled.  If not, it is a noop.
#-------------------------------------------------------------------------------
function configure_firewall {
  distro=$(which_linux_distro)
  case $distro in

    debian)
      if dpkg --get-selections | grep ufw 2>&1 > /dev/null
      then
        # Check to see if it is active
        if ! ssh root@localhost ufw status | grep inactive > /dev/null
        then
          for i in $RSYNCDIRECTORINTTEST_CONTAINER_TARGET_PORT $RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_PORT
          do
            echo "Adding $i to ufw firewall"
            ssh root@localhost ufw allow ${i}/tcp
          done
        fi
      fi
      ;;

    redhat)
      echo "redhat"
      ;;

    *)
      echo -n "unknown"
      ;;

  esac
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  create_virtenv
#   DESCRIPTION:  Create the virtual environment and install the application.
#-------------------------------------------------------------------------------
function create_virtenv {
  local env_type=$1
  local virt_env_dir=$RSYNCDIRECTORINTTEST_VIRTENV_DIR

  if [ "$env_type" == "dev" ];
  then
    virt_env_dir=~/.virtualenvs/rsyncdirector
  fi

  $RSYNCDIRECTORINTTEST_PYTHON -mvenv $virt_env_dir
  source $virt_env_dir/bin/activate
  pip install -U setuptools pip
  pip install .
  pip install .[test]

  if [ "$env_type" == "dev" ];
  then
    pip install .[dev]
  fi
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  create_test_env_file
#   DESCRIPTION:  Create the env file required for running tests via VSCode
#-------------------------------------------------------------------------------
function create_test_env_file {
  OUTFILE=.env
  echo > $OUTFILE
  env | grep RSYNCDIRECTORINTTEST_ | sort >> $OUTFILE
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  dev_setup
#   DESCRIPTION:  Cleans and creates the required test dirs based on the env
#                 vars already defined and creates a developer virtual env.
#-------------------------------------------------------------------------------
function dev_setup {
  setup "dev"
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  setup
#   DESCRIPTION:  Cleans and creates the required test dirs based on the env
#                 vars already defined.
#-------------------------------------------------------------------------------
function setup {
  local env_type=$1

  # First run teardown to remove anything left behind
  teardown

  echo "Setting up test environment"
  # Now create the directory structure needed for the tests.
  dirs=(
    "$RSYNCDIRECTORINTTEST_PARENT_DIR"
    "$RSYNCDIRECTORINTTEST_CONFIG_DIR"
    "$RSYNCDIRECTORINTTEST_DOCKER_DIR"
  )
  for dir in "${dirs[@]}"
  do
    mkdir -p $dir
  done

  install_dependencies
  configure_firewall
  create_test_env_file
  build_docker_test_image
  start_docker_containers
  create_virtenv $env_type
  echo "Test environment setup complete"
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  teardown
#   DESCRIPTION:  Cleans up the required test dirs based on the env vars already
#                 defined.
#-------------------------------------------------------------------------------
function teardown {
  echo "Tearing down test environment"
  echo "Deleting test dirs"
  rm -rf $RSYNCDIRECTORINTTEST_PARENT_DIR

  # It is possible that there is no container or images in existence, but we
  # will stop any running container and delete the image to ensure a clean
  # slate.
  echo "Stopping docker container and deleting test image"
  set +e
  docker stop $RSYNCDIRECTORINTTEST_CONTAINER_TARGET_NAME 2> /dev/null
  docker stop $RSYNCDIRECTORINTTEST_CONTAINER_REMOTE_NAME 2> /dev/null
  docker rmi $RSYNCDIRECTORINTTEST_IMAGE_NAME 2> /dev/null
  set -e

  echo "Test environment clean-up complete"
}

#---  FUNCTION  ----------------------------------------------------------------
#          NAME:  run_tests
#   DESCRIPTION:  Runs both the unit and integration tests.
#    PARAMETERS:  None
#       RETURNS:  void
#-------------------------------------------------------------------------------

function run_tests {
  set -e
  source $RSYNCDIRECTORINTTEST_VIRTENV_DIR/bin/activate
  export PYTHONTRACEMALLOC=1

  # Uncomment when we start building unit tests
  # Run the unit tests
  # echo "======================================================================="
  # echo "Running the unit tests"
  # coverage run -m unittest discover -s rsyncdirector/tests # add -k $test_name

  if [ "$OMIT_INTEGRATION_TESTS" -ne 1 ]
  then
    echo "======================================================================="
    echo "Running the integration tests"
    coverage run --append -m unittest discover -s rsyncdirector/integration_tests --failfast # add -k $test_name
  fi

  coverage report --include="rsyncdirector/*"
  coverage html
  set +e
}

################################################################################
# USAGE:

function usage {
   cat << EOF
Usage: run-tests.sh [OPTIONS]

Options:

  -e OVERRIDE_ENV_VARS_PATH
     Path to the file that contains the any overriding env vars.

  -i OMIT_INTEGRATION_TESTS
     Ommit running the integration tests and just run the unit tests.

  -l LEAVE
     Do not clean any existing environment previously setup.  By default the
     environment is cleaned and re-installed with each invocation of this
     script.

  -t TEARDOWN
     Teardown the test environment on the configured test host.

  --dev-setup Only run the setup for a dev environment without running the tests.

  --setup-only Only run the setup without running the tests.

  --export-env-vars-only
    Only export the require environmental variables for the test, overriding
    the defaults with those env vars defined in the -e file, but do not run the
    test.  To achieve this goal, you must source this script instead of running
    it as an executable script.

    Example:

    $ source ./run-tests.sh -e /path/to/required-env-vars.sh --export-env-vars-only

    Alternatively, you can omit the -e arg to use the defaults.

  -h HELP
     Outputs this basic usage information.
EOF
}

################################################################################
#
# Here we define variables to store the input from the command line arguments as
# well as define the default values.
#
HELP=0
LEAVE=0
TEARDOWN=0
TEARDOWN_ONLY=0
SETUP_ONLY=0
DEV_SETUP=0
EXPORT_ENV_VARS_ONLY=0
OVERRIDE_ENV_VARS_PATH=0
OMIT_INTEGRATION_TESTS=0

PARSED_OPTIONS=`getopt -o hltie: -l export-env-vars-only,dev-setup,setup-only,teardown-only -- "$@"`

# Check to see if the getopts command failed
if [ $? -ne 0 ];
then
   echo "Failed to parse arguments"
   exit 1
fi

eval set -- "$PARSED_OPTIONS"

# Loop through all of the options with a case statement
while true; do
   case "$1" in
      -h)
         HELP=1
         shift
         ;;

      -e)
         OVERRIDE_ENV_VARS_PATH=$2
         shift 2
         ;;

      -i)
         OMIT_INTEGRATION_TESTS=1
         shift
         ;;

      -l)
         LEAVE=1
         shift
         ;;

      -t)
         TEARDOWN=1
         shift
         ;;

      --export-env-vars-only)
         EXPORT_ENV_VARS_ONLY=1
         shift
         ;;

      --dev-setup)
         DEV_SETUP=1
         shift
         ;;

      --setup-only)
         SETUP_ONLY=1
         shift
         ;;

      --teardown-only)
         TEARDOWN_ONLY=1
         shift
         ;;


      --)
         shift
         break
         ;;
   esac
done

if [ "$HELP" -eq 1 ];
then
   usage
   exit
fi

################################################################################

export_env_vars $OVERRIDE_ENV_VARS_PATH
run_script_dir=$(cd $(dirname $0) && pwd)

if [ "$EXPORT_ENV_VARS_ONLY" -eq 1 ]
then
  #
  # Check to make sure that the user actually sourced this script otherwise we
  # can give them a warning so that they won't be confused when they do not
  # see any of the expected env vars.
  #
  current_dirname=`dirname "$0"`
  if [[ "$current_dirname" != *"/bin"* ]]
  then
    cat << EOF

!!!!! WARNING !!!!!
You are trying to just export the env vars, however, it seems as though you did not source this script, but were attempting to execute it.
EOF
    usage
  fi

else
  #
  # If we are to do more than export env vars, continue processing
  #
  if [ "$TEARDOWN_ONLY" -eq 1 ]
  then
    teardown
    exit
  fi

  if [ "$DEV_SETUP" -eq 1 ]
  then
    setup "dev"
    exit
  fi

  if [ "$SETUP_ONLY" -eq 1 ]
  then
    setup "test"
    exit
  fi

  if [ "$LEAVE" -eq 0 ]
  then
    # We are to run setup which will clean any existing test environment
    setup
  fi

  time run_tests

  if [ "$TEARDOWN" -eq 1 ]
  then
    #
    # We should teardown the test setup environment
    #
    teardown
  fi
fi

