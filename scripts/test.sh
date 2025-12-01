#!/bin/bash
# Comprehensive test runner for rdgDB

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
RUN_UNIT=true
RUN_INTEGRATION=false
RUN_BENCH=false
RUN_RACE=false
COVERAGE=false
VERBOSE=false

function usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --all             Run all tests (unit + integration)"
    echo "  --unit            Run unit tests only (default)"
    echo "  --integration     Run integration tests"
    echo "  --bench           Run benchmark tests"
    echo "  --race            Run with race detector"
    echo "  --coverage        Generate coverage report"
    echo "  --verbose, -v     Verbose output"
    echo "  --help, -h        Show this help"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run unit tests"
    echo "  $0 --all --coverage   # Run all tests with coverage"
    echo "  $0 --race --verbose   # Run with race detector, verbose"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            RUN_INTEGRATION=true
            ;;
        --unit)
            RUN_UNIT=true
            RUN_INTEGRATION=false
            ;;
        --integration)
            RUN_INTEGRATION=true
            ;;
        --bench)
            RUN_BENCH=true
            ;;
        --race)
            RUN_RACE=true
            ;;
        --coverage)
            COVERAGE=true
            ;;
        -v|--verbose)
            VERBOSE=true
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
    shift
done

# Build test flags
TEST_FLAGS=""
if [ "$VERBOSE" = true ]; then
    TEST_FLAGS="$TEST_FLAGS -v"
fi
if [ "$RUN_RACE" = true ]; then
    TEST_FLAGS="$TEST_FLAGS -race"
fi

echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BLUE}  rdgDB Test Suite${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"

# Run unit tests
if [ "$RUN_UNIT" = true ]; then
    echo -e "\n${YELLOW}Running unit tests...${NC}"
    
    if [ "$COVERAGE" = true ]; then
        go test $TEST_FLAGS -coverprofile=coverage.out ./...
        echo -e "\n${YELLOW}Coverage report:${NC}"
        go tool cover -func=coverage.out | tail -n 1
        echo -e "${YELLOW}HTML report: coverage.html${NC}"
        go tool cover -html=coverage.out -o coverage.html
    else
        go test $TEST_FLAGS ./...
    fi
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Unit tests passed${NC}"
    else
        echo -e "${RED}✗ Unit tests failed${NC}"
        exit 1
    fi
fi

# Run integration tests
if [ "$RUN_INTEGRATION" = true ]; then
    echo -e "\n${YELLOW}Running integration tests...${NC}"
    
    if [ -d "tests/integration" ]; then
        go test $TEST_FLAGS -tags=integration ./tests/integration/...
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Integration tests passed${NC}"
        else
            echo -e "${RED}✗ Integration tests failed${NC}"
            exit 1
        fi
    else
        echo -e "${YELLOW}No integration tests found (tests/integration/)${NC}"
    fi
fi

# Run benchmarks
if [ "$RUN_BENCH" = true ]; then
    echo -e "\n${YELLOW}Running benchmarks...${NC}"
    go test -bench=. -benchmem ./... | tee bench.txt
    echo -e "${YELLOW}Benchmark results saved to bench.txt${NC}"
fi

echo -e "\n${GREEN}════════════════════════════════════════${NC}"
echo -e "${GREEN}  All tests completed successfully!${NC}"
echo -e "${GREEN}════════════════════════════════════════${NC}"
