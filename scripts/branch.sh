#!/bin/bash
# Feature branch management script for rdgDB

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function usage() {
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  create <branch-name>    Create a new feature branch"
    echo "  test                    Run tests before merge"
    echo "  merge <branch-name>     Merge feature branch after tests pass"
    echo "  list                    List all feature branches"
    echo "  delete <branch-name>    Delete a feature branch"
    echo ""
    echo "Example:"
    echo "  $0 create feature/phase-1-storage"
    echo "  $0 test"
    echo "  $0 merge feature/phase-1-storage"
    exit 1
}

function create_branch() {
    local branch_name=$1
    if [ -z "$branch_name" ]; then
        echo -e "${RED}Error: Branch name required${NC}"
        usage
    fi
    
    echo -e "${YELLOW}Creating feature branch: $branch_name${NC}"
    git checkout -b "$branch_name"
    echo -e "${GREEN}✓ Branch created successfully${NC}"
    echo -e "${YELLOW}Remember to run './scripts/test.sh' before merging!${NC}"
}

function run_tests() {
    echo -e "${YELLOW}Running test suite...${NC}"
    ./scripts/test.sh
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}✗ Tests failed. Fix issues before merging.${NC}"
        return 1
    fi
}

function merge_branch() {
    local branch_name=$1
    if [ -z "$branch_name" ]; then
        echo -e "${RED}Error: Branch name required${NC}"
        usage
    fi
    
    echo -e "${YELLOW}Testing before merge...${NC}"
    if ! run_tests; then
        echo -e "${RED}Cannot merge: tests failing${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}Merging $branch_name into main...${NC}"
    current_branch=$(git branch --show-current)
    
    if [ "$current_branch" != "$branch_name" ]; then
        echo -e "${RED}Error: Not on branch $branch_name${NC}"
        echo "Current branch: $current_branch"
        exit 1
    fi
    
    git checkout main
    git merge --no-ff "$branch_name" -m "Merge $branch_name"
    
    echo -e "${GREEN}✓ Branch merged successfully${NC}"
    echo -e "${YELLOW}Delete feature branch with: $0 delete $branch_name${NC}"
}

function list_branches() {
    echo -e "${YELLOW}Feature branches:${NC}"
    git branch | grep -E 'feature/|phase-' || echo "No feature branches found"
}

function delete_branch() {
    local branch_name=$1
    if [ -z "$branch_name" ]; then
        echo -e "${RED}Error: Branch name required${NC}"
        usage
    fi
    
    echo -e "${YELLOW}Deleting branch: $branch_name${NC}"
    git branch -d "$branch_name"
    echo -e "${GREEN}✓ Branch deleted${NC}"
}

# Main command dispatch
case "${1:-}" in
    create)
        create_branch "$2"
        ;;
    test)
        run_tests
        ;;
    merge)
        merge_branch "$2"
        ;;
    list)
        list_branches
        ;;
    delete)
        delete_branch "$2"
        ;;
    *)
        usage
        ;;
esac
