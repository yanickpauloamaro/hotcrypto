// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

pragma solidity ^0.8.10;

contract TwoFunctions {
    function panic() pure public {
        assert(false);
    }

    function do_nothing() public {}
}
