#!/bin/sh
/*/sh -c "echo Hi! I have no dependencies. > $out"; exit 0; */
derivation {
  name = "binix-test-no-deps";
  builder = ./no-deps.nix;
  system = "x86_64-linux";
}
