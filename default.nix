let
  pkgs = import <nixpkgs> {};
  haskellPackages = pkgs.haskellPackages;
  cabal = haskellPackages.cabal;
in cabal.mkDerivation (self: {
  pname = "kafka-conduit";
  version = "0.1.0";
  src = ./.;
  buildDepends = [
    haskellPackages.cereal
    haskellPackages.cerealConduit
    haskellPackages.conduit
    haskellPackages.conduitCombinators
    haskellPackages.conduitExtra
    haskellPackages.either
    haskellPackages.exceptions
    haskellPackages.network
    haskellPackages.stm
  ];
})