/// The distributor of this Binix client.
///
/// Common values include `nixpkgs`, `binix` and `dev`.
pub const BINIX_DISTRIBUTOR: &str = if let Some(distro) = option_env!("BINIX_DISTRIBUTOR") {
    distro
} else {
    "unknown"
};
