#[test]
fn arsenal_depends_on_core_not_root_fusillade() {
    let manifest = include_str!("../crates/fusillade-arsenal/Cargo.toml");

    assert!(
        manifest.contains("fusillade-core"),
        "fusillade-arsenal should depend on fusillade-core for shared types"
    );
    assert!(
        !manifest
            .lines()
            .any(|line| line.trim_start().starts_with("fusillade =")),
        "fusillade-arsenal must not depend on the root fusillade daemon crate"
    );
}

#[test]
fn workspace_contains_the_three_release_packages() {
    let manifest = include_str!("../Cargo.toml");

    for package in ["crates/fusillade-core", "crates/fusillade-arsenal"] {
        assert!(
            manifest.contains(package),
            "workspace manifest should include {package}"
        );
    }
}
