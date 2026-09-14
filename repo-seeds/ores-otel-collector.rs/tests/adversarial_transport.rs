use ores_otel_collector::validate_upstream;

#[test]
fn clean_https_upstream_is_accepted() {
    let url = validate_upstream("https://collector.example/v1/").expect("https upstream");
    assert_eq!(url.scheme(), "https");
}

#[test]
fn loopback_http_variants_are_accepted() {
    for raw in [
        "http://localhost:4319/",
        "http://127.0.0.1:4319/",
        "http://[::1]:4319/",
    ] {
        assert!(validate_upstream(raw).is_ok(), "rejected loopback {raw}");
    }
}

#[test]
fn non_loopback_plaintext_upstream_is_rejected() {
    for raw in [
        "http://collector.example/",
        "http://192.0.2.10:4319/",
        "http://10.0.0.10:4319/",
    ] {
        assert!(validate_upstream(raw).is_err(), "accepted plaintext {raw}");
    }
}

#[test]
fn all_userinfo_forms_are_rejected() {
    for raw in [
        "https://user@collector.example/",
        "https://user:password@collector.example/",
    ] {
        assert!(validate_upstream(raw).is_err(), "accepted userinfo {raw}");
    }
}

#[test]
fn query_fragment_and_non_http_schemes_are_rejected() {
    for raw in [
        "https://collector.example/?token=secret",
        "https://collector.example/#secret",
        "ftp://collector.example/",
        "file:///tmp/collector",
    ] {
        assert!(
            validate_upstream(raw).is_err(),
            "accepted unsafe upstream {raw}"
        );
    }
}
