//! TiKV test fixture: one PD + one TiKV container on a shared network.
//!
//! Mirrors the connector suites' `try_start` soft-skip: no Docker daemon → a
//! printed `SKIP:` and `None`; containers up but the store unreachable → a
//! real error.
//!
//! Every resource name is per-test-unique (container names, network, key
//! prefixes), so nextest can run these tests in parallel: Docker container
//! names are daemon-global, and a shared name would make every test but the
//! first collide.

use std::net::TcpListener;
use std::time::Duration;

use testcontainers::core::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use uuid::Uuid;

/// A running PD + TiKV pair, reachable from the host through published ports.
pub struct TikvFixture {
    /// `host:port` of PD as the host sees it (the raw-client endpoint).
    pub pd_endpoint: String,
    /// Held so the containers live as long as the test.
    _pd: ContainerAsync<GenericImage>,
    _tikv: ContainerAsync<GenericImage>,
}

impl TikvFixture {
    /// A key prefix unique to this test run.
    pub fn prefix(&self, stem: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos();
        format!("{stem}-{nanos}")
    }
}

/// Start a PD + TiKV pair, or return `None` with a printed reason when Docker
/// is unavailable. Containers up but the store unreachable is a hard error.
pub async fn try_start() -> Option<TikvFixture> {
    match start().await {
        Ok(fx) => Some(fx),
        Err(e) => {
            eprintln!("SKIP: tikv container unavailable: {e}");
            None
        }
    }
}

/// Reserve a free host port.
fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

async fn start() -> anyhow::Result<TikvFixture> {
    // Advertise addresses must be host-reachable: the raw client connects to
    // whatever PD/TiKV advertise, so bake the chosen host ports in.
    let pd_host_port = free_port();
    let tikv_host_port = free_port();
    let suffix = Uuid::now_v7().simple().to_string();
    let network = format!("pcs-tikv-{suffix}");
    let pd_name = format!("pd-{suffix}");
    let tikv_name = format!("tikv-{suffix}");

    // PD's advertise-client-urls must be reachable from BOTH sides: the host
    // client (which connects through the published port) and the TiKV
    // container (which resolves the pd container name on the shared network).
    // v8.1.0 also refuses to start unless --advertise-peer-urls matches
    // --initial-cluster.
    let pd_image = GenericImage::new("pingcap/pd", "v8.1.0")
        .with_container_name(pd_name.clone())
        .with_network(network.clone())
        .with_mapped_port(pd_host_port, 2379.tcp())
        .with_cmd([
            "--name=pd",
            "--data-dir=/data",
            "--client-urls=http://0.0.0.0:2379",
            &format!(
                "--advertise-client-urls=http://{pd_name}:2379,http://127.0.0.1:{pd_host_port}"
            ),
            "--peer-urls=http://0.0.0.0:2380",
            "--advertise-peer-urls=http://127.0.0.1:2380",
            "--initial-cluster=pd=http://127.0.0.1:2380",
        ]);
    let pd = pd_image.start().await?;

    let tikv_image = GenericImage::new("pingcap/tikv", "v8.1.0")
        .with_container_name(tikv_name)
        .with_network(network.clone())
        .with_mapped_port(tikv_host_port, 20160.tcp())
        .with_cmd([
            &format!("--pd={pd_name}:2379"),
            "--addr=0.0.0.0:20160",
            &format!("--advertise-addr=127.0.0.1:{tikv_host_port}"),
            "--data-dir=/data",
        ]);
    let tikv = tikv_image.start().await?;

    let pd_endpoint = format!("127.0.0.1:{pd_host_port}");
    wait_until_ready(&pd_endpoint).await?;

    Ok(TikvFixture {
        pd_endpoint,
        _pd: pd,
        _tikv: tikv,
    })
}

/// Poll a raw put/get round-trip until it works (60 s budget).
///
/// This is the real readiness gate; the log-based `WaitFor` is unreliable for
/// these images (the ready line can race testcontainers' log buffer), so
/// startup waits rely on a gentle message wait and the probe below.
async fn wait_until_ready(pd_endpoint: &str) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let probe_key = format!("pcs-fixture-probe/{}", Uuid::now_v7().simple());
    loop {
        match tikv_client::RawClient::new(vec![pd_endpoint.to_string()]).await {
            Ok(client) => {
                let put = client.put(probe_key.clone(), b"ok".to_vec()).await;
                match put {
                    Ok(()) => match client.get(probe_key.clone()).await {
                        Ok(Some(value)) if value == b"ok" => return Ok(()),
                        Ok(_) => {}
                        Err(e) => eprintln!("tikv probe error (retrying): {e}"),
                    },
                    Err(e) => eprintln!("tikv probe error (retrying): {e}"),
                }
            }
            Err(e) => eprintln!("tikv connect error (retrying): {e}"),
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "PD/TiKV did not become ready within 60s (endpoint {pd_endpoint}); \
                 check the container logs"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
