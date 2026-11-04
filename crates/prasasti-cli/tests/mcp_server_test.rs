use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use tempfile::tempdir;

fn send_rpc(
    stdin: &mut impl Write,
    reader: &mut BufReader<impl std::io::Read>,
    req: &Value,
) -> Value {
    writeln!(stdin, "{}", serde_json::to_string(req).unwrap()).unwrap();
    stdin.flush().unwrap();
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .expect("must read response line");
    assert!(n > 0, "MCP server closed stdout unexpectedly");
    serde_json::from_str(&line).expect("response must be valid JSON-RPC")
}

#[test]
fn req_prs_mcp_01_end_to_end_stdio_telemetry_and_circuit_breaker() {
    let bin = env!("CARGO_BIN_EXE_prasasti");
    let mut child = Command::new(bin)
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn prasasti mcp");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    // 1. Initialize handshake
    let init_resp = send_rpc(
        &mut stdin,
        &mut reader,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        }),
    );
    assert_eq!(
        init_resp["result"]["serverInfo"]["name"],
        "prasasti-mcp-server"
    );

    // 2. List tools
    let list_resp = send_rpc(
        &mut stdin,
        &mut reader,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    );
    let tools = list_resp["result"]["tools"].as_array().unwrap();
    let tool_names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(tool_names.contains(&"prasasti_record_telemetry"));
    assert!(tool_names.contains(&"prasasti_check_circuit"));
    assert!(tool_names.contains(&"prasasti_audit_self"));

    // 3. Path traversal rejection gate
    let traversal_resp = send_rpc(
        &mut stdin,
        &mut reader,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "prasasti_record_telemetry",
                "arguments": {
                    "data_dir": "../escaped_dir",
                    "symbol": "bash_exec",
                    "latency_ms": 120.0,
                    "tokens": 500.0
                }
            }
        }),
    );
    assert_eq!(traversal_resp["result"]["isError"], true);
    let err_txt = traversal_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(err_txt.contains("path traversal"));

    // 4. Record 3 telemetry events for agent tool "cargo_test_loop"
    let tmp = tempdir().unwrap();
    let dir_str = tmp.path().to_str().unwrap();

    for i in 0..3 {
        let rec_resp = send_rpc(
            &mut stdin,
            &mut reader,
            &json!({
                "jsonrpc": "2.0",
                "id": 10 + i,
                "method": "tools/call",
                "params": {
                    "name": "prasasti_record_telemetry",
                    "arguments": {
                        "data_dir": dir_str,
                        "symbol": "cargo_test_loop",
                        "latency_ms": 250.0,
                        "tokens": 1000.0,
                        "ts_ns": 1_700_000_000_000_000_000i64 + (i as i64) * 1_000_000_000
                    }
                }
            }),
        );
        assert!(rec_resp["result"]["isError"].is_null());
    }

    // 5. Check circuit — ALLOW when max_repeats = 5 and max_tokens = 5000
    let allow_resp = send_rpc(
        &mut stdin,
        &mut reader,
        &json!({
            "jsonrpc": "2.0",
            "id": 20,
            "method": "tools/call",
            "params": {
                "name": "prasasti_check_circuit",
                "arguments": {
                    "data_dir": dir_str,
                    "symbol": "cargo_test_loop",
                    "max_repeats": 5,
                    "max_tokens": 5000.0
                }
            }
        }),
    );
    let allow_payload: Value =
        serde_json::from_str(allow_resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(allow_payload["status"], "ALLOW");
    assert_eq!(allow_payload["repeat_count"], 3);
    assert_eq!(allow_payload["total_tokens"], 3000.0);

    // 6. Check circuit — TRIP_CIRCUIT: LOOP_DETECTED when max_repeats = 3
    let loop_resp = send_rpc(
        &mut stdin,
        &mut reader,
        &json!({
            "jsonrpc": "2.0",
            "id": 21,
            "method": "tools/call",
            "params": {
                "name": "prasasti_check_circuit",
                "arguments": {
                    "data_dir": dir_str,
                    "symbol": "cargo_test_loop",
                    "max_repeats": 3,
                    "max_tokens": 10000.0
                }
            }
        }),
    );
    let loop_payload: Value =
        serde_json::from_str(loop_resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(loop_payload["status"], "TRIP_CIRCUIT");
    assert_eq!(loop_payload["reason"], "LOOP_DETECTED");

    // 7. Check circuit — TRIP_CIRCUIT: QUOTA_EXCEEDED when max_tokens = 2500.0
    let quota_resp = send_rpc(
        &mut stdin,
        &mut reader,
        &json!({
            "jsonrpc": "2.0",
            "id": 22,
            "method": "tools/call",
            "params": {
                "name": "prasasti_check_circuit",
                "arguments": {
                    "data_dir": dir_str,
                    "symbol": "cargo_test_loop",
                    "max_repeats": 10,
                    "max_tokens": 2500.0
                }
            }
        }),
    );
    let quota_payload: Value =
        serde_json::from_str(quota_resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(quota_payload["status"], "TRIP_CIRCUIT");
    assert_eq!(quota_payload["reason"], "QUOTA_EXCEEDED");

    // 8. Run prasasti_audit_self over MCP
    let audit_resp = send_rpc(
        &mut stdin,
        &mut reader,
        &json!({
            "jsonrpc": "2.0",
            "id": 30,
            "method": "tools/call",
            "params": {
                "name": "prasasti_audit_self",
                "arguments": {}
            }
        }),
    );
    let audit_txt = audit_resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(audit_txt.contains("ALL 4 PRASASTI DURABILITY & ANALYTICAL GATES PASSED."));

    drop(stdin);
    let _ = child.wait();
}
