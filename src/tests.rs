#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::logic::process_file;

    #[tokio::test]
    async fn test_non_existent_url_reports_get_error() {
        let input = "www.non-existent.com\n";
        let output = run_process_file(input, 1).await;

        assert!(output.contains("www.non-existent.com"));
        assert!(output.contains("Error in GET request"));
    }

    #[tokio::test]
    async fn test_no_urls_in_input_results_in_no_output() {
        let input = "This line has no URLs.\n";
        let output = run_process_file(input, 1).await;

        assert_eq!(output, "");
    }

    #[tokio::test]
    async fn test_single_url_extracts_title() {
        let mut responses = HashMap::new();
        responses.insert("/title".to_owned(), html("Example Title"));
        let (base_url, server) = spawn_http_server(responses, 1).await;

        let input = format!("{}/title\n", base_url);
        let output = run_process_file(&input, 1).await;

        server.await.unwrap();

        let expected = format!("[Example Title]({}/title)", base_url);
        assert_eq!(output, format!("{}\n", expected));
    }

    #[tokio::test]
    async fn test_multiple_urls_are_extracted_from_one_line() {
        let mut responses = HashMap::new();
        responses.insert("/first".to_owned(), html("First Title"));
        responses.insert("/second".to_owned(), html("Second Title"));
        let (base_url, server) = spawn_http_server(responses, 2).await;

        let input = format!(
            "Read ({}/first) and also {}/second on the same line.\n",
            base_url, base_url
        );
        let output = run_process_file(&input, 2).await;

        server.await.unwrap();

        assert_contains_all_lines(
            &output,
            &[
                &format!("[First Title]({}/first)", base_url),
                &format!("[Second Title]({}/second)", base_url),
            ],
        );
    }

    #[tokio::test]
    async fn test_missing_title_uses_default_text() {
        let mut responses = HashMap::new();
        responses.insert("/plain".to_owned(), b"No title here".to_vec());
        let (base_url, server) = spawn_http_server(responses, 1).await;

        let input = format!("{}/plain\n", base_url);
        let output = run_process_file(&input, 1).await;

        server.await.unwrap();

        assert_eq!(output, format!("[No title found]({}/plain)\n", base_url));
    }

    #[tokio::test]
    async fn test_invalid_utf8_response_reports_error() {
        let mut responses = HashMap::new();
        responses.insert("/invalid".to_owned(), vec![0xff, 0xfe, 0xfd]);
        let (base_url, server) = spawn_http_server(responses, 1).await;

        let input = format!("{}/invalid\n", base_url);
        let output = run_process_file(&input, 1).await;

        server.await.unwrap();

        assert!(output.contains(&format!("({}/invalid)", base_url)));
        assert!(output.contains("No title found"));
    }

    fn html(title: &str) -> Vec<u8> {
        format!(
            "<html><head><title>{}</title></head><body>Hello</body></html>",
            title
        )
        .into_bytes()
    }

    async fn run_process_file(input: &str, number_of_workers: usize) -> String {
        let mut output = vec![];

        process_file(input.as_bytes(), &mut output, number_of_workers).await;

        String::from_utf8(output).unwrap()
    }

    fn assert_contains_all_lines(output: &str, expected_lines: &[&str]) {
        let actual_lines: Vec<&str> = output.lines().collect();

        assert_eq!(actual_lines.len(), expected_lines.len());
        for expected_line in expected_lines {
            assert!(
                actual_lines.contains(expected_line),
                "missing line: {expected_line}"
            );
        }
    }

    async fn spawn_http_server(
        responses: HashMap<String, Vec<u8>>,
        expected_requests: usize,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            for _ in 0..expected_requests {
                let (mut socket, _) = listener.accept().await.unwrap();

                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let bytes_read = socket.read(&mut buffer).await.unwrap();
                    if bytes_read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..bytes_read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }

                let request_line = request
                    .split(|byte| *byte == b'\n')
                    .next()
                    .and_then(|line| std::str::from_utf8(line).ok())
                    .unwrap_or("");
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("/")
                    .to_owned();
                let body = responses
                    .get(&path)
                    .cloned()
                    .unwrap_or_else(|| b"<html><body>not found</body></html>".to_vec());
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n",
                    body.len()
                );

                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });

        (format!("http://{}", address), handle)
    }
}
