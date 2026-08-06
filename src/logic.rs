use futures::future::join_all;
use std::sync::Arc;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::join;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub async fn process_file(
    input: impl AsyncBufRead + Unpin,
    output: impl AsyncWrite + Unpin,
    number_of_workers: usize,
) {
    // spawn input file reader
    let (url_sender, url_receiver) = mpsc::channel(100);
    let input_task = collect_urls(input, url_sender);

    // spawn url requesters
    let (titles_sender, titles_receiver) = mpsc::channel(100);
    let mut query_tasks = vec![];
    let url_receiver = Arc::new(Mutex::new(url_receiver));
    for _ in 0..number_of_workers {
        query_tasks.push(request_urls(url_receiver.clone(), titles_sender.clone()));
    }
    // must drop the original sender to close the channel when all workers are done
    drop(titles_sender);

    // spawn output file writer
    let output_task = write_output(output, titles_receiver);

    // await for tasks to finish
    join!(input_task, join_all(query_tasks), output_task);
}

pub async fn collect_urls(mut input: impl AsyncBufRead + Unpin, url_sender: Sender<String>) {
    // in each line, search for one or many urls
    loop {
        let mut line = String::new();
        let Ok(s) = input
            .read_line(&mut line)
            .await
            .inspect_err(|e| eprintln!("error reading input: {}", e))
        else {
            continue;
        };
        if s == 0 {
            break;
        }
        let mut i = 0;
        // search for the start of an url
        let line = line.as_bytes();
        while i < line.len() {
            if line[i..].starts_with(b"http://")
                || line[i..].starts_with(b"https://")
                || line[i..].starts_with(b"www.")
            {
                // search for the end of the url
                let mut j = i + 1;
                while j < line.len() {
                    let is_end_character = line.get(j) == Some(&b' ') || line.get(j) == Some(&b')');
                    if is_end_character {
                        break;
                    }
                    j += 1;
                }
                // send url string to request workers
                if let Ok(s) = String::from_utf8(line[i..j].to_owned()) {
                    if url_sender.send(s).await.is_err() {
                        break;
                    }
                }
                i = j;
            }
            i += 1;
        }
    }
}

pub async fn request_urls(
    url_receiver: Arc<Mutex<Receiver<String>>>,
    titles_sender: Sender<(String, String)>,
) {
    loop {
        // receive an url from input reader
        let Some(url) = url_receiver.lock().await.recv().await else {
            break;
        };
        // get request to that url to get <title> section
        let Ok(response) = reqwest::get(&url).await else {
            if titles_sender
                .send((format!("Error in GET request").to_owned(), url))
                .await
                .is_err()
            {
                break;
            }
            continue;
        };
        let Ok(body) = response.text().await else {
            if titles_sender
                .send((format!("Error getting text from request").to_owned(), url))
                .await
                .is_err()
            {
                break;
            }
            continue;
        };
        // scan body for <title> section
        // search for the start of an url
        let mut title = "No title found".to_owned();
        if let Some((title_start, _)) = body.match_indices("<title>").next() {
            if let Some((title_end, _)) = body.match_indices("</title>").next() {
                title = body[title_start + 7..title_end].to_owned();
            }
        }
        if titles_sender.send((title, url)).await.is_err() {
            break;
        }
    }
}

pub async fn write_output(
    mut output: impl AsyncWrite + Unpin,
    mut titles_receiver: Receiver<(String, String)>,
) {
    loop {
        let Some((title, url)) = titles_receiver.recv().await else {
            break;
        };
        let title_clean = title.split_whitespace().collect::<Vec<&str>>().join(" ");
        let line = format!("[{}]({})\n", title_clean, url.trim());
        if let Err(e) = output.write_all(line.as_bytes()).await {
            eprintln!("error writing output: {}", e)
        }
        if let Err(e) = output.flush().await {
            eprintln!("error flushing output: {}", e);
        };
    }
}
