use std::{error::Error, sync::Arc};
use futures::future::join_all;

use tokio::join;
use tokio::sync::Mutex;

use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt,
};
use tokio::sync::mpsc::{self, Receiver, Sender};

pub async fn process_file(
    input: impl AsyncBufRead + Send + Unpin,
    output: impl AsyncWrite + Send + Unpin,
    number_of_workers: usize,
) -> Result<(), Box<dyn Error>> {
    // spawn input file reader
    let (url_sender, url_receiver) = mpsc::channel(100);
    let input_task = collect_urls(input, url_sender);

    // spawn url requesters
    let (titles_sender, titles_receiver) = mpsc::channel(100);
    let mut query_tasks = vec![];
    let url_receiver = Arc::new(Mutex::new(url_receiver));
    for _ in 0..number_of_workers {
        query_tasks.push(request_urls(
            url_receiver.clone(),
            titles_sender.clone(),
        ));
    }
    // drop the original `titles_sender` so that only the worker clones remain;
    // when all workers finish and drop their clones, `titles_receiver` will close
    drop(titles_sender);
    drop(url_receiver); // must drop this clone so that request_urls tasks can finish when the channel is closed

    // spawn output file writer
    let output_task =write_output(output, titles_receiver);

    // await for tasks to finish
    join!(input_task, join_all(query_tasks), output_task);
    Ok(())
}

pub async fn collect_urls(
    mut input: impl AsyncBufRead + Send + Unpin,
    url_sender: Sender<String>,
) {
    // in each line, search for one or many urls
    loop {
        let mut line = String::new();
        match input.read_line(&mut line).await {
            Ok(s) => {
                if s == 0 {
                    break;
                }
            }
            Err(e) => {
                eprintln!("error reading input: {}", e);
                continue;
            }
        };
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
                    let is_end_character =
                        line.get(j) == Some(&b' ') || line.get(j) == Some(&b')');
                    if is_end_character {
                        break;
                    }
                    j += 1;
                }
                // send url string to request workers
                // eprintln!("url found in text: {}", &line[i..j]);
                if let Ok(s) = String::from_utf8(line[i..j].to_owned()) {
                    if url_sender.send(s).await.is_err() {
                        break;
                    };
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
        let mut guard = url_receiver.lock().await;
        let url = guard.recv().await;
        drop(guard);
        let url = if let Some(url) = url {
            url
        } else {
            break;
        };
        // get request to that url to get <title> section
        let response = match reqwest::get(&url).await {
            Ok(r) => r,
            Err(e) => {
                if titles_sender
                    .send((format!("Error in GET request: {:?}", e).to_owned(), url))
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
        };
        let body = match response.text().await {
            Ok(t) => t,
            Err(e) => {
                if titles_sender
                    .send((
                        format!("Error getting text from request: {:?}", e).to_owned(),
                        url,
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
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
        let (title, url) = match titles_receiver.recv().await {
            Some(t) => t,
            None => break,
        };
        let line = format!("[{}]({})\n", title.trim(), url.trim());
        if let Err(e) = output.write_all(line.as_bytes()).await {
            eprintln!("error writing output: {}", e)
        }
        if let Err(e) = output.flush().await {
            eprintln!("error flushing output: {}", e);
        };
    }
}
