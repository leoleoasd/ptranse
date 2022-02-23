use std::fmt;
use std::fs;
use std::io::{Read, Write, BufWriter};
use rand::prelude::*;
use indicatif::{ProgressBar, ProgressStyle, ProgressIterator};
use crossbeam::scope;
use rayon::prelude::*;

#[derive(Debug, Clone)]
struct Triple<'a> {
    head: &'a str,
    tail: &'a str,
    relation: &'a str,
}
impl fmt::Display for Triple<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_> ) -> fmt::Result {
        write!(f, "{}\t{}\t{}", self.head, self.relation, self.tail)
    }
}

pub fn process(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut triples = vec![];
    let mut file = String::new();
    fs::File::open(name)?.read_to_string(&mut file)?;
    println!("Reading file...");
    for line in file.lines() {
        let mut parts = line.split_whitespace();
        let head = parts.next().ok_or("Wrong format!")?;
        let relation = parts.next().ok_or("Wrong format!")?;
        let tail = parts.next().ok_or("Wrong format!")?;
        triples.push(Triple {
            head: head,
            tail: tail,
            relation: relation,
        });
    }
    println!("{} triples", triples.len());
    println!("Building maps...");
    let bar = ProgressBar::new(triples.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed} / {eta}]({per_sec}) {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );
    bar.set_draw_delta(100000);
    let num_cores = num_cpus::get();
    let mut maps = vec![];
    scope(|s| {
        let mut threads = vec![];
        for chunk in triples.chunks(triples.len() / num_cores + 1) {
            threads.push(s.spawn(|_| {
                let mut map = std::collections::HashMap::<&str, Vec<&Triple>>::with_capacity(chunk.len());
                chunk.iter().for_each(|triple| { 
                    // bar.inc(1);
                    map.entry(triple.head).or_insert(Vec::with_capacity(4)).push(triple);
                });
                map
            }));
        }
        threads.into_iter().for_each(|thread| { 
            let map = thread.join().unwrap();
            maps.push(map);
            bar.inc(1);
        });
    }).unwrap();
    println!("{} maps built with total size {}", maps.len(), maps.iter().map(|m| m.len()).sum::<usize>());
    println!("Finding neighbor...");
    let bar = ProgressBar::new(triples.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed} / {eta}]({per_sec}) {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );
    bar.set_draw_delta(100000);
    let mut writer = BufWriter::new(fs::File::create(String::from(name) + "_ptranse")?);
    
    triples.par_iter().map(|triple| -> (&Triple, Option<&Triple>) {
        // bar.inc(1);
        let initial = thread_rng().gen_range(0..maps.len());
        // iterator maps from id:
        for id in 0..num_cores {
            if let Some(neighbors) = maps[(initial + id) % num_cores].get(triple.tail) {
                return (triple, Some(neighbors.choose(&mut thread_rng()).unwrap()));
            }
        }
        (triple, None)
        // Some(map.get(triple.tail)?.choose(&mut rand::thread_rng())?.clone())
    }).collect::<Vec<_>>().iter().for_each(|triple| {
        match triple {
            (triple, Some(neighbor)) => {
                writer.write(format!("{}\t{}\t{}\n", triple, neighbor.relation, neighbor.tail).as_bytes()).unwrap();
            },
            (triple, None) => {
                writer.write(format!("{}\n", triple).as_bytes()).unwrap();
            },
        }
    });
    writer.flush()?;
    println!("Found neighbor!");
    Ok(())
}
