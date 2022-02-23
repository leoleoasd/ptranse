use std::fmt;
use std::fs;
use std::io::{Read, Write, BufWriter};
use rand::prelude::*;
use indicatif::{ProgressBar, ProgressStyle, ProgressIterator};

#[derive(Debug, Clone)]
struct Triple<'a> {
    head: &'a str,
    tail: &'a str,
    relation: &'a str,
}
impl fmt::Display for Triple<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_> ) -> fmt::Result {
        write!(f, "{} {} {}", self.head, self.relation, self.tail)
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
    let mut map = std::collections::HashMap::<&str, Vec<&Triple>>::with_capacity(triples.len());
    println!("Building maps...");
    let bar = ProgressBar::new(triples.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed} / {eta}]({per_sec}) {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );
    use std::cell::RefCell;
    triples.iter().progress_with(bar).for_each(|triple| {
        
        map.entry(triple.head).or_insert(Vec::with_capacity(4)).push(triple);
    });
    println!("Map built.");
    println!("Finding neighbor...");
    let bar = ProgressBar::new(triples.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed} / {eta}]({per_sec}) {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );
    let new_triples = triples.iter().progress_with(bar).filter_map(|triple| -> Option<&Triple> {
        Some(map.get(triple.tail)?.choose(&mut rand::thread_rng())?.clone())
    });
    let mut writer = BufWriter::new(fs::File::create(String::from(name) + "_ptranse")?);
    for triple in new_triples {
        writer.write(format!("{}\n", triple).as_bytes())?;
    }
    println!("Found neighbor!");
    Ok(())
}
